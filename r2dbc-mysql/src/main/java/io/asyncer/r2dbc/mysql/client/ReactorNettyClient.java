/*
 * Copyright 2023 asyncer.io projects
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.asyncer.r2dbc.mysql.client;

import io.asyncer.r2dbc.mysql.ConnectionContext;
import io.asyncer.r2dbc.mysql.MySqlSslConfiguration;
import io.asyncer.r2dbc.mysql.internal.util.OperatorUtils;
import io.asyncer.r2dbc.mysql.message.client.ClientMessage;
import io.asyncer.r2dbc.mysql.message.client.ExitMessage;
import io.asyncer.r2dbc.mysql.message.server.ServerMessage;
import io.asyncer.r2dbc.mysql.message.server.WarningMessage;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.r2dbc.spi.R2dbcException;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.netty.FutureMono;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of client based on the Reactor Netty project.
 */
final class ReactorNettyClient implements Client {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ReactorNettyClient.class);

    private static final boolean DEBUG_ENABLED = logger.isDebugEnabled();

    private static final boolean INFO_ENABLED = logger.isInfoEnabled();

    private static final int ST_CONNECTED = 0;

    private static final int ST_CLOSING = 1;

    private static final int ST_CLOSED = 2;

    private static final AtomicIntegerFieldUpdater<ReactorNettyClient> STATE_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(ReactorNettyClient.class, "state");

    private volatile int state = ST_CONNECTED;

    private final Connection connection;

    private final ConnectionContext context;

    private final Sinks.Many<ClientMessage> requests = Sinks.many().unicast().onBackpressureBuffer();

    private final Sinks.Many<ServerMessage> responseProcessor =
        Sinks.many().multicast().onBackpressureBuffer(512, false);

    private final RequestQueue requestQueue = new RequestQueue();

    ReactorNettyClient(Connection connection, MySqlSslConfiguration ssl, ConnectionContext context) {
        requireNonNull(connection, "connection must not be null");
        requireNonNull(context, "context must not be null");
        requireNonNull(ssl, "ssl must not be null");
        require(responseProcessor.asFlux() instanceof Subscriber,
            "responseProcessor(" + responseProcessor + ") must be a Subscriber");

        this.connection = connection;
        this.context = context;

        // Note: encoder/decoder should before reactor bridge.
        connection.addHandlerLast(MessageDuplexCodec.NAME, new MessageDuplexCodec(context));

        if (ssl.getSslMode().startSsl()) {
            connection.addHandlerFirst(SslBridgeHandler.NAME, new SslBridgeHandler(context, ssl));
        }

        if (logger.isTraceEnabled()) {
            logger.debug("Connection tracking logging is enabled");

            connection.addHandlerFirst(LoggingHandler.class.getSimpleName(),
                new LoggingHandler(ReactorNettyClient.class, LogLevel.TRACE));
        }

        ResponseSink sink = new ResponseSink();

        connection.inbound().receiveObject()
            .doOnNext(it -> {
                if (it instanceof ServerMessage) {
                    if (it instanceof ReferenceCounted) {
                        ((ReferenceCounted) it).retain();
                    }
                    sink.next((ServerMessage) it);
                } else {
                    // ReferenceCounted will be released by Netty.
                    throw ClientExceptions.unsupportedProtocol(it.getClass().getTypeName());
                }
            })
            .onErrorResume(this::resumeError)
            .subscribe(new ResponseSubscriber(sink));

        this.requests.asFlux()
            .concatMap(message -> {
                if (DEBUG_ENABLED) {
                    logger.debug("Request: {}", message);
                }

                if (message.isSequenceReset()) {
                    resetSequence(connection);
                }

                return connection.outbound().sendObject(message);
            })
            .onErrorResume(this::resumeError)
            .doAfterTerminate(this::handleClose)
            .subscribe();
    }

    @Override
    public <T> Flux<T> exchange(ClientMessage request,
        BiConsumer<ServerMessage, SynchronousSink<T>> handler) {
        requireNonNull(request, "request must not be null");

        return Mono.<Flux<T>>create(sink -> {
            if (!isConnected()) {
                if (request instanceof Disposable) {
                    ((Disposable) request).dispose();
                }
                sink.error(ClientExceptions.exchangeClosed());
                return;
            }

            Flux<T> responses = OperatorUtils.discardOnCancel(
                responseProcessor.asFlux()
                    .doOnSubscribe(ignored -> emitNextRequest(request))
                    .handle(handler)
                    .doOnTerminate(requestQueue)
            ).doOnDiscard(ReferenceCounted.class, ReferenceCounted::release);

            requestQueue.submit(RequestTask.wrap(request, sink, responses));
        }).flatMapMany(Function.identity());
    }

    @Override
    public <T> Flux<T> exchange(FluxExchangeable<T> exchangeable) {
        requireNonNull(exchangeable, "exchangeable must not be null");

        return Mono.<Flux<T>>create(sink -> {
            if (!isConnected()) {
                exchangeable.subscribe(request -> {
                    if (request instanceof Disposable) {
                        ((Disposable) request).dispose();
                    }
                }, e -> requests.emitError(e, Sinks.EmitFailureHandler.FAIL_FAST));
                sink.error(ClientExceptions.exchangeClosed());
                return;
            }

            Flux<T> responses = responseProcessor
                .asFlux()
                .doOnSubscribe(ignored -> exchangeable.subscribe(
                    this::emitNextRequest,
                    e -> requests.emitError(e, Sinks.EmitFailureHandler.FAIL_FAST)
                ))
                .handle(exchangeable)
                .doOnTerminate(() -> {
                    exchangeable.dispose();
                    requestQueue.run();
                });

            requestQueue.submit(RequestTask.wrap(exchangeable, sink, OperatorUtils.discardOnCancel(responses)
                .doOnDiscard(ReferenceCounted.class, ReferenceCounted::release)
                .doOnCancel(exchangeable::dispose)));
        }).flatMapMany(Function.identity());
    }

    @Override
    public Mono<Void> close() {
        return Mono.<Mono<Void>>create(sink -> {
            if (state == ST_CLOSED) {
                logger.debug("Close request ignored (connection already closed)");
                sink.success();
                return;
            }

            logger.debug("Close request accepted");

            requestQueue.submit(RequestTask.wrap(sink, Mono.fromRunnable(() -> {
                Sinks.EmitResult result = requests.tryEmitNext(ExitMessage.INSTANCE);

                if (result != Sinks.EmitResult.OK) {
                    logger.error("Exit message sending failed due to {}, force closing", result);
                } else {
                    if (STATE_UPDATER.compareAndSet(this, ST_CONNECTED, ST_CLOSING)) {
                        logger.debug("Exit message sent");
                    } else {
                        logger.debug("Exit message sent (duplicated / connection already closed)");
                    }
                }
            })));
        }).flatMap(Function.identity()).onErrorResume(e -> {
            logger.error("Exit message sending failed, force closing", e);
            return Mono.empty();
        }).then(forceClose());
    }

    @Override
    public Mono<Void> forceClose() {
        return FutureMono.deferFuture(() -> connection.channel().close());
    }

    @Override
    public ByteBufAllocator getByteBufAllocator() {
        return connection.outbound().alloc();
    }

    @Override
    public ConnectionContext getContext() {
        return context;
    }

    @Override
    public boolean isConnected() {
        return state < ST_CLOSED && connection.channel().isOpen();
    }

    @Override
    public void sslUnsupported() {
        connection.channel().pipeline().fireUserEventTriggered(SslState.UNSUPPORTED);
    }

    @Override
    public void loginSuccess() {
        if (context.getCapability().isCompression()) {
            connection.channel().pipeline().fireUserEventTriggered(PacketEvent.USE_COMPRESSION);
        } else {
            resetSequence(connection);
        }
    }

    private static void resetSequence(Connection connection) {
        connection.channel().pipeline().fireUserEventTriggered(PacketEvent.RESET_SEQUENCE);
    }

    @Override
    public String toString() {
        return String.format("ReactorNettyClient(%s){connectionId=%d}",
            isConnected() ? "activating" : "closing or closed", context.getConnectionId());
    }

    private void emitNextRequest(ClientMessage request) {
        if (isConnected() && requests.tryEmitNext(request) == Sinks.EmitResult.OK) {
            return;
        }

        if (request instanceof Disposable) {
            ((Disposable) request).dispose();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> Mono<T> resumeError(Throwable e) {
        drainError(ClientExceptions.wrap(e));

        requests.emitComplete((signalType, emitResult) -> {
            if (emitResult.isFailure()) {
                logger.error("Error: {}", emitResult);
            }

            return false;
        });

        logger.error("Error: {}", e.getLocalizedMessage(), e);

        return (Mono<T>) close();
    }

    private void drainError(R2dbcException e) {
        this.requestQueue.dispose();
        responseProcessor.emitError(e, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    private void handleClose() {
        final int oldState = state;
        if (oldState == ST_CLOSED) {
            logger.debug("Connection already closed");
            return;
        }

        STATE_UPDATER.set(this, ST_CLOSED);

        if (oldState != ST_CLOSING) {
            logger.warn("Connection unexpectedly closed");
            drainError(ClientExceptions.unexpectedClosed());
        } else {
            logger.debug("Connection closed");
            drainError(ClientExceptions.expectedClosed());
        }
    }

    private final class ResponseSubscriber implements CoreSubscriber<Object> {

        private final ResponseSink sink;

        private ResponseSubscriber(ResponseSink sink) {
            this.sink = sink;
        }

        @Override
        public void onSubscribe(Subscription s) {
            ((Subscriber<?>) responseProcessor.asFlux()).onSubscribe(s);
        }

        @Override
        public void onNext(Object message) {
            // The message is already used, see also constructor.
        }

        @Override
        public void onError(Throwable t) {
            sink.error(t);
        }

        @Override
        public void onComplete() {
            handleClose();
        }
    }

    private final class ResponseSink implements SynchronousSink<ServerMessage> {

        @Override
        public void complete() {
            throw new UnsupportedOperationException();
        }

        @Deprecated
        @Override
        public Context currentContext() {
            return Context.empty();
        }

        @Override
        public ContextView contextView() {
            return Context.empty();
        }

        @Override
        public void error(Throwable e) {
            responseProcessor.emitError(ClientExceptions.wrap(e), EmitFailureHandler.FAIL_FAST);
        }

        @Override
        public void next(ServerMessage message) {
            if (message instanceof WarningMessage) {
                int warnings = ((WarningMessage) message).getWarnings();
                if (warnings == 0) {
                    if (DEBUG_ENABLED) {
                        logger.debug("Response: {}", message);
                    }
                } else if (INFO_ENABLED) {
                    logger.info("Response: {}, reports {} warning(s)", message, warnings);
                }
            } else if (DEBUG_ENABLED) {
                logger.debug("Response: {}", message);
            }

            responseProcessor.emitNext(message, EmitFailureHandler.FAIL_FAST);
        }
    }
}
