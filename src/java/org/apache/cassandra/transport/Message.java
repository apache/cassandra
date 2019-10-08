/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.transport;

import java.util.ArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.LocalAwareExecutorService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.transport.messages.*;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.concurrent.SharedExecutorPool.SHARED;

/**
 * A message from the CQL binary protocol.
 */
public abstract class Message
{
    protected static final Logger logger = LoggerFactory.getLogger(Message.class);

    /**
     * When we encounter an unexpected IOException we look for these {@link Throwable#getMessage() messages}
     * (because we have no better way to distinguish) and log them at DEBUG rather than INFO, since they
     * are generally caused by unclean client disconnects rather than an actual problem.
     */
    private static final Set<String> ioExceptionsAtDebugLevel = ImmutableSet.<String>builder().
            add("Connection reset by peer").
            add("Broken pipe").
            add("Connection timed out").
            build();

    public interface Codec<M extends Message> extends CBCodec<M> {}

    public enum Direction
    {
        REQUEST, RESPONSE;

        public static Direction extractFromVersion(int versionWithDirection)
        {
            return (versionWithDirection & 0x80) == 0 ? REQUEST : RESPONSE;
        }

        public int addToVersion(int rawVersion)
        {
            return this == REQUEST ? (rawVersion & 0x7F) : (rawVersion | 0x80);
        }
    }

    public enum Type
    {
        ERROR          (0,  Direction.RESPONSE, ErrorMessage.codec),
        STARTUP        (1,  Direction.REQUEST,  StartupMessage.codec),
        READY          (2,  Direction.RESPONSE, ReadyMessage.codec),
        AUTHENTICATE   (3,  Direction.RESPONSE, AuthenticateMessage.codec),
        CREDENTIALS    (4,  Direction.REQUEST,  CredentialsMessage.codec),
        OPTIONS        (5,  Direction.REQUEST,  OptionsMessage.codec),
        SUPPORTED      (6,  Direction.RESPONSE, SupportedMessage.codec),
        QUERY          (7,  Direction.REQUEST,  QueryMessage.codec),
        RESULT         (8,  Direction.RESPONSE, ResultMessage.codec),
        PREPARE        (9,  Direction.REQUEST,  PrepareMessage.codec),
        EXECUTE        (10, Direction.REQUEST,  ExecuteMessage.codec),
        REGISTER       (11, Direction.REQUEST,  RegisterMessage.codec),
        EVENT          (12, Direction.RESPONSE, EventMessage.codec),
        BATCH          (13, Direction.REQUEST,  BatchMessage.codec),
        AUTH_CHALLENGE (14, Direction.RESPONSE, AuthChallenge.codec),
        AUTH_RESPONSE  (15, Direction.REQUEST,  AuthResponse.codec),
        AUTH_SUCCESS   (16, Direction.RESPONSE, AuthSuccess.codec);

        public final int opcode;
        public final Direction direction;
        public final Codec<?> codec;

        private static final Type[] opcodeIdx;
        static
        {
            int maxOpcode = -1;
            for (Type type : Type.values())
                maxOpcode = Math.max(maxOpcode, type.opcode);
            opcodeIdx = new Type[maxOpcode + 1];
            for (Type type : Type.values())
            {
                if (opcodeIdx[type.opcode] != null)
                    throw new IllegalStateException("Duplicate opcode");
                opcodeIdx[type.opcode] = type;
            }
        }

        Type(int opcode, Direction direction, Codec<?> codec)
        {
            this.opcode = opcode;
            this.direction = direction;
            this.codec = codec;
        }

        public static Type fromOpcode(int opcode, Direction direction)
        {
            if (opcode >= opcodeIdx.length)
                throw new ProtocolException(String.format("Unknown opcode %d", opcode));
            Type t = opcodeIdx[opcode];
            if (t == null)
                throw new ProtocolException(String.format("Unknown opcode %d", opcode));
            if (t.direction != direction)
                throw new ProtocolException(String.format("Wrong protocol direction (expected %s, got %s) for opcode %d (%s)",
                                                          t.direction,
                                                          direction,
                                                          opcode,
                                                          t));
            return t;
        }
    }

    public final Type type;
    protected Connection connection;
    private int streamId;
    private Frame sourceFrame;
    private Map<String, ByteBuffer> customPayload;
    protected ProtocolVersion forcedProtocolVersion = null;

    protected Message(Type type)
    {
        this.type = type;
    }

    public void attach(Connection connection)
    {
        this.connection = connection;
    }

    public Connection connection()
    {
        return connection;
    }

    public Message setStreamId(int streamId)
    {
        this.streamId = streamId;
        return this;
    }

    public int getStreamId()
    {
        return streamId;
    }

    public void setSourceFrame(Frame sourceFrame)
    {
        this.sourceFrame = sourceFrame;
    }

    public Frame getSourceFrame()
    {
        return sourceFrame;
    }

    public Map<String, ByteBuffer> getCustomPayload()
    {
        return customPayload;
    }

    public void setCustomPayload(Map<String, ByteBuffer> customPayload)
    {
        this.customPayload = customPayload;
    }

    public static abstract class Request extends Message
    {
        protected boolean tracingRequested;

        protected Request(Type type)
        {
            super(type);

            if (type.direction != Direction.REQUEST)
                throw new IllegalArgumentException();
        }

        public abstract Response execute(QueryState queryState, long queryStartNanoTime);

        public void setTracingRequested()
        {
            this.tracingRequested = true;
        }

        public boolean isTracingRequested()
        {
            return tracingRequested;
        }
    }

    public static abstract class Response extends Message
    {
        protected UUID tracingId;
        protected List<String> warnings;

        protected Response(Type type)
        {
            super(type);

            if (type.direction != Direction.RESPONSE)
                throw new IllegalArgumentException();
        }

        public Message setTracingId(UUID tracingId)
        {
            this.tracingId = tracingId;
            return this;
        }

        public UUID getTracingId()
        {
            return tracingId;
        }

        public Message setWarnings(List<String> warnings)
        {
            this.warnings = warnings;
            return this;
        }

        public List<String> getWarnings()
        {
            return warnings;
        }
    }

    @ChannelHandler.Sharable
    public static class ProtocolDecoder extends MessageToMessageDecoder<Frame>
    {
        public void decode(ChannelHandlerContext ctx, Frame frame, List results)
        {
            boolean isRequest = frame.header.type.direction == Direction.REQUEST;
            boolean isTracing = frame.header.flags.contains(Frame.Header.Flag.TRACING);
            boolean isCustomPayload = frame.header.flags.contains(Frame.Header.Flag.CUSTOM_PAYLOAD);
            boolean hasWarning = frame.header.flags.contains(Frame.Header.Flag.WARNING);

            UUID tracingId = isRequest || !isTracing ? null : CBUtil.readUUID(frame.body);
            List<String> warnings = isRequest || !hasWarning ? null : CBUtil.readStringList(frame.body);
            Map<String, ByteBuffer> customPayload = !isCustomPayload ? null : CBUtil.readBytesMap(frame.body);

            try
            {
                if (isCustomPayload && frame.header.version.isSmallerThan(ProtocolVersion.V4))
                    throw new ProtocolException("Received frame with CUSTOM_PAYLOAD flag for native protocol version < 4");

                Message message = frame.header.type.codec.decode(frame.body, frame.header.version);
                message.setStreamId(frame.header.streamId);
                message.setSourceFrame(frame);
                message.setCustomPayload(customPayload);

                if (isRequest)
                {
                    assert message instanceof Request;
                    Request req = (Request)message;
                    Connection connection = ctx.channel().attr(Connection.attributeKey).get();
                    req.attach(connection);
                    if (isTracing)
                        req.setTracingRequested();
                }
                else
                {
                    assert message instanceof Response;
                    if (isTracing)
                        ((Response)message).setTracingId(tracingId);
                    if (hasWarning)
                        ((Response)message).setWarnings(warnings);
                }

                results.add(message);
            }
            catch (Throwable ex)
            {
                frame.release();
                // Remember the streamId
                throw ErrorMessage.wrap(ex, frame.header.streamId);
            }
        }
    }

    @ChannelHandler.Sharable
    public static class ProtocolEncoder extends MessageToMessageEncoder<Message>
    {
        private final ProtocolVersionLimit versionCap;

        ProtocolEncoder(ProtocolVersionLimit versionCap)
        {
            this.versionCap = versionCap;
        }

        public void encode(ChannelHandlerContext ctx, Message message, List results)
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();
            // The only case the connection can be null is when we send the initial STARTUP message (client side thus)
            ProtocolVersion version = connection == null ? versionCap.getMaxVersion() : connection.getVersion();

            EnumSet<Frame.Header.Flag> flags = EnumSet.noneOf(Frame.Header.Flag.class);

            Codec<Message> codec = (Codec<Message>)message.type.codec;
            try
            {
                int messageSize = codec.encodedSize(message, version);
                ByteBuf body;
                if (message instanceof Response)
                {
                    UUID tracingId = ((Response)message).getTracingId();
                    Map<String, ByteBuffer> customPayload = message.getCustomPayload();
                    if (tracingId != null)
                        messageSize += CBUtil.sizeOfUUID(tracingId);
                    List<String> warnings = ((Response)message).getWarnings();
                    if (warnings != null)
                    {
                        if (version.isSmallerThan(ProtocolVersion.V4))
                            throw new ProtocolException("Must not send frame with WARNING flag for native protocol version < 4");
                        messageSize += CBUtil.sizeOfStringList(warnings);
                    }
                    if (customPayload != null)
                    {
                        if (version.isSmallerThan(ProtocolVersion.V4))
                            throw new ProtocolException("Must not send frame with CUSTOM_PAYLOAD flag for native protocol version < 4");
                        messageSize += CBUtil.sizeOfBytesMap(customPayload);
                    }
                    body = CBUtil.allocator.buffer(messageSize);
                    if (tracingId != null)
                    {
                        CBUtil.writeUUID(tracingId, body);
                        flags.add(Frame.Header.Flag.TRACING);
                    }
                    if (warnings != null)
                    {
                        CBUtil.writeStringList(warnings, body);
                        flags.add(Frame.Header.Flag.WARNING);
                    }
                    if (customPayload != null)
                    {
                        CBUtil.writeBytesMap(customPayload, body);
                        flags.add(Frame.Header.Flag.CUSTOM_PAYLOAD);
                    }
                }
                else
                {
                    assert message instanceof Request;
                    if (((Request)message).isTracingRequested())
                        flags.add(Frame.Header.Flag.TRACING);
                    Map<String, ByteBuffer> payload = message.getCustomPayload();
                    if (payload != null)
                        messageSize += CBUtil.sizeOfBytesMap(payload);
                    body = CBUtil.allocator.buffer(messageSize);
                    if (payload != null)
                    {
                        CBUtil.writeBytesMap(payload, body);
                        flags.add(Frame.Header.Flag.CUSTOM_PAYLOAD);
                    }
                }

                try
                {
                    codec.encode(message, body, version);
                }
                catch (Throwable e)
                {
                    body.release();
                    throw e;
                }

                // if the driver attempted to connect with a protocol version lower than the minimum supported
                // version, respond with a protocol error message with the correct frame header for that version
                ProtocolVersion responseVersion = message.forcedProtocolVersion == null
                                    ? version
                                    : message.forcedProtocolVersion;

                if (responseVersion.isBeta())
                    flags.add(Frame.Header.Flag.USE_BETA);

                results.add(Frame.create(message.type, message.getStreamId(), responseVersion, flags, body));
            }
            catch (Throwable e)
            {
                throw ErrorMessage.wrap(e, message.getStreamId());
            }
        }
    }

    public static class Dispatcher extends SimpleChannelInboundHandler<Request>
    {
        private static final LocalAwareExecutorService requestExecutor = SHARED.newExecutor(DatabaseDescriptor.getNativeTransportMaxThreads(),
                                                                                            Integer.MAX_VALUE,
                                                                                            "transport",
                                                                                            "Native-Transport-Requests");

        /**
         * Current count of *request* bytes that are live on the channel.
         *
         * Note: should only be accessed while on the netty event loop.
         */
        private long channelPayloadBytesInFlight;

        private final Server.EndpointPayloadTracker endpointPayloadTracker;

        private boolean paused;

        private static class FlushItem
        {
            final ChannelHandlerContext ctx;
            final Object response;
            final Frame sourceFrame;
            final Dispatcher dispatcher;

            private FlushItem(ChannelHandlerContext ctx, Object response, Frame sourceFrame, Dispatcher dispatcher)
            {
                this.ctx = ctx;
                this.sourceFrame = sourceFrame;
                this.response = response;
                this.dispatcher = dispatcher;
            }

            public void release()
            {
                dispatcher.releaseItem(this);
            }
        }

        private static abstract class Flusher implements Runnable
        {
            final EventLoop eventLoop;
            final ConcurrentLinkedQueue<FlushItem> queued = new ConcurrentLinkedQueue<>();
            final AtomicBoolean scheduled = new AtomicBoolean(false);
            final HashSet<ChannelHandlerContext> channels = new HashSet<>();
            final List<FlushItem> flushed = new ArrayList<>();

            void start()
            {
                if (!scheduled.get() && scheduled.compareAndSet(false, true))
                {
                    this.eventLoop.execute(this);
                }
            }

            public Flusher(EventLoop eventLoop)
            {
                this.eventLoop = eventLoop;
            }
        }

        private static final class LegacyFlusher extends Flusher
        {
            int runsSinceFlush = 0;
            int runsWithNoWork = 0;

            private LegacyFlusher(EventLoop eventLoop)
            {
                super(eventLoop);
            }

            public void run()
            {

                boolean doneWork = false;
                FlushItem flush;
                while ( null != (flush = queued.poll()) )
                {
                    channels.add(flush.ctx);
                    flush.ctx.write(flush.response, flush.ctx.voidPromise());
                    flushed.add(flush);
                    doneWork = true;
                }

                runsSinceFlush++;

                if (!doneWork || runsSinceFlush > 2 || flushed.size() > 50)
                {
                    for (ChannelHandlerContext channel : channels)
                        channel.flush();
                    for (FlushItem item : flushed)
                        item.release();

                    channels.clear();
                    flushed.clear();
                    runsSinceFlush = 0;
                }

                if (doneWork)
                {
                    runsWithNoWork = 0;
                }
                else
                {
                    // either reschedule or cancel
                    if (++runsWithNoWork > 5)
                    {
                        scheduled.set(false);
                        if (queued.isEmpty() || !scheduled.compareAndSet(false, true))
                            return;
                    }
                }

                eventLoop.schedule(this, 10000, TimeUnit.NANOSECONDS);
            }
        }

        private static final class ImmediateFlusher extends Flusher
        {
            private ImmediateFlusher(EventLoop eventLoop)
            {
                super(eventLoop);
            }

            public void run()
            {
                boolean doneWork = false;
                FlushItem flush;
                scheduled.set(false);

                while (null != (flush = queued.poll()))
                {
                    channels.add(flush.ctx);
                    flush.ctx.write(flush.response, flush.ctx.voidPromise());
                    flushed.add(flush);
                    doneWork = true;
                }

                if (doneWork)
                {
                    for (ChannelHandlerContext channel : channels)
                        channel.flush();
                    for (FlushItem item : flushed)
                        item.release();

                    channels.clear();
                    flushed.clear();
                }
            }
        }

        private static final ConcurrentMap<EventLoop, Flusher> flusherLookup = new ConcurrentHashMap<>();

        private final boolean useLegacyFlusher;

        public Dispatcher(boolean useLegacyFlusher, Server.EndpointPayloadTracker endpointPayloadTracker)
        {
            super(false);
            this.useLegacyFlusher = useLegacyFlusher;
            this.endpointPayloadTracker = endpointPayloadTracker;
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Request request)
        {
            // if we decide to handle this message, process it outside of the netty event loop
            if (shouldHandleRequest(ctx, request))
                requestExecutor.submit(() -> processRequest(ctx, request));
        }

        /** This check for inflight payload to potentially discard the request should have been ideally in one of the
         * first handlers in the pipeline (Frame::decode()). However, incase of any exception thrown between that
         * handler (where inflight payload is incremented) and this handler (Dispatcher::channelRead0) (where inflight
         * payload in decremented), inflight payload becomes erroneous. ExceptionHandler is not sufficient for this
         * purpose since it does not have the frame associated with the exception.
         *
         * Note: this method should execute on the netty event loop.
         */
        private boolean shouldHandleRequest(ChannelHandlerContext ctx, Request request)
        {
            long frameSize = request.getSourceFrame().header.bodySizeInBytes;

            ResourceLimits.EndpointAndGlobal endpointAndGlobalPayloadsInFlight = endpointPayloadTracker.endpointAndGlobalPayloadsInFlight;

            // check for overloaded state by trying to allocate framesize to inflight payload trackers
            if (endpointAndGlobalPayloadsInFlight.tryAllocate(frameSize) != ResourceLimits.Outcome.SUCCESS)
            {
                if (request.connection.isThrowOnOverload())
                {
                    // discard the request and throw an exception
                    ClientMetrics.instance.markRequestDiscarded();
                    logger.trace("Discarded request of size: {}. InflightChannelRequestPayload: {}, InflightEndpointRequestPayload: {}, InflightOverallRequestPayload: {}, Request: {}",
                                 frameSize,
                                 channelPayloadBytesInFlight,
                                 endpointAndGlobalPayloadsInFlight.endpoint().using(),
                                 endpointAndGlobalPayloadsInFlight.global().using(),
                                 request);
                    throw ErrorMessage.wrap(new OverloadedException("Server is in overloaded state. Cannot accept more requests at this point"),
                                            request.getSourceFrame().header.streamId);
                }
                else
                {
                    // set backpressure on the channel, and handle the request
                    endpointAndGlobalPayloadsInFlight.allocate(frameSize);
                    ctx.channel().config().setAutoRead(false);
                    ClientMetrics.instance.pauseConnection();
                    paused = true;
                }
            }

            channelPayloadBytesInFlight += frameSize;
            return true;
        }

        /**
         * Note: this method will be used in the {@link Flusher#run()}, which executes on the netty event loop
         * ({@link Dispatcher#flusherLookup}). Thus, we assume the semantics and visibility of variables
         * of being on the event loop.
         */
        private void releaseItem(FlushItem item)
        {
            long itemSize = item.sourceFrame.header.bodySizeInBytes;
            item.sourceFrame.release();

            // since the request has been processed, decrement inflight payload at channel, endpoint and global levels
            channelPayloadBytesInFlight -= itemSize;
            ResourceLimits.Outcome endpointGlobalReleaseOutcome = endpointPayloadTracker.endpointAndGlobalPayloadsInFlight.release(itemSize);

            // now check to see if we need to reenable the channel's autoRead.
            // If the current payload side is zero, we must reenable autoread as
            // 1) we allow no other thread/channel to do it, and
            // 2) there's no other events following this one (becuase we're at zero bytes in flight),
            // so no successive to trigger the other clause in this if-block
            ChannelConfig config = item.ctx.channel().config();
            if (paused && (channelPayloadBytesInFlight == 0 || endpointGlobalReleaseOutcome == ResourceLimits.Outcome.BELOW_LIMIT))
            {
                paused = false;
                ClientMetrics.instance.unpauseConnection();
                config.setAutoRead(true);
            }
        }

        /**
         * Note: this method is not expected to execute on the netty event loop.
         */
        void processRequest(ChannelHandlerContext ctx, Request request)
        {
            final Response response;
            final ServerConnection connection;
            long queryStartNanoTime = System.nanoTime();

            try
            {
                assert request.connection() instanceof ServerConnection;
                connection = (ServerConnection)request.connection();
                if (connection.getVersion().isGreaterOrEqualTo(ProtocolVersion.V4))
                    ClientWarn.instance.captureWarnings();

                QueryState qstate = connection.validateNewMessage(request.type, connection.getVersion(), request.getStreamId());

                logger.trace("Received: {}, v={}", request, connection.getVersion());
                response = request.execute(qstate, queryStartNanoTime);
                response.setStreamId(request.getStreamId());
                response.setWarnings(ClientWarn.instance.getWarnings());
                response.attach(connection);
                connection.applyStateTransition(request.type, response.type);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                UnexpectedChannelExceptionHandler handler = new UnexpectedChannelExceptionHandler(ctx.channel(), true);
                flush(new FlushItem(ctx, ErrorMessage.fromException(t, handler).setStreamId(request.getStreamId()), request.getSourceFrame(), this));
                return;
            }
            finally
            {
                ClientWarn.instance.resetWarnings();
            }

            logger.trace("Responding: {}, v={}", response, connection.getVersion());
            flush(new FlushItem(ctx, response, request.getSourceFrame(), this));
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx)
        {
            endpointPayloadTracker.release();
            if (paused)
            {
                paused = false;
                ClientMetrics.instance.unpauseConnection();
            }
            ctx.fireChannelInactive();
        }

        private void flush(FlushItem item)
        {
            EventLoop loop = item.ctx.channel().eventLoop();
            Flusher flusher = flusherLookup.get(loop);
            if (flusher == null)
            {
                Flusher created = useLegacyFlusher ? new LegacyFlusher(loop) : new ImmediateFlusher(loop);
                Flusher alt = flusherLookup.putIfAbsent(loop, flusher = created);
                if (alt != null)
                    flusher = alt;
            }

            flusher.queued.add(item);
            flusher.start();
        }

        public static void shutdown()
        {
            if (requestExecutor != null)
            {
                requestExecutor.shutdown();
            }
        }
    }

    @ChannelHandler.Sharable
    public static final class ExceptionHandler extends ChannelInboundHandlerAdapter
    {

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, Throwable cause)
        {
            // Provide error message to client in case channel is still open
            UnexpectedChannelExceptionHandler handler = new UnexpectedChannelExceptionHandler(ctx.channel(), false);
            ErrorMessage errorMessage = ErrorMessage.fromException(cause, handler);
            if (ctx.channel().isOpen())
            {
                ChannelFuture future = ctx.writeAndFlush(errorMessage);
                // On protocol exception, close the channel as soon as the message have been sent
                if (cause instanceof ProtocolException)
                {
                    future.addListener(new ChannelFutureListener()
                    {
                        public void operationComplete(ChannelFuture future)
                        {
                            ctx.close();
                        }
                    });
                }
            }
        }
    }

    /**
     * Include the channel info in the logged information for unexpected errors, and (if {@link #alwaysLogAtError} is
     * false then choose the log level based on the type of exception (some are clearly client issues and shouldn't be
     * logged at server ERROR level)
     */
    static final class UnexpectedChannelExceptionHandler implements Predicate<Throwable>
    {
        private final Channel channel;
        private final boolean alwaysLogAtError;

        UnexpectedChannelExceptionHandler(Channel channel, boolean alwaysLogAtError)
        {
            this.channel = channel;
            this.alwaysLogAtError = alwaysLogAtError;
        }

        @Override
        public boolean apply(Throwable exception)
        {
            String message;
            try
            {
                message = "Unexpected exception during request; channel = " + channel;
            }
            catch (Exception ignore)
            {
                // We don't want to make things worse if String.valueOf() throws an exception
                message = "Unexpected exception during request; channel = <unprintable>";
            }

            if (!alwaysLogAtError && exception instanceof IOException)
            {
                String errorMessage = exception.getMessage();
                boolean logAtTrace = false;

                for (String ioException : ioExceptionsAtDebugLevel)
                {
                    // exceptions thrown from the netty epoll transport add the name of the function that failed
                    // to the exception string (which is simply wrapping a JDK exception), so we can't do a simple/naive comparison
                    if (errorMessage.contains(ioException))
                    {
                        logAtTrace = true;
                        break;
                    }
                }

                if (logAtTrace)
                {
                    // Likely unclean client disconnects
                    logger.trace(message, exception);
                }
                else
                {
                    // Generally unhandled IO exceptions are network issues, not actual ERRORS
                    logger.info(message, exception);
                }
            }
            else
            {
                // Anything else is probably a bug in server of client binary protocol handling
                logger.error(message, exception);
            }

            // We handled the exception.
            return true;
        }
    }
}
