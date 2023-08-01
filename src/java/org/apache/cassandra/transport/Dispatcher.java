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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;
import org.apache.cassandra.concurrent.DebuggableTask.RunnableDebuggableTask;
import org.apache.cassandra.concurrent.LocalAwareExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;
import org.apache.cassandra.transport.ClientResourceLimits.Overload;
import org.apache.cassandra.transport.Flusher.FlushItem;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.concurrent.SharedExecutorPool.SHARED;

public class Dispatcher
{
    private static final Logger logger = LoggerFactory.getLogger(Dispatcher.class);

    @VisibleForTesting
    static final LocalAwareExecutorPlus requestExecutor = SHARED.newExecutor(DatabaseDescriptor.getNativeTransportMaxThreads(),
                                                                             DatabaseDescriptor::setNativeTransportMaxThreads,
                                                                             "transport",
                                                                             "Native-Transport-Requests");

    /** CASSANDRA-17812: Rate-limit new client connection setup to avoid overwhelming during bcrypt
     *
     * authExecutor is a separate thread pool for handling requests on connections that need to be authenticated.
     * Calls to AUTHENTICATE can be expensive if the number of rounds for bcrypt is configured to a high value,
     * so during a connection storm checking the password hash would starve existing connected clients for CPU and
     * trigger timeouts if on the same thread pool as standard requests.
     *
     * Moving authentication requests to a small, separate pool prevents starvation handling all other
     * requests. If the authExecutor pool backs up, it may cause authentication timeouts but the clients should
     * back off and retry while the rest of the system continues to make progress.
     *
     * Setting less than 1 will service auth requests on the standard {@link Dispatcher#requestExecutor}
     */
    @VisibleForTesting
    static final LocalAwareExecutorPlus authExecutor = SHARED.newExecutor(Math.max(1, DatabaseDescriptor.getNativeTransportMaxAuthThreads()),
                                                                          DatabaseDescriptor::setNativeTransportMaxAuthThreads,
                                                                          "transport",
                                                                          "Native-Transport-Auth-Requests");

    private static final ConcurrentMap<EventLoop, Flusher> flusherLookup = new ConcurrentHashMap<>();
    private final boolean useLegacyFlusher;

    /**
     * Takes a Channel, Request and the Response produced by processRequest and outputs a FlushItem
     * appropriate for the pipeline, which is specific to the protocol version. V5 and above will
     * produce FlushItem.Framed instances whereas earlier versions require FlushItem.Unframed.
     * The instances of these FlushItem subclasses are specialized to release resources in the
     * right way for the specific pipeline that produced them.
     */
    // TODO parameterize with FlushItem subclass
    interface FlushItemConverter
    {
        FlushItem<?> toFlushItem(Channel channel, Message.Request request, Message.Response response);
    }

    public Dispatcher(boolean useLegacyFlusher)
    {
        this.useLegacyFlusher = useLegacyFlusher;
    }

    public void dispatch(Channel channel, Message.Request request, FlushItemConverter forFlusher, Overload backpressure)
    {
        // if native_transport_max_auth_threads is < 1, don't delegate to new pool on auth messages
        boolean isAuthQuery = DatabaseDescriptor.getNativeTransportMaxAuthThreads() > 0 &&
                              (request.type == Message.Type.AUTH_RESPONSE || request.type == Message.Type.CREDENTIALS);

        // Importantly, the authExecutor will handle the AUTHENTICATE message which may be CPU intensive.
        LocalAwareExecutorPlus executor = isAuthQuery ? authExecutor : requestExecutor;

        executor.submit(new RequestProcessor(channel, request, forFlusher, backpressure));
        ClientMetrics.instance.markRequestDispatched();
    }

    public class RequestProcessor implements RunnableDebuggableTask
    {
        private final Channel channel;
        private final Message.Request request;
        private final FlushItemConverter forFlusher;
        private final Overload backpressure;
        
        private final long approxCreationTimeNanos = MonotonicClock.Global.approxTime.now();
        private volatile long approxStartTimeNanos;
        
        public RequestProcessor(Channel channel, Message.Request request, FlushItemConverter forFlusher, Overload backpressure)
        {
            this.channel = channel;
            this.request = request;
            this.forFlusher = forFlusher;
            this.backpressure = backpressure;
        }

        @Override
        public void run()
        {
            approxStartTimeNanos = MonotonicClock.Global.approxTime.now();
            processRequest(channel, request, forFlusher, backpressure, approxStartTimeNanos);
        }

        @Override
        public long creationTimeNanos()
        {
            return approxCreationTimeNanos;
        }

        @Override
        public long startTimeNanos()
        {
            return approxStartTimeNanos;
        }

        @Override
        public String description()
        {
            return request.toString();
        }
    }

    /**
     * Note: this method may be executed on the netty event loop, during initial protocol negotiation; the caller is
     * responsible for cleaning up any global or thread-local state. (ex. tracing, client warnings, etc.).
     */
    private static Message.Response processRequest(ServerConnection connection, Message.Request request, Overload backpressure, long startTimeNanos)
    {
        if (connection.getVersion().isGreaterOrEqualTo(ProtocolVersion.V4))
            ClientWarn.instance.captureWarnings();

        // even if ClientWarn is disabled, still setup CoordinatorTrackWarnings, as this will populate metrics and
        // emit logs on the server; the warnings will just be ignored and not sent to the client
        if (request.isTrackable())
            CoordinatorWarnings.init();

        if (backpressure == Overload.REQUESTS)
        {
            String message = String.format("Request breached global limit of %d requests/second and triggered backpressure.",
                                           ClientResourceLimits.getNativeTransportMaxRequestsPerSecond());

            NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, 1, TimeUnit.MINUTES, message);
            ClientWarn.instance.warn(message);
        }
        else if (backpressure == Overload.BYTES_IN_FLIGHT)
        {
            String message = String.format("Request breached limit(s) on bytes in flight (Endpoint: %d, Global: %d) and triggered backpressure.",
                                           ClientResourceLimits.getEndpointLimit(), ClientResourceLimits.getGlobalLimit());

            NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, 1, TimeUnit.MINUTES, message);
            ClientWarn.instance.warn(message);
        }

        QueryState qstate = connection.validateNewMessage(request.type, connection.getVersion());

        Message.logger.trace("Received: {}, v={}", request, connection.getVersion());
        connection.requests.inc();
        Message.Response response = request.execute(qstate, startTimeNanos);

        if (request.isTrackable())
            CoordinatorWarnings.done();

        response.setStreamId(request.getStreamId());
        response.setWarnings(ClientWarn.instance.getWarnings());
        response.attach(connection);
        connection.applyStateTransition(request.type, response.type);
        return response;
    }
    
    /**
     * Note: this method may be executed on the netty event loop.
     */
    static Message.Response processRequest(Channel channel, Message.Request request, Overload backpressure, long approxStartTimeNanos)
    {
        try
        {
            return processRequest((ServerConnection) request.connection(), request, backpressure, approxStartTimeNanos);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);

            if (request.isTrackable())
                CoordinatorWarnings.done();

            Predicate<Throwable> handler = ExceptionHandlers.getUnexpectedExceptionHandler(channel, true);
            ErrorMessage error = ErrorMessage.fromException(t, handler);
            error.setStreamId(request.getStreamId());
            error.setWarnings(ClientWarn.instance.getWarnings());
            return error;
        }
        finally
        {
            CoordinatorWarnings.reset();
            ClientWarn.instance.resetWarnings();
        }
    }

    /**
     * Note: this method is not expected to execute on the netty event loop.
     */
    void processRequest(Channel channel, Message.Request request, FlushItemConverter forFlusher, Overload backpressure, long approxStartTimeNanos)
    {
        Message.Response response = processRequest(channel, request, backpressure, approxStartTimeNanos);
        FlushItem<?> toFlush = forFlusher.toFlushItem(channel, request, response);
        Message.logger.trace("Responding: {}, v={}", response, request.connection().getVersion());
        flush(toFlush);
    }

    private void flush(FlushItem<?> item)
    {
        EventLoop loop = item.channel.eventLoop();
        Flusher flusher = flusherLookup.get(loop);
        if (flusher == null)
        {
            Flusher created = useLegacyFlusher ? Flusher.legacy(loop) : Flusher.immediate(loop);
            Flusher alt = flusherLookup.putIfAbsent(loop, flusher = created);
            if (alt != null)
                flusher = alt;
        }

        flusher.enqueue(item);
        flusher.start();
    }

    public static void shutdown()
    {
        requestExecutor.shutdown();
        authExecutor.shutdown();
    }

    /**
     * Dispatcher for EventMessages. In {@link Server.ConnectionTracker#send(Event)}, the strategy
     * for delivering events to registered clients is dependent on protocol version and the configuration
     * of the pipeline. For v5 and newer connections, the event message is encoded into an Envelope,
     * wrapped in a FlushItem and then delivered via the pipeline's flusher, in a similar way to
     * a Response returned from {@link #processRequest(Channel, Message.Request, FlushItemConverter, Overload, long)}.
     * It's worth noting that events are not generally fired as a direct response to a client request,
     * so this flush item has a null request attribute. The dispatcher itself is created when the
     * pipeline is first configured during protocol negotiation and is attached to the channel for
     * later retrieval.
     *
     * Pre-v5 connections simply write the EventMessage directly to the pipeline.
     */
    static final AttributeKey<Consumer<EventMessage>> EVENT_DISPATCHER = AttributeKey.valueOf("EVTDISP");
    Consumer<EventMessage> eventDispatcher(final Channel channel,
                                           final ProtocolVersion version,
                                           final FrameEncoder.PayloadAllocator allocator)
    {
        return eventMessage -> flush(new FlushItem.Framed(channel,
                                                          eventMessage.encode(version),
                                                          null,
                                                          allocator,
                                                          f -> f.response.release()));
    }
}
