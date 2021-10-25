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

import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;
import org.apache.cassandra.concurrent.LocalAwareExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.reads.trackwarnings.CoordinatorWarnings;
import org.apache.cassandra.transport.ClientResourceLimits.Overload;
import org.apache.cassandra.transport.Flusher.FlushItem;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.concurrent.SharedExecutorPool.SHARED;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class Dispatcher
{
    private static final Logger logger = LoggerFactory.getLogger(Dispatcher.class);
    
    private static final LocalAwareExecutorPlus requestExecutor = SHARED.newExecutor(DatabaseDescriptor.getNativeTransportMaxThreads(),
                                                                                     DatabaseDescriptor::setNativeTransportMaxThreads,
                                                                                     "transport",
                                                                                     "Native-Transport-Requests");

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
        requestExecutor.submit(() -> processRequest(channel, request, forFlusher, backpressure));
    }

    /**
     * Note: this method may be executed on the netty event loop, during initial protocol negotiation; the caller is
     * responsible for cleaning up any global or thread-local state. (ex. tracing, client warnings, etc.).
     */
    private static Message.Response processRequest(ServerConnection connection, Message.Request request, Overload backpressure)
    {
        long queryStartNanoTime = nanoTime();
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
        Message.Response response = request.execute(qstate, queryStartNanoTime);

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
    static Message.Response processRequest(Channel channel, Message.Request request, Overload backpressure)
    {
        try
        {
            return processRequest((ServerConnection) request.connection(), request, backpressure);
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
    void processRequest(Channel channel, Message.Request request, FlushItemConverter forFlusher, Overload backpressure)
    {
        Message.Response response = processRequest(channel, request, backpressure);
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
        if (requestExecutor != null)
        {
            requestExecutor.shutdown();
        }
    }


    /**
     * Dispatcher for EventMessages. In {@link Server.ConnectionTracker#send(Event)}, the strategy
     * for delivering events to registered clients is dependent on protocol version and the configuration
     * of the pipeline. For v5 and newer connections, the event message is encoded into an Envelope,
     * wrapped in a FlushItem and then delivered via the pipeline's flusher, in a similar way to
     * a Response returned from {@link #processRequest(Channel, Message.Request, FlushItemConverter, Overload)}.
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
