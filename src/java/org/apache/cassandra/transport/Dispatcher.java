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
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;
import org.apache.cassandra.concurrent.LocalAwareExecutorService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Flusher.FlushItem;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.concurrent.SharedExecutorPool.SHARED;

public class Dispatcher
{
    @VisibleForTesting
    static final LocalAwareExecutorService requestExecutor = SHARED.newExecutor(DatabaseDescriptor.getNativeTransportMaxThreads(),
                                                                                        DatabaseDescriptor::setNativeTransportMaxThreads,
                                                                                        "transport",
                                                                                        "Native-Transport-Requests");

    /** CASSANDRA-17812: Rate-limit new client connection setup to avoid overwhelming during bcrypt
     *
     * Backported by CASSANDRA-20057
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
    static final LocalAwareExecutorService authExecutor = SHARED.newExecutor(Math.max(1, DatabaseDescriptor.getNativeTransportMaxAuthThreads()),
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

    public void dispatch(Channel channel, Message.Request request, FlushItemConverter forFlusher)
    {
        // if native_transport_max_auth_threads is < 1, don't delegate to new pool on auth messages
        boolean isAuthQuery = DatabaseDescriptor.getNativeTransportMaxAuthThreads() > 0 &&
                              (request.type == Message.Type.AUTH_RESPONSE || request.type == Message.Type.CREDENTIALS);

        // Importantly, the authExecutor will handle the AUTHENTICATE message which may be CPU intensive.
        LocalAwareExecutorService executor = isAuthQuery ? authExecutor : requestExecutor;

        executor.submit(() -> processRequest(channel, request, forFlusher));
    }

    /**
     * Note: this method may be executed on the netty event loop, during initial protocol negotiation
     */
    static Message.Response processRequest(ServerConnection connection, Message.Request request)
    {
        long queryStartNanoTime = System.nanoTime();
        if (connection.getVersion().isGreaterOrEqualTo(ProtocolVersion.V4))
            ClientWarn.instance.captureWarnings();

        QueryState qstate = connection.validateNewMessage(request.type, connection.getVersion());

        Message.logger.trace("Received: {}, v={}", request, connection.getVersion());
        connection.requests.inc();
        Message.Response response = request.execute(qstate, queryStartNanoTime);
        response.setStreamId(request.getStreamId());
        response.setWarnings(ClientWarn.instance.getWarnings());
        response.attach(connection);
        connection.applyStateTransition(request.type, response.type);
        return response;
    }

    /**
     * Note: this method is not expected to execute on the netty event loop.
     */
    void processRequest(Channel channel, Message.Request request, FlushItemConverter forFlusher)
    {
        final Message.Response response;
        final ServerConnection connection;
        FlushItem<?> toFlush;
        try
        {
            assert request.connection() instanceof ServerConnection;
            connection = (ServerConnection) request.connection();
            response = processRequest(connection, request);
            toFlush = forFlusher.toFlushItem(channel, request, response);
            Message.logger.trace("Responding: {}, v={}", response, connection.getVersion());
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            ExceptionHandlers.UnexpectedChannelExceptionHandler handler = new ExceptionHandlers.UnexpectedChannelExceptionHandler(channel, true);
            ErrorMessage error = ErrorMessage.fromException(t, handler);
            error.setStreamId(request.getStreamId());
            toFlush = forFlusher.toFlushItem(channel, request, error);
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
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
     * a Response returned from {@link #processRequest(Channel, Message.Request, FlushItemConverter)}.
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
