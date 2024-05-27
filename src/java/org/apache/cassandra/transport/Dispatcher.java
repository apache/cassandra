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
import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.LocalAwareExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.OverloadedException;
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

public class Dispatcher implements CQLMessageHandler.MessageConsumer<Message.Request>
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

    @Override
    public void dispatch(Channel channel, Message.Request request, FlushItemConverter forFlusher, Overload backpressure)
    {
        requestExecutor.submit(new RequestProcessor(channel, request, forFlusher, backpressure));
        ClientMetrics.instance.markRequestDispatched();
    }

    public static class RequestTime
    {
        private final long enqueuedAtNanos;
        private final long startedAtNanos;

        public RequestTime(long createdAtNanos)
        {
            this(createdAtNanos, createdAtNanos);
        }

        public RequestTime(long enqueuedAtNanos, long startedAtNanos)
        {
            this.enqueuedAtNanos = enqueuedAtNanos;
            this.startedAtNanos = startedAtNanos;
        }

        public static RequestTime forImmediateExecution()
        {
            return new RequestTime(MonotonicClock.Global.preciseTime.now());
        }

        public long startedAtNanos()
        {
            return startedAtNanos;
        }

        public long enqueuedAtNanos()
        {
            return enqueuedAtNanos;
        }

        /**
         * Base time is used by timeouts, and can be set to either when the request was added to the queue,
         * or when the processing has started. Since client read/write timeouts are usually aligned with
         * server-side timeouts, it is desireable to use enqueue time as a base. However, since client removes
         * the handler `readTimeoutMillis` (which is 12 seconds by default), the upper bound for any execution on
         * the coordinator is 12 seconds (thanks to CASSANDRA-7392, any replica-side query is capped by the verb timeout),
         * if REQUEST option is used. But even simply allowing such long timeouts also implicitly allows queues to grow
         * large, since our queues are currently unbounded.
         *
         * Latency, however, is _always_ based on request processing time, since the amount of time that request spends
         * in the queue is not a representative metric of replica performance.
         */
        public long baseTimeNanos()
        {
            switch (DatabaseDescriptor.getCQLStartTime())
            {
                case REQUEST:
                    return startedAtNanos();
                case QUEUE:
                    return enqueuedAtNanos();
                default:
                    throw new IllegalArgumentException("Unknown start time: " + DatabaseDescriptor.getCQLStartTime());
            }
        }

        /**
         * Given the current time and a base timeout for the verb return a request's expiration deadline,
         * the time at which it becomes eligible for load shedding.
         * The two factors to consider are the per-verb and client timeouts. Both are calculated by subtracting the
         * time already elapsed during the lifetime of the request from some base value.
         *
         * When deriving verb timeout, two alternative semantics are available. This timeout may represent either:
         *  * the total time available for a coordinator to process a client request and return its response
         *  * a time bound for a coordinator to send internode requests and gather responses from replicas
         *
         * The point from which elapsed time is measured here is configurable to accommodate these two different
         * options. For the former, the clock starts when a client request is received and enqueued by the coordinator.
         * For the latter, it starts when the request is dequeued by the coordinator and processing is started.
         * See {@link #baseTimeNanos()} for details.
         *
         * The client timeout represents how long the sender of a request is prepared to wait for a response. By
         * implication, after this window has passed any further work done on the server side is wasted effort. Ideally,
         * the base for this timeout would be set on a per-request basis but as this not currently supported in the
         * protocol, it is configured uniformly for all requests. See {@DatabaseDescriptor#getNativeTransportTimeout}.
         * For this calculation, elapsed time is always measured from the point when a request is received and enqueued.
         *
         * Where verb timeout is based on queue admission, deadline computation is straightforward. The expiration
         * deadline is simply the current time plus the smaller of the verb and client timeouts.
         *
         * However, if verb timeout is based on when the request is dequeued, the implications are more nuanced.
         * In this scenario, while there may still be "headroom" available within the verb timeout, using it could
         * exceed the client timeout (which is always based on admission time).
         *
         * For example:
         *
         * * Client timeout base is 10 (cb), verb timeout base is 5 (vb)
         * * Request is enqueued at t1 (e)
         * * Request is dequeued at t8 (d)
         * * computeDeadline is called at t9 (n)
         *
         * If verb timeout is based on dequeuing, there would still some time remaining before a verb-based deadline.
         * elapsed  = (n - d)        ;  1
         * timeout  = (vb - elapsed) ;  4
         * deadline = (n + timeout)  ;  t13
         * ostensibly, the coordinator has until t13 to complete processing
         *
         * But as client timeout is measured from admission time, the request may exceeded the maximum wait period for
         * the client sooner.
         * elapsed  = (n - e)        ;  8
         * timeout  = (cb - elapsed) ;  2
         * deadline = (n + timeout)  ;  t11
         * So the coordinator actually only has until t11 to complete processing, beyond then the client will not accept
         * any response.
         *
         * @param verbExpiresAfterNanos the base timeout value for the verb being executed
         * @return the point in time after which no further processing should occur
         */
        public long computeDeadline(long verbExpiresAfterNanos)
        {
            long clientDeadline = clientDeadline();

            long verbDeadline = baseTimeNanos() + verbExpiresAfterNanos;
            // Whichever one is closer
            return Math.min(verbDeadline, clientDeadline);
        }

        public long computeTimeout(long now, long verbExpiresAfterNanos)
        {
            return computeDeadline(verbExpiresAfterNanos) - now;
        }

        /**
         * No request should survive native request deadline, but in order to err on the side of caution, we have this
         * swtich that allows hints to be submitted to mutation stage when cluster is potentially overloaded. Allowing
         * hints to be not bound by deadline can exacerbate overload, but since there are also correctness implications,
         * this seemed like a reasonable configuration option.
         */
        public boolean shouldSendHints()
        {
            if (!DatabaseDescriptor.getEnforceNativeDeadlineForHints())
                return true;

            long now = MonotonicClock.Global.preciseTime.now();
            long clientDeadline = clientDeadline();
            return now < clientDeadline;
        }

        public long clientDeadline()
        {
            return enqueuedAtNanos() + DatabaseDescriptor.getNativeTransportTimeout(TimeUnit.NANOSECONDS);
        }

        public long timeSpentInQueueNanos()
        {
            return startedAtNanos - enqueuedAtNanos;
        }
    }

    /**
     * It is important to keep this class an instance of {@link DebuggableTask}, either runnable or callable since this
     * is the only way we can keep it not wrapped into a callable on SEPExecutor submission path. And we need this
     * functionality for tracking time purposes.
     */
    public class RequestProcessor implements DebuggableTask.RunnableDebuggableTask
    {
        private final Channel channel;
        private final Message.Request request;
        private final FlushItemConverter forFlusher;
        private final Overload backpressure;

        private volatile long startTimeNanos;

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
            startTimeNanos = MonotonicClock.Global.preciseTime.now();
            processRequest(channel, request, forFlusher, backpressure, new RequestTime(request.createdAtNanos, startTimeNanos));
        }

        @Override
        public long creationTimeNanos()
        {
            return request.createdAtNanos;
        }

        @Override
        public long startTimeNanos()
        {
            return startTimeNanos;
        }

        @Override
        public String description()
        {
            return request.toString();
        }

        @Override
        public String toString()
        {
            return "RequestProcessor{" +
                   "request=" + request +
                   ", approxStartTimeNanos=" + startTimeNanos +
                   '}';
        }
    }

    /**
     * Checks if the item in the head of the queue has spent more than allowed time in the queue.
     */
    @Override
    public boolean hasQueueCapacity()
    {
        double threshold = DatabaseDescriptor.getNativeTransportQueueMaxItemAgeThreshold();
        if (threshold <= 0)
            return true;

        return requestExecutor.oldestTaskQueueTime() < (DatabaseDescriptor.getNativeTransportTimeout(TimeUnit.NANOSECONDS) * threshold);
    }

    /**
     * Note: this method may be executed on the netty event loop, during initial protocol negotiation; the caller is
     * responsible for cleaning up any global or thread-local state. (ex. tracing, client warnings, etc.).
     */
    private static Message.Response processRequest(ServerConnection connection, Message.Request request, Overload backpressure, RequestTime requestTime)
    {
        long queueTime = requestTime.timeSpentInQueueNanos();

        // If we have already crossed the max timeout for all possible RPCs, we time out the query immediately.
        // We do not differentiate between query types here, since if we got into a situation when, say, we have a PREPARE
        // query that is stuck behind the EXECUTE query, we would rather time it out and catch up with a backlog, expecting
        // that the bursts are going to be short-lived.
        ClientMetrics.instance.queueTime(queueTime, TimeUnit.NANOSECONDS);
        if (queueTime > DatabaseDescriptor.getNativeTransportTimeout(TimeUnit.NANOSECONDS))
        {
            ClientMetrics.instance.markTimedOutBeforeProcessing();
            return ErrorMessage.fromException(new OverloadedException("Query timed out before it could start"));
        }

        if (connection.getVersion().isGreaterOrEqualTo(ProtocolVersion.V4))
            ClientWarn.instance.captureWarnings();

        // even if ClientWarn is disabled, still setup CoordinatorTrackWarnings, as this will populate metrics and
        // emit logs on the server; the warnings will just be ignored and not sent to the client
        if (request.isTrackable())
            CoordinatorWarnings.init();

        switch (backpressure)
        {
            case NONE:
                break;
            case REQUESTS:
            {
                String message = String.format("Request breached global limit of %d requests/second and triggered backpressure.",
                                               ClientResourceLimits.getNativeTransportMaxRequestsPerSecond());

                NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, 1, TimeUnit.MINUTES, message);
                ClientWarn.instance.warn(message);
                break;
            }
            case BYTES_IN_FLIGHT:
            {
                String message = String.format("Request breached limit(s) on bytes in flight (Endpoint: %d, Global: %d) and triggered backpressure.",
                                               ClientResourceLimits.getEndpointLimit(), ClientResourceLimits.getGlobalLimit());

                NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, 1, TimeUnit.MINUTES, message);
                ClientWarn.instance.warn(message);
                break;
            }
            case QUEUE_TIME:
            {
                String message = String.format("Request has spent over %s time of the maximum timeout %dms in the queue",
                                               DatabaseDescriptor.getNativeTransportQueueMaxItemAgeThreshold(),
                                               DatabaseDescriptor.getNativeTransportTimeout(TimeUnit.MILLISECONDS));

                NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, 1, TimeUnit.MINUTES, message);
                ClientWarn.instance.warn(message);
                break;
            }
        }

        QueryState qstate = connection.validateNewMessage(request.type, connection.getVersion());

        Message.logger.trace("Received: {}, v={}", request, connection.getVersion());
        connection.requests.inc();
        Message.Response response = request.execute(qstate, requestTime);

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
    static Message.Response processRequest(Channel channel, Message.Request request, Overload backpressure, RequestTime requestTime)
    {
        try
        {
            return processRequest((ServerConnection) request.connection(), request, backpressure, requestTime);
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
    void processRequest(Channel channel, Message.Request request, FlushItemConverter forFlusher, Overload backpressure, RequestTime requestTime)
    {
        Message.Response response = processRequest(channel, request, backpressure, requestTime);
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
     * a Response returned from {@link #processRequest(Channel, Message.Request, FlushItemConverter, Overload, RequestTime)}.
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
