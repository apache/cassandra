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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.LocalAwareExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ExecuteCommandStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;
import org.apache.cassandra.service.writes.thresholds.CoordinatorWriteWarnings;
import org.apache.cassandra.transport.ClientResourceLimits.Overload;
import org.apache.cassandra.transport.Flusher.FlushItem;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;

import static org.apache.cassandra.concurrent.SharedExecutorPool.SHARED;
import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

public class Dispatcher implements CQLMessageHandler.MessageConsumer<Message.Request>
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

    /**
     * Executor for handling requests from management connections (part of CEP-38).
     *
     * <p>Management connections are identified via Connection's flag set by the management
     * transport server at initial connection setup. Their requests are then routed to a dedicated
     * executor instead of the standard {@link #requestExecutor}, so management operations keep
     * making progress while the client pool is saturated.
     *
     * <p>The executor is sized separately via
     * {@link DatabaseDescriptor#getNativeTransportManagementMaxThreads()}.
     *
     * <p>Management connections are established through the management transport server
     * (see {@link org.apache.cassandra.service.NativeTransportManagementService}), which listens
     * on a separate port from the regular native transport.
     *
     * <p>Unlike auth requests, management requests have no fallback executor, so values
     * less than 1 are treated as 1: a zero-sized pool would queue management requests forever.
     */
    @VisibleForTesting
    static final LocalAwareExecutorPlus managementExecutor = SHARED.newExecutor(Math.max(1, DatabaseDescriptor.getNativeTransportManagementMaxThreads()),
                                                                                 DatabaseDescriptor::setNativeTransportManagementMaxThreads,
                                                                                 "transport",
                                                                                 "Native-Transport-Management-Tasks");

    private static final ConcurrentMap<EventLoop, Flusher> flusherLookup = new ConcurrentHashMap<>();

    /**
     * Set while a management request is executing on the current thread. A management command may itself
     * initiate the management server's stop (e.g. INVOKE COMMAND stopdaemon), in which case the drain-wait
     * in {@link Server#close} runs on this very thread and must not wait for its own task to complete.
     */
    private static final ThreadLocal<Boolean> IN_MANAGEMENT_TASK = ThreadLocal.withInitial(() -> Boolean.FALSE);

    private final boolean useLegacyFlusher;
    private final boolean isManagementDispatcher;

    /**
     * Takes a Channel, Request and the Response produced by processRequest and outputs a FlushItem
     * appropriate for the pipeline, which is specific to the protocol version. V5 and above will
     * produce FlushItem.Framed instances whereas earlier versions require FlushItem.Unframed.
     * The instances of these FlushItem subclasses are specialized to release resources in the
     * right way for the specific pipeline that produced them.
     */
    public interface FlushItemConverter<P>
    {
        FlushItem<?> toFlushItem(P param, Channel channel, Message.Request request, Message.Response response);
    }

    public Dispatcher(boolean useLegacyFlusher, boolean isManagementDispatcher)
    {
        this.useLegacyFlusher = useLegacyFlusher;
        this.isManagementDispatcher = isManagementDispatcher;
    }

    @Override
    public <P> void dispatch(Channel channel, Message.Request request, FlushItemConverter<P> forFlusher, P param, Overload backpressure)
    {
        if (!request.connection().getTracker().isRunning())
        {
            // We can not respond with a custom, transport, or server exceptions since, given current implementation of clients,
            // they will defunct the connection. Without a protocol version bump that introduces an "I am going away message",
            // we have to stick to an existing error code.
            Message.Response response = ErrorMessage.fromTransportException(new OverloadedException("Server is shutting down"));
            response.setWarnings(ClientWarn.instance.getWarnings());
            response.attach(request.connection);
            FlushItem<?> toFlush = forFlusher.toFlushItem(param, channel, request, response);
            flush(toFlush);
            return;
        }

        // Count every request accepted for dispatch. This runs on the connection's Netty event loop.
        ((ServerConnection) request.connection()).incrementRequests();

        // if native_transport_max_auth_threads is < 1, don't delegate to new pool on auth messages
        boolean isAuthQuery = DatabaseDescriptor.getNativeTransportMaxAuthThreads() > 0 &&
                              (request.type == Message.Type.AUTH_RESPONSE || request.type == Message.Type.CREDENTIALS);

        if (isAuthQuery)
        {
            // Importantly, the authExecutor will handle the AUTHENTICATE message which may be CPU intensive.
            authExecutor.submit(new RequestProcessor<>(channel, request, forFlusher, param, backpressure));
            ClientMetrics.instance.markRequestDispatched();
            return;
        }

        // Check the connection object rather than the channel attributes, which should be cheaper on every
        // request. Management connections are routed to the management executor.
        Connection connection = request.connection();
        if (connection instanceof ServerConnection)
        {
            ServerConnection serverConnection = (ServerConnection) connection;
            if (serverConnection.isManagementConnection())
            {
                // Intentionally skipping ClientMetrics calls here: that meter tracks regular client request
                // dispatch, and management API requests have their own metrics rather than being mixed
                // into the client request rate.
                managementExecutor.submit(new ManagementRequestProcessor<>(channel, request, forFlusher, param, backpressure));
                return;
            }
        }

        requestExecutor.submit(new RequestProcessor<>(channel, request, forFlusher, param, backpressure));
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
            Preconditions.checkArgument(enqueuedAtNanos != -1);
            this.enqueuedAtNanos = enqueuedAtNanos;
            this.startedAtNanos = startedAtNanos;
        }

        public static RequestTime forImmediateExecution()
        {
            return new RequestTime(MonotonicClock.Global.preciseTime.now());
        }

        public RequestTime withStartedAt(long startedAtNanos)
        {
            return new RequestTime(enqueuedAtNanos, startedAtNanos);
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
         * or when the processing has started, which is controlled by {@link DatabaseDescriptor#getCQLStartTime()}
         *
         * Since client read/write timeouts are usually aligned with server-side timeouts, it is desireable to use
         * enqueue time as a base. However, since client removes the handler `readTimeoutMillis` (which is 12 seconds
         * by default), the upper bound for any execution on the coordinator is 12 seconds (thanks to CASSANDRA-7392,
         * any replica-side query is capped by the verb timeout), if REQUEST option is used. But even simply allowing
         * such long timeouts also implicitly allows queues to grow large, since our queues are currently unbounded.
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
         * protocol, it is configured uniformly for all requests. See {@link DatabaseDescriptor#getNativeTransportTimeout}.
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
    public class RequestProcessor<P> implements DebuggableTask.RunnableDebuggableTask
    {
        protected final Channel channel;
        protected final Message.Request request;
        protected final FlushItemConverter<P> forFlusher;
        protected final P flusherParam;
        protected final Overload backpressure;

        protected volatile long startTimeNanos;

        public RequestProcessor(Channel channel, Message.Request request, FlushItemConverter<P> forFlusher, P flusherParam, Overload backpressure)
        {
            this.channel = channel;
            this.request = request;
            this.forFlusher = forFlusher;
            this.flusherParam = flusherParam;
            this.backpressure = backpressure;
        }

        @Override
        public void run()
        {
            startTimeNanos = MonotonicClock.Global.preciseTime.now();
            processRequest(channel, request, forFlusher, flusherParam, backpressure, new RequestTime(request.createdAtNanos, startTimeNanos));
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

    /** RequestProcessor for management connections that validates before executing. */
    private class ManagementRequestProcessor<P> extends RequestProcessor<P> {

        public ManagementRequestProcessor(Channel channel,
                                          Message.Request request,
                                          FlushItemConverter<P> forFlusher,
                                          P flusherParam,
                                          Overload backpressure) {
            super(channel, request, forFlusher, flusherParam, backpressure);
        }

        @Override
        public void run() {
            startTimeNanos = MonotonicClock.Global.preciseTime.now();
            RequestTime requestTime = new RequestTime(request.createdAtNanos, startTimeNanos);

            // Validate the management request before executing it
            Connection connection = request.connection();
            if (connection instanceof ServerConnection) {
                ServerConnection serverConnection = (ServerConnection) connection;
                if (serverConnection.isManagementConnection()) {
                    if (!isManagementRequestAllowed(request)) {
                        // The flush pipeline takes the stream id from the request envelope, so the
                        // response must not carry one of its own.
                        Message.Response response = ErrorMessage.fromExceptionNoStreamId(
                        new InvalidRequestException(
                            "Only executions of the INVOKE COMMAND statements are allowed on the management port."));
                        response.attach(connection);
                        FlushItem<?> toFlush = forFlusher.toFlushItem(flusherParam, channel, request, response);
                        flush(toFlush);
                        return;
                    }
                }
            }

            IN_MANAGEMENT_TASK.set(Boolean.TRUE);
            try {
                processRequest(channel, request, forFlusher, flusherParam, backpressure, requestTime);
            } finally {
                IN_MANAGEMENT_TASK.set(Boolean.FALSE);
            }
        }
    }

    @VisibleForTesting
    static boolean isManagementRequestAllowed(Message.Request request)
    {
        switch (request.type)
        {
            case QUERY:
                try
                {
                    // Early parse the query to check if it's an INVOKE COMMAND statement.
                    // For management non-intensive operations double parsing is probably acceptable.
                    CQLStatement.Raw rawStatement = QueryProcessor.parseStatement(((QueryMessage) request).query);
                    if (rawStatement instanceof ExecuteCommandStatement.Raw)
                        return true;

                    // Allow read-only SELECT queries on system keyspaces (needed for driver metadata
                    // discovery), except system_auth (see isManagementReadableSystemKeyspace).
                    if (rawStatement instanceof SelectStatement.RawStatement)
                    {
                        SelectStatement.RawStatement selectRaw = (SelectStatement.RawStatement) rawStatement;
                        return selectRaw.isFullyQualified()
                               && isManagementReadableSystemKeyspace(selectRaw.keyspace());
                    }

                    // This is also a corner case for the driver's behavior on the management port.
                    // When connecting, the driver sends a USE statement for the keyspace provided
                    // in driver.connect("system_schema").
                    if (rawStatement instanceof UseStatement)
                    {
                        UseStatement useStatement = (UseStatement) rawStatement;
                        return isManagementReadableSystemKeyspace(useStatement.keyspace());
                    }

                    return false;
                }
                catch (Exception e)
                {
                    logger.warn("The command request parsing failed. The command will not be executed: {}", e.getMessage());
                    // If parsing fails (syntax error, etc.), it's not a valid command statement;
                    // this is expected for non-command queries.
                    return false;
                }
            case STARTUP:
            case CREDENTIALS:
            case AUTH_RESPONSE:
            case OPTIONS:
            case REGISTER:
                return true; // Protocol messages are always allowed.
            case EXECUTE:
            case PREPARE:
            case BATCH:
            default:
                return false; // Not supported and not allowed on management connections.
        }
    }

    /**
     * System keyspaces a CQL driver may read/USE on the management port for metadata discovery. This is
     * {@link SchemaConstants#isSystemKeyspace(String)} (which also covers virtual system keyspaces) minus
     * {@code system_auth}: that keyspace is the credential store (bcrypt {@code salted_hash}, superuser
     * flags) and must never be readable over the management port, which is reachable without authorization
     * (if AllowAllAuthorizer is enabled).
     */
    private static boolean isManagementReadableSystemKeyspace(String keyspace)
    {
        if (keyspace == null)
            return false;
        if (SchemaConstants.AUTH_KEYSPACE_NAME.equals(toLowerCaseLocalized(keyspace)))
            return false;
        return SchemaConstants.isSystemKeyspace(keyspace)
               || SchemaConstants.isVirtualSystemKeyspace(keyspace);
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

        LocalAwareExecutorPlus executor = isManagementDispatcher ? managementExecutor : requestExecutor;
        return executor.oldestTaskQueueTime() < (DatabaseDescriptor.getNativeTransportTimeout(TimeUnit.NANOSECONDS) * threshold);
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
            return ErrorMessage.fromTransportException(new OverloadedException("Query timed out before it could start"));
        }

        if (connection.getVersion().isGreaterOrEqualTo(ProtocolVersion.V4))
            ClientWarn.instance.captureWarnings();

        // even if ClientWarn is disabled, still setup CoordinatorTrackWarnings, as this will populate metrics and
        // emit logs on the server; the warnings will just be ignored and not sent to the client
        if (request.isTrackable())
        {
            CoordinatorWarnings.init();
            CoordinatorWriteWarnings.init();
        }

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
        Message.Response response = request.execute(qstate, requestTime);

        if (request.isTrackable())
        {
            CoordinatorWarnings.done();
            CoordinatorWriteWarnings.done();
        }

        response.attach(connection);
        connection.applyStateTransition(request.type, response.type);
        return response;
    }

    /**
     * Note: this method may be executed on the netty event loop.
     */
    static Message.Response processRequest(Channel channel, Message.Request request, Overload backpressure, RequestTime requestTime)
    {
        Message.Response response = null;
        try
        {
            response = processRequest((ServerConnection) request.connection(), request, backpressure, requestTime);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);

            if (request.isTrackable())
            {
                CoordinatorWarnings.done();
                CoordinatorWriteWarnings.done();
            }

            Predicate<Throwable> handler = ExceptionHandlers.getUnexpectedExceptionHandler(channel, true);
            response = ErrorMessage.fromExceptionNoStreamId(t, handler);
        }
        finally
        {
            if (response != null)
                response.setWarnings(ClientWarn.instance.getWarnings());
            CoordinatorWarnings.reset();
            CoordinatorWriteWarnings.reset();
            ClientWarn.instance.resetWarnings();
        }
        return response;
    }

    /**
     * Note: this method is not expected to execute on the netty event loop.
     */
    <P> void processRequest(Channel channel, Message.Request request, FlushItemConverter<P> forFlusher, P param, Overload backpressure, RequestTime requestTime)
    {
        Message.Response response = processRequest(channel, request, backpressure, requestTime);
        FlushItem<?> toFlush = forFlusher.toFlushItem(param, channel, request, response);
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

    /**
     * @return true once the executor serving this dispatcher's server has drained: {@link #managementExecutor}
     * for the management transport, {@link #requestExecutor} otherwise. A management task running on the
     * calling thread is excluded, since it is the task that initiated the stop and cannot drain before it.
     */
    public boolean isDone()
    {
        LocalAwareExecutorPlus executor = isManagementDispatcher ? managementExecutor : requestExecutor;
        int self = isManagementDispatcher && IN_MANAGEMENT_TASK.get() ? 1 : 0;
        return executor.getPendingTaskCount() == 0 && executor.getActiveTaskCount() - self <= 0;
    }

    public static void shutdown()
    {
        requestExecutor.shutdown();
        authExecutor.shutdown();
        managementExecutor.shutdown();
    }

    /**
     * Dispatcher for EventMessages. In {@link Server.ConnectionTracker#send(Event)}, the strategy
     * for delivering events to registered clients is dependent on protocol version and the configuration
     * of the pipeline. For v5 and newer connections, the event message is encoded into an Envelope,
     * wrapped in a FlushItem and then delivered via the pipeline's flusher, in a similar way to
     * a Response returned from {@link #processRequest(Channel, Message.Request, FlushItemConverter, Object, Overload, RequestTime)}.
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
                                                          eventMessage.encode(version, EventMessage.EVENT_MESSAGE_STREAM_ID), // -1 was set in EventMessage previously
                                                          null,
                                                          allocator,
                                                          f -> f.responseEnvelope.release()));
    }
}
