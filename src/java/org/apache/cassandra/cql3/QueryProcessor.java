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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.antlr.runtime.*;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.selection.ResultSetBuilder;
import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.metrics.CQLMetrics;
import org.apache.cassandra.service.*;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import static org.apache.cassandra.config.CassandraRelevantProperties.ENABLE_NODELOCAL_QUERIES;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class QueryProcessor implements QueryHandler
{
    public static final CassandraVersion CQL_VERSION = new CassandraVersion("3.4.7");

    // See comments on QueryProcessor #prepare
    public static final CassandraVersion NEW_PREPARED_STATEMENT_BEHAVIOUR_SINCE_40 = new CassandraVersion("4.0.2");

    public static final QueryProcessor instance = new QueryProcessor();

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

    private static final Cache<MD5Digest, Prepared> preparedStatements;

    // A map for prepared statements used internally (which we don't want to mix with user statement, in particular we don't
    // bother with expiration on those.
    private static final ConcurrentMap<String, Prepared> internalStatements = new ConcurrentHashMap<>();

    // Direct calls to processStatement do not increment the preparedStatementsExecuted/regularStatementsExecuted
    // counters. Callers of processStatement are responsible for correctly notifying metrics
    public static final CQLMetrics metrics = new CQLMetrics();

    private static final AtomicInteger lastMinuteEvictionsCount = new AtomicInteger(0);

    static
    {
        preparedStatements = Caffeine.newBuilder()
                             .executor(ImmediateExecutor.INSTANCE)
                             .maximumWeight(capacityToBytes(DatabaseDescriptor.getPreparedStatementsCacheSizeMiB()))
                             .weigher(QueryProcessor::measure)
                             .removalListener((key, prepared, cause) -> {
                                 MD5Digest md5Digest = (MD5Digest) key;
                                 if (cause.wasEvicted())
                                 {
                                     metrics.preparedStatementsEvicted.inc();
                                     lastMinuteEvictionsCount.incrementAndGet();
                                     SystemKeyspace.removePreparedStatement(md5Digest);
                                 }
                             }).build();

        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(() -> {
            long count = lastMinuteEvictionsCount.getAndSet(0);
            if (count > 0)
                logger.warn("{} prepared statements discarded in the last minute because cache limit reached ({} MiB)",
                            count,
                            DatabaseDescriptor.getPreparedStatementsCacheSizeMiB());
        }, 1, 1, TimeUnit.MINUTES);

        logger.info("Initialized prepared statement caches with {} MiB",
                    DatabaseDescriptor.getPreparedStatementsCacheSizeMiB());
    }

    private static long capacityToBytes(long cacheSizeMB)
    {
        return cacheSizeMB * 1024 * 1024;
    }

    public static int preparedStatementsCount()
    {
        return preparedStatements.asMap().size();
    }

    // Work around initialization dependency
    private enum InternalStateInstance
    {
        INSTANCE;

        private final ClientState clientState;

        InternalStateInstance()
        {
            clientState = ClientState.forInternalCalls(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        }
    }

    public void preloadPreparedStatements()
    {
        int count = SystemKeyspace.loadPreparedStatements((id, query, keyspace) -> {
            try
            {
                ClientState clientState = ClientState.forInternalCalls();
                if (keyspace != null)
                    clientState.setKeyspace(keyspace);

                Prepared prepared = parseAndPrepare(query, clientState, false);
                preparedStatements.put(id, prepared);

                // Preload `null` statement for non-fully qualified statements, since it can't be parsed if loaded from cache and will be dropped
                if (!prepared.fullyQualified)
                    preparedStatements.get(computeId(query, null), (ignored_) -> prepared);
                return true;
            }
            catch (RequestValidationException e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.warn(String.format("Prepared statement recreation error, removing statement: %s %s %s", id, query, keyspace));
                SystemKeyspace.removePreparedStatement(id);
                return false;
            }
        });
        logger.info("Preloaded {} prepared statements", count);
    }


    /**
     * Clears the prepared statement cache.
     * @param memoryOnly {@code true} if only the in memory caches must be cleared, {@code false} otherwise.
     */
    @VisibleForTesting
    public static void clearPreparedStatements(boolean memoryOnly)
    {
        preparedStatements.invalidateAll();
        if (!memoryOnly)
            SystemKeyspace.resetPreparedStatements();
    }

    @VisibleForTesting
    public static ConcurrentMap<String, Prepared> getInternalStatements()
    {
        return internalStatements;
    }

    @VisibleForTesting
    public static QueryState internalQueryState()
    {
        return new QueryState(InternalStateInstance.INSTANCE.clientState);
    }

    private QueryProcessor()
    {
        Schema.instance.registerListener(new StatementInvalidatingListener());
    }

    @VisibleForTesting
    public void evictPrepared(MD5Digest id)
    {
        preparedStatements.invalidate(id);
        SystemKeyspace.removePreparedStatement(id);
    }

    public HashMap<MD5Digest, Prepared> getPreparedStatements()
    {
        return new HashMap<>(preparedStatements.asMap());
    }

    public Prepared getPrepared(MD5Digest id)
    {
        return preparedStatements.getIfPresent(id);
    }

    public static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new InvalidRequestException("Key may not be empty");
        }
        if (key == ByteBufferUtil.UNSET_BYTE_BUFFER)
            throw new InvalidRequestException("Key may not be unset");

        // check that key can be handled by ByteArrayUtil.writeWithShortLength and ByteBufferUtil.writeWithShortLength
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
        }
    }

    public ResultMessage processStatement(CQLStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        logger.trace("Process {} @CL.{}", statement, options.getConsistency());
        ClientState clientState = queryState.getClientState();
        statement.authorize(clientState);
        statement.validate(clientState);

        ResultMessage result = options.getConsistency() == ConsistencyLevel.NODE_LOCAL
                             ? processNodeLocalStatement(statement, queryState, options)
                             : statement.execute(queryState, options, queryStartNanoTime);

        return result == null ? new ResultMessage.Void() : result;
    }

    private ResultMessage processNodeLocalStatement(CQLStatement statement, QueryState queryState, QueryOptions options)
    {
        if (!ENABLE_NODELOCAL_QUERIES.getBoolean())
            throw new InvalidRequestException("NODE_LOCAL consistency level is highly dangerous and should be used only for debugging purposes");

        if (statement instanceof BatchStatement || statement instanceof ModificationStatement)
            return processNodeLocalWrite(statement, queryState, options);
        else if (statement instanceof SelectStatement)
            return processNodeLocalSelect((SelectStatement) statement, queryState, options);
        else
            throw new InvalidRequestException("NODE_LOCAL consistency level can only be used with BATCH, UPDATE, INSERT, DELETE, and SELECT statements");
    }

    private ResultMessage processNodeLocalWrite(CQLStatement statement, QueryState queryState, QueryOptions options)
    {
        ClientRequestMetrics  levelMetrics = ClientRequestsMetricsHolder.writeMetricsForLevel(ConsistencyLevel.NODE_LOCAL);
        ClientRequestMetrics globalMetrics = ClientRequestsMetricsHolder.writeMetrics;

        long startTime = nanoTime();
        try
        {
            return statement.executeLocally(queryState, options);
        }
        finally
        {
            long latency = nanoTime() - startTime;
             levelMetrics.addNano(latency);
            globalMetrics.addNano(latency);
        }
    }

    private ResultMessage processNodeLocalSelect(SelectStatement statement, QueryState queryState, QueryOptions options)
    {
        ClientRequestMetrics  levelMetrics = ClientRequestsMetricsHolder.readMetricsForLevel(ConsistencyLevel.NODE_LOCAL);
        ClientRequestMetrics globalMetrics = ClientRequestsMetricsHolder.readMetrics;

        if (StorageService.instance.isBootstrapMode() && !SchemaConstants.isLocalSystemKeyspace(statement.keyspace()))
        {
            levelMetrics.unavailables.mark();
            globalMetrics.unavailables.mark();
            throw new IsBootstrappingException();
        }

        long startTime = nanoTime();
        try
        {
            return statement.executeLocally(queryState, options);
        }
        finally
        {
            long latency = nanoTime() - startTime;
             levelMetrics.addNano(latency);
            globalMetrics.addNano(latency);
        }
    }

    public static ResultMessage process(String queryString, ConsistencyLevel cl, QueryState queryState, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        QueryOptions options = QueryOptions.forInternalCalls(cl, Collections.<ByteBuffer>emptyList());
        CQLStatement statement = instance.parse(queryString, queryState, options);
        return instance.process(statement, queryState, options, queryStartNanoTime);
    }

    public CQLStatement parse(String queryString, QueryState queryState, QueryOptions options)
    {
        return getStatement(queryString, queryState.getClientState().cloneWithKeyspaceIfSet(options.getKeyspace()));
    }

    public ResultMessage process(CQLStatement statement,
                                 QueryState state,
                                 QueryOptions options,
                                 Map<String, ByteBuffer> customPayload,
                                 long queryStartNanoTime) throws RequestExecutionException, RequestValidationException
    {
        return process(statement, state, options, queryStartNanoTime);
    }

    public ResultMessage process(CQLStatement prepared, QueryState queryState, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        options.prepare(prepared.getBindVariables());
        if (prepared.getBindVariables().size() != options.getValues().size())
            throw new InvalidRequestException("Invalid amount of bind variables");

        if (!queryState.getClientState().isInternal)
            metrics.regularStatementsExecuted.inc();

        return processStatement(prepared, queryState, options, queryStartNanoTime);
    }

    public static CQLStatement parseStatement(String queryStr, ClientState clientState) throws RequestValidationException
    {
        return getStatement(queryStr, clientState);
    }

    public static UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        return process(query, cl, Collections.<ByteBuffer>emptyList());
    }

    public static UntypedResultSet process(String query, ConsistencyLevel cl, List<ByteBuffer> values) throws RequestExecutionException
    {
        QueryState queryState = QueryState.forInternalCalls();
        QueryOptions options = QueryOptions.forInternalCalls(cl, values);
        CQLStatement statement = instance.parse(query, queryState, options);
        ResultMessage result = instance.process(statement, queryState, options, nanoTime());
        if (result instanceof ResultMessage.Rows)
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    @VisibleForTesting
    public static QueryOptions makeInternalOptions(CQLStatement prepared, Object[] values)
    {
        return makeInternalOptions(prepared, values, ConsistencyLevel.ONE);
    }

    private static QueryOptions makeInternalOptions(CQLStatement prepared, Object[] values, ConsistencyLevel cl)
    {
        return makeInternalOptionsWithNowInSec(prepared, FBUtilities.nowInSeconds(), values, cl);
    }

    public static QueryOptions makeInternalOptionsWithNowInSec(CQLStatement prepared, long nowInSec, Object[] values)
    {
        return makeInternalOptionsWithNowInSec(prepared, nowInSec, values, ConsistencyLevel.ONE);
    }

    private static QueryOptions makeInternalOptionsWithNowInSec(CQLStatement prepared, long nowInSec, Object[] values, ConsistencyLevel cl)
    {
        if (prepared.getBindVariables().size() != values.length)
            throw new IllegalArgumentException(String.format("Invalid number of values. Expecting %d but got %d", prepared.getBindVariables().size(), values.length));

        List<ByteBuffer> boundValues = new ArrayList<>(values.length);
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            AbstractType<?> type = prepared.getBindVariables().get(i).type;
            boundValues.add(value instanceof ByteBuffer || value == null ? (ByteBuffer)value : type.decomposeUntyped(value));
        }
        return QueryOptions.forInternalCallsWithNowInSec(nowInSec, cl, boundValues);
    }

    public static Prepared prepareInternal(String query) throws RequestValidationException
    {
        Prepared prepared = internalStatements.get(query);
        if (prepared != null)
            return prepared;

        prepared = parseAndPrepare(query, internalQueryState().getClientState(), true);
        internalStatements.put(query, prepared);
        return prepared;
    }

    public static Prepared parseAndPrepare(String query, ClientState clientState, boolean isInternal) throws RequestValidationException
    {
        CQLStatement.Raw raw = parseStatement(query);

        boolean fullyQualified = false;
        String keyspace = null;

        // Set keyspace for statement that require login
        if (raw instanceof QualifiedStatement)
        {
            QualifiedStatement qualifiedStatement = ((QualifiedStatement) raw);
            fullyQualified = qualifiedStatement.isFullyQualified();
            qualifiedStatement.setKeyspace(clientState);
            keyspace = qualifiedStatement.keyspace();
        }

        // Note: if 2 threads prepare the same query, we'll live so don't bother synchronizing
        CQLStatement statement = raw.prepare(clientState);
        statement.validate(clientState);

        if (isInternal)
            return new Prepared(statement, "", fullyQualified, keyspace);
        else
            return new Prepared(statement, query, fullyQualified, keyspace);
    }

    public static UntypedResultSet executeInternal(String query, Object... values)
    {
        Prepared prepared = prepareInternal(query);
        ResultMessage result = prepared.statement.executeLocally(internalQueryState(), makeInternalOptions(prepared.statement, values));
        if (result instanceof ResultMessage.Rows)
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    public static Future<UntypedResultSet> executeAsync(InetAddressAndPort address, String query, Object... values)
    {
        Prepared prepared = prepareInternal(query);
        long nowInSec = FBUtilities.nowInSeconds();
        QueryOptions options = makeInternalOptionsWithNowInSec(prepared.statement, nowInSec, values);
        if (prepared.statement instanceof SelectStatement)
        {
            SelectStatement select = (SelectStatement) prepared.statement;
            ReadQuery readQuery = select.getQuery(options, nowInSec);
            List<ReadCommand> commands;
            if (readQuery instanceof ReadCommand)
            {
                commands = Collections.singletonList((ReadCommand) readQuery);
            }
            else if (readQuery instanceof SinglePartitionReadQuery.Group)
            {
                List<? extends SinglePartitionReadQuery> queries = ((SinglePartitionReadQuery.Group<? extends SinglePartitionReadQuery>) readQuery).queries;
                queries.forEach(a -> {
                    if (!(a instanceof ReadCommand))
                        throw new IllegalArgumentException("Queries found which are not ReadCommand: " + a.getClass());
                });
                commands = (List<ReadCommand>) (List<?>) queries;
            }
            else
            {
                throw new IllegalArgumentException("Unable to handle; only expected ReadCommands but given " + readQuery.getClass());
            }
            Future<List<Message<ReadResponse>>> future = FutureCombiner.allOf(commands.stream()
                                                                                      .map(rc -> Message.out(rc.verb(), rc))
                                                                                      .map(m -> MessagingService.instance().<ReadCommand, ReadResponse>sendWithResult(m, address))
                                                                                      .collect(Collectors.toList()));

            ResultSetBuilder result = new ResultSetBuilder(select.getResultMetadata(), select.getSelection().newSelectors(options), false);
            return future.map(list -> {
                int i = 0;
                for (Message<ReadResponse> m : list)
                {
                    ReadResponse rsp = m.payload;
                    try (PartitionIterator it = UnfilteredPartitionIterators.filter(rsp.makeIterator(commands.get(i++)), nowInSec))
                    {
                        while (it.hasNext())
                        {
                            try (RowIterator partition = it.next())
                            {
                                select.processPartition(partition, options, result, nowInSec);
                            }
                        }
                    }
                }
                return result.build();
            }).map(UntypedResultSet::create);
        }
        throw new IllegalArgumentException("Unable to execute query; only SELECT supported but given: " + query);
    }

    public static UntypedResultSet execute(String query, ConsistencyLevel cl, Object... values)
    throws RequestExecutionException
    {
        return execute(query, cl, internalQueryState(), values);
    }

    public static UntypedResultSet executeInternalWithNowInSec(String query, long nowInSec, Object... values)
    {
        Prepared prepared = prepareInternal(query);
        ResultMessage result = prepared.statement.executeLocally(internalQueryState(), makeInternalOptionsWithNowInSec(prepared.statement, nowInSec, values));
        if (result instanceof ResultMessage.Rows)
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    public static UntypedResultSet execute(String query, ConsistencyLevel cl, QueryState state, Object... values)
    throws RequestExecutionException
    {
        try
        {
            Prepared prepared = prepareInternal(query);
            ResultMessage result = prepared.statement.execute(state, makeInternalOptionsWithNowInSec(prepared.statement, state.getNowInSeconds(), values, cl), nanoTime());
            if (result instanceof ResultMessage.Rows)
                return UntypedResultSet.create(((ResultMessage.Rows)result).result);
            else
                return null;
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException("Error validating " + query, e);
        }
    }

    public static UntypedResultSet executeInternalWithPaging(String query, int pageSize, Object... values)
    {
        Prepared prepared = prepareInternal(query);
        if (!(prepared.statement instanceof SelectStatement))
            throw new IllegalArgumentException("Only SELECTs can be paged");

        SelectStatement select = (SelectStatement)prepared.statement;
        long nowInSec = FBUtilities.nowInSeconds();
        QueryPager pager = select.getQuery(makeInternalOptionsWithNowInSec(prepared.statement, nowInSec, values), nowInSec).getPager(null, ProtocolVersion.CURRENT);
        return UntypedResultSet.create(select, pager, pageSize);
    }

    /**
     * Same than executeLocally, but to use for queries we know are only executed once so that the
     * created statement object is not cached.
     */
    public static UntypedResultSet executeOnceInternal(String query, Object... values)
    {
        return executeOnceInternal(internalQueryState(), query, values);
    }

    /**
     * Execute an internal query with the provided {@code nowInSec} and {@code timestamp} for the {@code QueryState}.
     * <p>This method ensure that the statement will not be cached in the prepared statement cache.</p>
     */
    @VisibleForTesting
    public static UntypedResultSet executeOnceInternalWithNowAndTimestamp(long nowInSec, long timestamp, String query, Object... values)
    {
        QueryState queryState = new QueryState(InternalStateInstance.INSTANCE.clientState, timestamp, nowInSec);
        return executeOnceInternal(queryState, query, values);
    }

    private static UntypedResultSet executeOnceInternal(QueryState queryState, String query, Object... values)
    {
        CQLStatement statement = parseStatement(query, queryState.getClientState());
        statement.validate(queryState.getClientState());
        ResultMessage result = statement.executeLocally(queryState, makeInternalOptionsWithNowInSec(statement, queryState.getNowInSeconds(), values));
        if (result instanceof ResultMessage.Rows)
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    /**
     * A special version of executeLocally that takes the time used as "now" for the query in argument.
     * Note that this only make sense for Selects so this only accept SELECT statements and is only useful in rare
     * cases.
     */
    public static UntypedResultSet executeInternalWithNow(long nowInSec, long queryStartNanoTime, String query, Object... values)
    {
        Prepared prepared = prepareInternal(query);
        assert prepared.statement instanceof SelectStatement;
        SelectStatement select = (SelectStatement)prepared.statement;
        ResultMessage result = select.executeInternal(internalQueryState(), makeInternalOptionsWithNowInSec(prepared.statement, nowInSec, values), nowInSec, queryStartNanoTime);
        assert result instanceof ResultMessage.Rows;
        return UntypedResultSet.create(((ResultMessage.Rows)result).result);
    }

    /**
     * A special version of executeInternal that takes the time used as "now" for the query in argument.
     * Note that this only make sense for Selects so this only accept SELECT statements and is only useful in rare
     * cases.
     */
    public static Map<DecoratedKey, List<Row>> executeInternalRawWithNow(long nowInSec, String query, Object... values)
    {
        Prepared prepared = prepareInternal(query);
        assert prepared.statement instanceof SelectStatement;
        SelectStatement select = (SelectStatement) prepared.statement;
        return select.executeRawInternal(makeInternalOptionsWithNowInSec(prepared.statement, nowInSec, values), internalQueryState().getClientState(), nowInSec);
    }

    @VisibleForTesting
    public static UntypedResultSet resultify(String query, RowIterator partition)
    {
        return resultify(query, PartitionIterators.singletonIterator(partition));
    }

    @VisibleForTesting
    public static UntypedResultSet resultify(String query, PartitionIterator partitions)
    {
        try (PartitionIterator iter = partitions)
        {
            SelectStatement ss = (SelectStatement) getStatement(query, null);
            ResultSet cqlRows = ss.process(iter, FBUtilities.nowInSeconds(), true);
            return UntypedResultSet.create(cqlRows);
        }
    }

    public ResultMessage.Prepared prepare(String query,
                                          ClientState clientState,
                                          Map<String, ByteBuffer> customPayload) throws RequestValidationException
    {
        return prepare(query, clientState);
    }

    private volatile boolean newPreparedStatementBehaviour = false;
    public boolean useNewPreparedStatementBehaviour()
    {
        if (newPreparedStatementBehaviour || DatabaseDescriptor.getForceNewPreparedStatementBehaviour())
            return true;

        synchronized (this)
        {
            CassandraVersion minVersion = Gossiper.instance.getMinVersion(DatabaseDescriptor.getWriteRpcTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            if (minVersion != null && minVersion.compareTo(NEW_PREPARED_STATEMENT_BEHAVIOUR_SINCE_40, true) >= 0)
            {
                logger.info("Fully upgraded to at least {}", minVersion);
                newPreparedStatementBehaviour = true;
            }

            return newPreparedStatementBehaviour;
        }
    }

    /**
     * This method got slightly out of hand, but this is with best intentions: to allow users to be upgraded from any
     * prior version, and help implementers avoid previous mistakes by clearly separating fully qualified and non-fully
     * qualified statement behaviour.
     * <p>
     * Basically we need to handle 4 different hashes here;
     * 1. fully qualified query with keyspace
     * 2. fully qualified query without keyspace
     * 3. unqualified query with keyspace
     * 4. unqualified query without keyspace
     * <p>
     * The correct combination to return is 2/3 - the problem is during upgrades (assuming upgrading from < 4.0.2)
     * - Existing clients have hash 1 or 3
     * - Query prepared on a post-4.0.2 instance needs to return hash 1/3 to be able to execute it on a pre-4.0.2 instance
     * - This is handled by the useNewPreparedStatementBehaviour flag - while there still are pre-4.0.2 instances in
     *   the cluster we always return hash 1/3
     * - Once fully upgraded we start returning hash 2/3, this will cause a prepared statement id mismatch for existing
     *   clients, but they will be able to continue using the old prepared statement id after that exception since we
     *   store the query both with and without keyspace.
     */
    public ResultMessage.Prepared prepare(String queryString, ClientState clientState)
    {
        boolean useNewPreparedStatementBehaviour = useNewPreparedStatementBehaviour();
        MD5Digest hashWithoutKeyspace = computeId(queryString, null);
        MD5Digest hashWithKeyspace = computeId(queryString, clientState.getRawKeyspace());
        Prepared cachedWithoutKeyspace = preparedStatements.getIfPresent(hashWithoutKeyspace);
        Prepared cachedWithKeyspace = preparedStatements.getIfPresent(hashWithKeyspace);
        // We assume it is only safe to return cached prepare if we have both instances
        boolean safeToReturnCached = cachedWithoutKeyspace != null && cachedWithKeyspace != null;

        if (safeToReturnCached)
        {
            if (useNewPreparedStatementBehaviour)
            {
                if (cachedWithoutKeyspace.fullyQualified) // For fully qualified statements, we always skip keyspace to avoid digest switching
                    return createResultMessage(hashWithoutKeyspace, cachedWithoutKeyspace);

                if (clientState.getRawKeyspace() != null && !cachedWithKeyspace.fullyQualified) // For non-fully qualified statements, we always include keyspace to avoid ambiguity
                    return createResultMessage(hashWithKeyspace, cachedWithKeyspace);

            }
            else // legacy caches, pre-CASSANDRA-15252 behaviour
            {
                return createResultMessage(hashWithKeyspace, cachedWithKeyspace);
            }
        }
        else
        {
            // Make sure the missing one is going to be eventually re-prepared
            evictPrepared(hashWithKeyspace);
            evictPrepared(hashWithoutKeyspace);
        }

        Prepared prepared = parseAndPrepare(queryString, clientState, false);
        CQLStatement statement = prepared.statement;

        int boundTerms = statement.getBindVariables().size();
        if (boundTerms > FBUtilities.MAX_UNSIGNED_SHORT)
            throw new InvalidRequestException(String.format("Too many markers(?). %d markers exceed the allowed maximum of %d", boundTerms, FBUtilities.MAX_UNSIGNED_SHORT));

        if (prepared.fullyQualified)
        {
            ResultMessage.Prepared qualifiedWithoutKeyspace = storePreparedStatement(queryString, null, prepared);
            ResultMessage.Prepared qualifiedWithKeyspace = null;
            if (clientState.getRawKeyspace() != null)
                qualifiedWithKeyspace = storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared);

            if (!useNewPreparedStatementBehaviour && qualifiedWithKeyspace != null)
                return qualifiedWithKeyspace;

            return qualifiedWithoutKeyspace;
        }
        else
        {
            clientState.warnAboutUseWithPreparedStatements(hashWithKeyspace, clientState.getRawKeyspace());

            ResultMessage.Prepared nonQualifiedWithKeyspace = storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared);
            ResultMessage.Prepared nonQualifiedWithNullKeyspace = storePreparedStatement(queryString, null, prepared);
            if (!useNewPreparedStatementBehaviour)
                return nonQualifiedWithNullKeyspace;

            return nonQualifiedWithKeyspace;
        }
    }

    private static MD5Digest computeId(String queryString, String keyspace)
    {
        String toHash = keyspace == null ? queryString : keyspace + queryString;
        return MD5Digest.compute(toHash);
    }

    @VisibleForTesting
    public static ResultMessage.Prepared getStoredPreparedStatement(String queryString, String clientKeyspace)
    throws InvalidRequestException
    {
        MD5Digest statementId = computeId(queryString, clientKeyspace);
        Prepared existing = preparedStatements.getIfPresent(statementId);
        if (existing == null)
            return null;

        checkTrue(queryString.equals(existing.rawCQLStatement),
                  "MD5 hash collision: query with the same MD5 hash was already prepared. \n Existing: '%s'",
                  existing.rawCQLStatement);

        return createResultMessage(statementId, existing);
    }

    @VisibleForTesting
    private static ResultMessage.Prepared createResultMessage(MD5Digest statementId, Prepared existing)
    throws InvalidRequestException
    {
        ResultSet.PreparedMetadata preparedMetadata = ResultSet.PreparedMetadata.fromPrepared(existing.statement);
        ResultSet.ResultMetadata resultMetadata = ResultSet.ResultMetadata.fromPrepared(existing.statement);
        return new ResultMessage.Prepared(statementId, resultMetadata.getResultMetadataId(), preparedMetadata, resultMetadata);
    }

    @VisibleForTesting
    public static ResultMessage.Prepared storePreparedStatement(String queryString, String keyspace, Prepared prepared)
    throws InvalidRequestException
    {
        // Concatenate the current keyspace so we don't mix prepared statements between keyspace (#5352).
        // (if the keyspace is null, queryString has to have a fully-qualified keyspace so it's fine.
        long statementSize = ObjectSizes.measureDeep(prepared.statement);
        // don't execute the statement if it's bigger than the allowed threshold
        if (statementSize > capacityToBytes(DatabaseDescriptor.getPreparedStatementsCacheSizeMiB()))
            throw new InvalidRequestException(String.format("Prepared statement of size %d bytes is larger than allowed maximum of %d MB: %s...",
                                                            statementSize,
                                                            DatabaseDescriptor.getPreparedStatementsCacheSizeMiB(),
                                                            queryString.substring(0, 200)));
        MD5Digest statementId = computeId(queryString, keyspace);
        Prepared previous = preparedStatements.get(statementId, (ignored_) -> prepared);
        if (previous == prepared)
            SystemKeyspace.writePreparedStatement(keyspace, statementId, queryString);

        ResultSet.PreparedMetadata preparedMetadata = ResultSet.PreparedMetadata.fromPrepared(prepared.statement);
        ResultSet.ResultMetadata resultMetadata = ResultSet.ResultMetadata.fromPrepared(prepared.statement);
        return new ResultMessage.Prepared(statementId, resultMetadata.getResultMetadataId(), preparedMetadata, resultMetadata);
    }

    public ResultMessage processPrepared(CQLStatement statement,
                                         QueryState state,
                                         QueryOptions options,
                                         Map<String, ByteBuffer> customPayload,
                                         long queryStartNanoTime)
                                                 throws RequestExecutionException, RequestValidationException
    {
        return processPrepared(statement, state, options, queryStartNanoTime);
    }

    public ResultMessage processPrepared(CQLStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> variables = options.getValues();
        // Check to see if there are any bound variables to verify
        if (!(variables.isEmpty() && statement.getBindVariables().isEmpty()))
        {
            if (variables.size() != statement.getBindVariables().size())
                throw new InvalidRequestException(String.format("there were %d markers(?) in CQL but %d bound variables",
                                                                statement.getBindVariables().size(),
                                                                variables.size()));

            // at this point there is a match in count between markers and variables that is non-zero
            if (logger.isTraceEnabled())
                for (int i = 0; i < variables.size(); i++)
                    logger.trace("[{}] '{}'", i+1, variables.get(i));
        }

        metrics.preparedStatementsExecuted.inc();
        return processStatement(statement, queryState, options, queryStartNanoTime);
    }

    public ResultMessage processBatch(BatchStatement statement,
                                      QueryState state,
                                      BatchQueryOptions options,
                                      Map<String, ByteBuffer> customPayload,
                                      long queryStartNanoTime)
                                              throws RequestExecutionException, RequestValidationException
    {
        return processBatch(statement, state, options, queryStartNanoTime);
    }

    public ResultMessage processBatch(BatchStatement batch, QueryState queryState, BatchQueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        ClientState clientState = queryState.getClientState().cloneWithKeyspaceIfSet(options.getKeyspace());
        batch.authorize(clientState);
        batch.validate();
        batch.validate(clientState);
        return batch.execute(queryState, options, queryStartNanoTime);
    }

    public static CQLStatement getStatement(String queryStr, ClientState clientState)
    throws RequestValidationException
    {
        Tracing.trace("Parsing {}", queryStr);
        CQLStatement.Raw statement = parseStatement(queryStr);

        // Set keyspace for statement that require login
        if (statement instanceof QualifiedStatement)
            ((QualifiedStatement) statement).setKeyspace(clientState);

        Tracing.trace("Preparing statement");
        return statement.prepare(clientState);
    }

    public static <T extends CQLStatement.Raw> T parseStatement(String queryStr, Class<T> klass, String type) throws SyntaxException
    {
        try
        {
            CQLStatement.Raw stmt = parseStatement(queryStr);

            if (!klass.isAssignableFrom(stmt.getClass()))
                throw new IllegalArgumentException("Invalid query, must be a " + type + " statement but was: " + stmt.getClass());

            return klass.cast(stmt);
        }
        catch (RequestValidationException e)
        {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
    public static CQLStatement.Raw parseStatement(String queryStr) throws SyntaxException
    {
        try
        {
            return CQLFragmentParser.parseAnyUnhandled(CqlParser::query, queryStr);
        }
        catch (CassandraException ce)
        {
            throw ce;
        }
        catch (RuntimeException re)
        {
            logger.error(String.format("The statement: [%s] could not be parsed.", queryStr), re);
            throw new SyntaxException(String.format("Failed parsing statement: [%s] reason: %s %s",
                                                    queryStr,
                                                    re.getClass().getSimpleName(),
                                                    re.getMessage()));
        }
        catch (RecognitionException e)
        {
            throw new SyntaxException("Invalid or malformed CQL query string: " + e.getMessage());
        }
    }

    private static int measure(Object key, Prepared value)
    {
        return Ints.checkedCast(ObjectSizes.measureDeep(key) + ObjectSizes.measureDeep(value));
    }

    /**
     * Clear our internal statmeent cache for test purposes.
     */
    @VisibleForTesting
    public static void clearInternalStatementsCache()
    {
        internalStatements.clear();
    }

    @VisibleForTesting
    public static void clearPreparedStatementsCache()
    {
        preparedStatements.asMap().clear();
    }

    private static class StatementInvalidatingListener implements SchemaChangeListener
    {
        private static void removeInvalidPreparedStatements(String ksName, String cfName)
        {
            removeInvalidPreparedStatements(internalStatements.values().iterator(), ksName, cfName);
            removeInvalidPersistentPreparedStatements(preparedStatements.asMap().entrySet().iterator(), ksName, cfName);
        }

        private static void removeInvalidPreparedStatementsForFunction(String ksName, String functionName)
        {
            Predicate<Function> matchesFunction = f -> ksName.equals(f.name().keyspace) && functionName.equals(f.name().name);

            for (Iterator<Map.Entry<MD5Digest, Prepared>> iter = preparedStatements.asMap().entrySet().iterator();
                 iter.hasNext();)
            {
                Map.Entry<MD5Digest, Prepared> pstmt = iter.next();
                if (Iterables.any(pstmt.getValue().statement.getFunctions(), matchesFunction))
                {
                    SystemKeyspace.removePreparedStatement(pstmt.getKey());
                    iter.remove();
                }
            }


            Iterators.removeIf(internalStatements.values().iterator(),
                               statement -> Iterables.any(statement.statement.getFunctions(), matchesFunction));
        }

        private static void removeInvalidPersistentPreparedStatements(Iterator<Map.Entry<MD5Digest, Prepared>> iterator,
                                                                      String ksName, String cfName)
        {
            while (iterator.hasNext())
            {
                Map.Entry<MD5Digest, Prepared> entry = iterator.next();
                if (shouldInvalidate(ksName, cfName, entry.getValue().statement))
                {
                    SystemKeyspace.removePreparedStatement(entry.getKey());
                    iterator.remove();
                }
            }
        }

        private static void removeInvalidPreparedStatements(Iterator<Prepared> iterator, String ksName, String cfName)
        {
            while (iterator.hasNext())
            {
                if (shouldInvalidate(ksName, cfName, iterator.next().statement))
                    iterator.remove();
            }
        }

        private static boolean shouldInvalidate(String ksName, String cfName, CQLStatement statement)
        {
            String statementKsName;
            String statementCfName;

            if (statement instanceof ModificationStatement)
            {
                ModificationStatement modificationStatement = ((ModificationStatement) statement);
                statementKsName = modificationStatement.keyspace();
                statementCfName = modificationStatement.table();
            }
            else if (statement instanceof SelectStatement)
            {
                SelectStatement selectStatement = ((SelectStatement) statement);
                statementKsName = selectStatement.keyspace();
                statementCfName = selectStatement.table();
            }
            else if (statement instanceof BatchStatement)
            {
                BatchStatement batchStatement = ((BatchStatement) statement);
                for (ModificationStatement stmt : batchStatement.getStatements())
                {
                    if (shouldInvalidate(ksName, cfName, stmt))
                        return true;
                }
                return false;
            }
            else
            {
                return false;
            }

            return ksName.equals(statementKsName) && (cfName == null || cfName.equals(statementCfName));
        }

        @Override
        public void onCreateFunction(UDFunction function)
        {
            onCreateFunctionInternal(function.name().keyspace, function.name().name, function.argTypes());
        }

        @Override
        public void onCreateAggregate(UDAggregate aggregate)
        {
            onCreateFunctionInternal(aggregate.name().keyspace, aggregate.name().name, aggregate.argTypes());
        }

        private static void onCreateFunctionInternal(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            // in case there are other overloads, we have to remove all overloads since argument type
            // matching may change (due to type casting)
            if (Schema.instance.getKeyspaceMetadata(ksName).userFunctions.get(new FunctionName(ksName, functionName)).size() > 1)
                removeInvalidPreparedStatementsForFunction(ksName, functionName);
        }

        @Override
        public void onAlterTable(TableMetadata before, TableMetadata after, boolean affectsStatements)
        {
            logger.trace("Column definitions for {}.{} changed, invalidating related prepared statements", before.keyspace, before.name);
            if (affectsStatements)
                removeInvalidPreparedStatements(before.keyspace, before.name);
        }

        @Override
        public void onAlterFunction(UDFunction before, UDFunction after)
        {
            // Updating a function may imply we've changed the body of the function, so we need to invalid statements so that
            // the new definition is picked (the function is resolved at preparation time).
            // TODO: if the function has multiple overload, we could invalidate only the statement refering to the overload
            // that was updated. This requires a few changes however and probably doesn't matter much in practice.
            removeInvalidPreparedStatementsForFunction(before.name().keyspace, before.name().name);
        }

        @Override
        public void onAlterAggregate(UDAggregate before, UDAggregate after)
        {
            // Updating a function may imply we've changed the body of the function, so we need to invalid statements so that
            // the new definition is picked (the function is resolved at preparation time).
            // TODO: if the function has multiple overload, we could invalidate only the statement refering to the overload
            // that was updated. This requires a few changes however and probably doesn't matter much in practice.
            removeInvalidPreparedStatementsForFunction(before.name().keyspace, before.name().name);
        }

        @Override
        public void onDropKeyspace(KeyspaceMetadata keyspace, boolean dropData)
        {
            logger.trace("Keyspace {} was dropped, invalidating related prepared statements", keyspace.name);
            removeInvalidPreparedStatements(keyspace.name, null);
        }

        @Override
        public void onDropTable(TableMetadata table, boolean dropData)
        {
            logger.trace("Table {}.{} was dropped, invalidating related prepared statements", table.keyspace, table.name);
            removeInvalidPreparedStatements(table.keyspace, table.name);
        }

        @Override
        public void onDropFunction(UDFunction function)
        {
            removeInvalidPreparedStatementsForFunction(function.name().keyspace, function.name().name);
        }

        @Override
        public void onDropAggregate(UDAggregate aggregate)
        {
            removeInvalidPreparedStatementsForFunction(aggregate.name().keyspace, aggregate.name().name);
        }
    }
}
