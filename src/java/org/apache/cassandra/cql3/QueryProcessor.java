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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.antlr.runtime.*;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.metrics.CQLMetrics;
import org.apache.cassandra.service.*;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

public class QueryProcessor implements QueryHandler
{
    public static final CassandraVersion CQL_VERSION = new CassandraVersion("3.4.5");

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
                             .executor(MoreExecutors.directExecutor())
                             .maximumWeight(capacityToBytes(DatabaseDescriptor.getPreparedStatementsCacheSizeMB()))
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11718
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11718
                             .weigher(QueryProcessor::measure)
                             .removalListener((key, prepared, cause) -> {
                                 MD5Digest md5Digest = (MD5Digest) key;
                                 if (cause.wasEvicted())
                                 {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7921
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7921
                                     metrics.preparedStatementsEvicted.inc();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7930
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7930
                                     lastMinuteEvictionsCount.incrementAndGet();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13641
                                     SystemKeyspace.removePreparedStatement(md5Digest);
                                 }
                             }).build();

        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(() -> {
            long count = lastMinuteEvictionsCount.getAndSet(0);
            if (count > 0)
                logger.warn("{} prepared statements discarded in the last minute because cache limit reached ({} MB)",
                            count,
                            DatabaseDescriptor.getPreparedStatementsCacheSizeMB());
        }, 1, 1, TimeUnit.MINUTES);

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11115
        logger.info("Initialized prepared statement caches with {} MB",
                    DatabaseDescriptor.getPreparedStatementsCacheSizeMB());
    }

    private static long capacityToBytes(long cacheSizeMB)
    {
        return cacheSizeMB * 1024 * 1024;
    }

    public static int preparedStatementsCount()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10855
        return preparedStatements.asMap().size();
    }

    // Work around initialization dependency
    private enum InternalStateInstance
    {
        INSTANCE;

        private final ClientState clientState;

        InternalStateInstance()
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14677
            clientState = ClientState.forInternalCalls(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        }
    }

    public static void preloadPreparedStatement()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8831
        ClientState clientState = ClientState.forInternalCalls();
        int count = 0;
        for (Pair<String, String> useKeyspaceAndCQL : SystemKeyspace.loadPreparedStatements())
        {
            try
            {
                clientState.setKeyspace(useKeyspaceAndCQL.left);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11115
                prepare(useKeyspaceAndCQL.right, clientState);
                count++;
            }
            catch (RequestValidationException e)
            {
                logger.warn("prepared statement recreation error: {}", useKeyspaceAndCQL.right, e);
            }
        }
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13641
        if (!memoryOnly)
            SystemKeyspace.resetPreparedStatements();
    }

    @VisibleForTesting
    public static QueryState internalQueryState()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14677
        return new QueryState(InternalStateInstance.INSTANCE.clientState);
    }

    private QueryProcessor()
    {
        Schema.instance.registerListener(new StatementInvalidatingListener());
    }

    public Prepared getPrepared(MD5Digest id)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10855
        return preparedStatements.getIfPresent(id);
    }

    public static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new InvalidRequestException("Key may not be empty");
        }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7304
        if (key == ByteBufferUtil.UNSET_BYTE_BUFFER)
            throw new InvalidRequestException("Key may not be unset");

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
        }
    }

    public ResultMessage processStatement(CQLStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-3979
    throws RequestExecutionException, RequestValidationException
    {
        logger.trace("Process {} @CL.{}", statement, options.getConsistency());
        ClientState clientState = queryState.getClientState();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
        statement.authorize(clientState);
        statement.validate(clientState);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-4693

        ResultMessage result;
        if (options.getConsistency() == ConsistencyLevel.NODE_LOCAL)
        {
            assert Boolean.getBoolean("cassandra.enable_nodelocal_queries") : "Node local consistency level is highly dangerous and should be used only for debugging purposes";
            assert statement instanceof SelectStatement : "Only SELECT statements are permitted for node-local execution";
            logger.info("Statement {} executed with NODE_LOCAL consistency level.", statement);
            result = statement.executeLocally(queryState, options);
        }
        else
        {
            result = statement.execute(queryState, options, queryStartNanoTime);
        }
        return result == null ? new ResultMessage.Void() : result;
    }

    public static ResultMessage process(String queryString, ConsistencyLevel cl, QueryState queryState, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14772
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7719

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12256
        return processStatement(prepared, queryState, options, queryStartNanoTime);
    }

    public static CQLStatement parseStatement(String queryStr, ClientState clientState) throws RequestValidationException
    {
        return getStatement(queryStr, clientState);
    }

    public static UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9534
        return process(query, cl, Collections.<ByteBuffer>emptyList());
    }

    public static UntypedResultSet process(String query, ConsistencyLevel cl, List<ByteBuffer> values) throws RequestExecutionException
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-14772
        QueryState queryState = QueryState.forInternalCalls();
        QueryOptions options = QueryOptions.forInternalCalls(cl, values);
        CQLStatement statement = instance.parse(query, queryState, options);
        ResultMessage result = instance.process(statement, queryState, options, System.nanoTime());
        if (result instanceof ResultMessage.Rows)
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    @VisibleForTesting
    public static QueryOptions makeInternalOptions(CQLStatement prepared, Object[] values)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8099
        return makeInternalOptions(prepared, values, ConsistencyLevel.ONE);
    }

    private static QueryOptions makeInternalOptions(CQLStatement prepared, Object[] values, ConsistencyLevel cl)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
        if (prepared.getBindVariables().size() != values.length)
            throw new IllegalArgumentException(String.format("Invalid number of values. Expecting %d but got %d", prepared.getBindVariables().size(), values.length));

        List<ByteBuffer> boundValues = new ArrayList<>(values.length);
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            AbstractType type = prepared.getBindVariables().get(i).type;
            boundValues.add(value instanceof ByteBuffer || value == null ? (ByteBuffer)value : type.decompose(value));
        }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8099
        return QueryOptions.forInternalCalls(cl, boundValues);
    }

    public static Prepared prepareInternal(String query) throws RequestValidationException
    {
        Prepared prepared = internalStatements.get(query);
        if (prepared != null)
            return prepared;

        // Note: if 2 threads prepare the same query, we'll live so don't bother synchronizing
        CQLStatement statement = parseStatement(query, internalQueryState().getClientState());
        statement.validate(internalQueryState().getClientState());

        prepared = new Prepared(statement);
        internalStatements.put(query, prepared);
        return prepared;
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

    public static UntypedResultSet execute(String query, ConsistencyLevel cl, Object... values)
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9967
    throws RequestExecutionException
    {
        return execute(query, cl, internalQueryState(), values);
    }

    public static UntypedResultSet execute(String query, ConsistencyLevel cl, QueryState state, Object... values)
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8099
    throws RequestExecutionException
    {
        try
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
            Prepared prepared = prepareInternal(query);
            ResultMessage result = prepared.statement.execute(state, makeInternalOptions(prepared.statement, values, cl), System.nanoTime());
            if (result instanceof ResultMessage.Rows)
                return UntypedResultSet.create(((ResultMessage.Rows)result).result);
            else
                return null;
        }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-3979
        catch (RequestValidationException e)
        {
            throw new RuntimeException("Error validating " + query, e);
        }
    }

    public static UntypedResultSet executeInternalWithPaging(String query, int pageSize, Object... values)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
        Prepared prepared = prepareInternal(query);
        if (!(prepared.statement instanceof SelectStatement))
            throw new IllegalArgumentException("Only SELECTs can be paged");

        SelectStatement select = (SelectStatement)prepared.statement;
        QueryPager pager = select.getQuery(makeInternalOptions(prepared.statement, values), FBUtilities.nowInSeconds()).getPager(null, ProtocolVersion.CURRENT);
        return UntypedResultSet.create(select, pager, pageSize);
    }

    /**
     * Same than executeLocally, but to use for queries we know are only executed once so that the
     * created statement object is not cached.
     */
    public static UntypedResultSet executeOnceInternal(String query, Object... values)
    {
        CQLStatement statement = parseStatement(query, internalQueryState().getClientState());
        statement.validate(internalQueryState().getClientState());
        ResultMessage result = statement.executeLocally(internalQueryState(), makeInternalOptions(statement, values));
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8560
        if (result instanceof ResultMessage.Rows)
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5417
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5417
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    /**
     * A special version of executeLocally that takes the time used as "now" for the query in argument.
     * Note that this only make sense for Selects so this only accept SELECT statements and is only useful in rare
     * cases.
     */
    public static UntypedResultSet executeInternalWithNow(int nowInSec, long queryStartNanoTime, String query, Object... values)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
        Prepared prepared = prepareInternal(query);
        assert prepared.statement instanceof SelectStatement;
        SelectStatement select = (SelectStatement)prepared.statement;
        ResultMessage result = select.executeInternal(internalQueryState(), makeInternalOptions(prepared.statement, values), nowInSec, queryStartNanoTime);
        assert result instanceof ResultMessage.Rows;
        return UntypedResultSet.create(((ResultMessage.Rows)result).result);
    }

    public static UntypedResultSet resultify(String query, RowIterator partition)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8099
        return resultify(query, PartitionIterators.singletonIterator(partition));
    }

    public static UntypedResultSet resultify(String query, PartitionIterator partitions)
    {
        try (PartitionIterator iter = partitions)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
            SelectStatement ss = (SelectStatement) getStatement(query, null);
            ResultSet cqlRows = ss.process(iter, FBUtilities.nowInSeconds());
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5417
            return UntypedResultSet.create(cqlRows);
        }
    }

    public ResultMessage.Prepared prepare(String query,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10145
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10145
                                          ClientState clientState,
                                          Map<String, ByteBuffer> customPayload) throws RequestValidationException
    {
        return prepare(query, clientState);
    }

    public static ResultMessage.Prepared prepare(String queryString, ClientState clientState)
    {
        ResultMessage.Prepared existing = getStoredPreparedStatement(queryString, clientState.getRawKeyspace());
        if (existing != null)
            return existing;

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
        CQLStatement statement = getStatement(queryString, clientState);
        Prepared prepared = new Prepared(statement, queryString);

        int boundTerms = statement.getBindVariables().size();
        if (boundTerms > FBUtilities.MAX_UNSIGNED_SHORT)
            throw new InvalidRequestException(String.format("Too many markers(?). %d markers exceed the allowed maximum of %d", boundTerms, FBUtilities.MAX_UNSIGNED_SHORT));

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11115
        return storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared);
    }

    private static MD5Digest computeId(String queryString, String keyspace)
    {
        String toHash = keyspace == null ? queryString : keyspace + queryString;
        return MD5Digest.compute(toHash);
    }

    private static ResultMessage.Prepared getStoredPreparedStatement(String queryString, String keyspace)
    throws InvalidRequestException
    {
        MD5Digest statementId = computeId(queryString, keyspace);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
        Prepared existing = preparedStatements.getIfPresent(statementId);
        if (existing == null)
            return null;

        checkTrue(queryString.equals(existing.rawCQLStatement),
                String.format("MD5 hash collision: query with the same MD5 hash was already prepared. \n Existing: '%s'", existing.rawCQLStatement));

        ResultSet.PreparedMetadata preparedMetadata = ResultSet.PreparedMetadata.fromPrepared(existing.statement);
        ResultSet.ResultMetadata resultMetadata = ResultSet.ResultMetadata.fromPrepared(existing.statement);
        return new ResultMessage.Prepared(statementId, resultMetadata.getResultMetadataId(), preparedMetadata, resultMetadata);
    }

    private static ResultMessage.Prepared storePreparedStatement(String queryString, String keyspace, Prepared prepared)
    throws InvalidRequestException
    {
        // Concatenate the current keyspace so we don't mix prepared statements between keyspace (#5352).
        // (if the keyspace is null, queryString has to have a fully-qualified keyspace so it's fine.
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11718
        long statementSize = ObjectSizes.measureDeep(prepared.statement);
        // don't execute the statement if it's bigger than the allowed threshold
        if (statementSize > capacityToBytes(DatabaseDescriptor.getPreparedStatementsCacheSizeMB()))
            throw new InvalidRequestException(String.format("Prepared statement of size %d bytes is larger than allowed maximum of %d MB: %s...",
                                                            statementSize,
                                                            DatabaseDescriptor.getPreparedStatementsCacheSizeMB(),
                                                            queryString.substring(0, 200)));
        MD5Digest statementId = computeId(queryString, keyspace);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6855
        preparedStatements.put(statementId, prepared);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8831
        SystemKeyspace.writePreparedStatement(keyspace, statementId, queryString);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
        ResultSet.PreparedMetadata preparedMetadata = ResultSet.PreparedMetadata.fromPrepared(prepared.statement);
        ResultSet.ResultMetadata resultMetadata = ResultSet.ResultMetadata.fromPrepared(prepared.statement);
        return new ResultMessage.Prepared(statementId, resultMetadata.getResultMetadataId(), preparedMetadata, resultMetadata);
    }

    public ResultMessage processPrepared(CQLStatement statement,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9515
                                         QueryState state,
                                         QueryOptions options,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12256
                                         Map<String, ByteBuffer> customPayload,
                                         long queryStartNanoTime)
                                                 throws RequestExecutionException, RequestValidationException
    {
        return processPrepared(statement, state, options, queryStartNanoTime);
    }

    public ResultMessage processPrepared(CQLStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-3979
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> variables = options.getValues();
        // Check to see if there are any bound variables to verify
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
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

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7719
        metrics.preparedStatementsExecuted.inc();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12256
        return processStatement(statement, queryState, options, queryStartNanoTime);
    }

    public ResultMessage processBatch(BatchStatement statement,
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9515
                                      QueryState state,
                                      BatchQueryOptions options,
                                      Map<String, ByteBuffer> customPayload,
                                      long queryStartNanoTime)
                                              throws RequestExecutionException, RequestValidationException
    {
        return processBatch(statement, state, options, queryStartNanoTime);
    }

    public ResultMessage processBatch(BatchStatement batch, QueryState queryState, BatchQueryOptions options, long queryStartNanoTime)
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-3979
    throws RequestExecutionException, RequestValidationException
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10145
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10145
        ClientState clientState = queryState.getClientState().cloneWithKeyspaceIfSet(options.getKeyspace());
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
        batch.authorize(clientState);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7351
        batch.validate();
        batch.validate(clientState);
        return batch.execute(queryState, options, queryStartNanoTime);
    }

    public static CQLStatement getStatement(String queryStr, ClientState clientState)
    throws RequestValidationException
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5638
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12450
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10650
            return CQLFragmentParser.parseAnyUnhandled(CqlParser::query, queryStr);
        }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8560
        catch (CassandraException ce)
        {
            throw ce;
        }
        catch (RuntimeException re)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8455
            logger.error(String.format("The statement: [%s] could not be parsed.", queryStr), re);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5893
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

    private static class StatementInvalidatingListener extends SchemaChangeListener
    {
        private static void removeInvalidPreparedStatements(String ksName, String cfName)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8693
            removeInvalidPreparedStatements(internalStatements.values().iterator(), ksName, cfName);
            removeInvalidPersistentPreparedStatements(preparedStatements.asMap().entrySet().iterator(), ksName, cfName);
        }

        private static void removeInvalidPreparedStatementsForFunction(String ksName, String functionName)
        {
            Predicate<Function> matchesFunction = f -> ksName.equals(f.name().keyspace) && functionName.equals(f.name().name);

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
                Map.Entry<MD5Digest, Prepared> entry = iterator.next();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12450
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
                statementCfName = modificationStatement.columnFamily();
            }
            else if (statement instanceof SelectStatement)
            {
                SelectStatement selectStatement = ((SelectStatement) statement);
                statementKsName = selectStatement.keyspace();
                statementCfName = selectStatement.columnFamily();
            }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8652
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

        public void onCreateFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9665
            onCreateFunctionInternal(ksName, functionName, argTypes);
        }

        public void onCreateAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
        {
            onCreateFunctionInternal(ksName, aggregateName, argTypes);
        }

        private static void onCreateFunctionInternal(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            // in case there are other overloads, we have to remove all overloads since argument type
            // matching may change (due to type casting)
            if (Schema.instance.getKeyspaceMetadata(ksName).functions.get(new FunctionName(ksName, functionName)).size() > 1)
                removeInvalidPreparedStatementsForFunction(ksName, functionName);
        }

        public void onAlterTable(String ksName, String cfName, boolean affectsStatements)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10241
            logger.trace("Column definitions for {}.{} changed, invalidating related prepared statements", ksName, cfName);
            if (affectsStatements)
                removeInvalidPreparedStatements(ksName, cfName);
        }

        public void onAlterFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            // Updating a function may imply we've changed the body of the function, so we need to invalid statements so that
            // the new definition is picked (the function is resolved at preparation time).
            // TODO: if the function has multiple overload, we could invalidate only the statement refering to the overload
            // that was updated. This requires a few changes however and probably doesn't matter much in practice.
            removeInvalidPreparedStatementsForFunction(ksName, functionName);
        }

        public void onAlterAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
        {
            // Updating a function may imply we've changed the body of the function, so we need to invalid statements so that
            // the new definition is picked (the function is resolved at preparation time).
            // TODO: if the function has multiple overload, we could invalidate only the statement refering to the overload
            // that was updated. This requires a few changes however and probably doesn't matter much in practice.
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8831
            removeInvalidPreparedStatementsForFunction(ksName, aggregateName);
        }

        public void onDropKeyspace(String ksName)
        {
            logger.trace("Keyspace {} was dropped, invalidating related prepared statements", ksName);
            removeInvalidPreparedStatements(ksName, null);
        }

        public void onDropTable(String ksName, String cfName)
        {
            logger.trace("Table {}.{} was dropped, invalidating related prepared statements", ksName, cfName);
            removeInvalidPreparedStatements(ksName, cfName);
        }

        public void onDropFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8831
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8831
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8831
            removeInvalidPreparedStatementsForFunction(ksName, functionName);
        }

        public void onDropAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
        {
            removeInvalidPreparedStatementsForFunction(ksName, aggregateName);
        }
    }
}
