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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EntryWeigher;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import org.antlr.runtime.*;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Schema;
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
import org.apache.cassandra.thrift.ThriftClientState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.*;
import org.github.jamm.MemoryMeter;

public class QueryProcessor implements QueryHandler
{
    public static final CassandraVersion CQL_VERSION = new CassandraVersion("3.4.2");

    public static final QueryProcessor instance = new QueryProcessor();

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);
    private static final MemoryMeter meter = new MemoryMeter().withGuessing(MemoryMeter.Guess.FALLBACK_BEST).ignoreKnownSingletons();
    private static final long MAX_CACHE_PREPARED_MEMORY = Runtime.getRuntime().maxMemory() / 256;

    private static final EntryWeigher<MD5Digest, ParsedStatement.Prepared> cqlMemoryUsageWeigher = new EntryWeigher<MD5Digest, ParsedStatement.Prepared>()
    {
        @Override
        public int weightOf(MD5Digest key, ParsedStatement.Prepared value)
        {
            return Ints.checkedCast(measure(key) + measure(value.statement) + measure(value.boundNames));
        }
    };

    private static final EntryWeigher<Integer, ParsedStatement.Prepared> thriftMemoryUsageWeigher = new EntryWeigher<Integer, ParsedStatement.Prepared>()
    {
        @Override
        public int weightOf(Integer key, ParsedStatement.Prepared value)
        {
            return Ints.checkedCast(measure(key) + measure(value.statement) + measure(value.boundNames));
        }
    };

    private static final ConcurrentLinkedHashMap<MD5Digest, ParsedStatement.Prepared> preparedStatements;
    private static final ConcurrentLinkedHashMap<Integer, ParsedStatement.Prepared> thriftPreparedStatements;

    // A map for prepared statements used internally (which we don't want to mix with user statement, in particular we don't
    // bother with expiration on those.
    private static final ConcurrentMap<String, ParsedStatement.Prepared> internalStatements = new ConcurrentHashMap<>();

    // Direct calls to processStatement do not increment the preparedStatementsExecuted/regularStatementsExecuted
    // counters. Callers of processStatement are responsible for correctly notifying metrics
    public static final CQLMetrics metrics = new CQLMetrics();

    private static final AtomicInteger lastMinuteEvictionsCount = new AtomicInteger(0);

    static
    {
        preparedStatements = new ConcurrentLinkedHashMap.Builder<MD5Digest, ParsedStatement.Prepared>()
                             .maximumWeightedCapacity(MAX_CACHE_PREPARED_MEMORY)
                             .weigher(cqlMemoryUsageWeigher)
                             .listener(new EvictionListener<MD5Digest, ParsedStatement.Prepared>()
                             {
                                 public void onEviction(MD5Digest md5Digest, ParsedStatement.Prepared prepared)
                                 {
                                     metrics.preparedStatementsEvicted.inc();
                                     lastMinuteEvictionsCount.incrementAndGet();
                                 }
                             }).build();

        thriftPreparedStatements = new ConcurrentLinkedHashMap.Builder<Integer, ParsedStatement.Prepared>()
                                   .maximumWeightedCapacity(MAX_CACHE_PREPARED_MEMORY)
                                   .weigher(thriftMemoryUsageWeigher)
                                   .listener(new EvictionListener<Integer, ParsedStatement.Prepared>()
                                   {
                                       public void onEviction(Integer integer, ParsedStatement.Prepared prepared)
                                       {
                                           metrics.preparedStatementsEvicted.inc();
                                           lastMinuteEvictionsCount.incrementAndGet();
                                       }
                                   })
                                   .build();

        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(new Runnable()
        {
            public void run()
            {
                long count = lastMinuteEvictionsCount.getAndSet(0);
                if (count > 0)
                    logger.info("{} prepared statements discarded in the last minute because cache limit reached ({})",
                                count,
                                FBUtilities.prettyPrintMemory(MAX_CACHE_PREPARED_MEMORY));
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public static int preparedStatementsCount()
    {
        return preparedStatements.size() + thriftPreparedStatements.size();
    }

    // Work around initialization dependency
    private static enum InternalStateInstance
    {
        INSTANCE;

        private final QueryState queryState;

        InternalStateInstance()
        {
            ClientState state = ClientState.forInternalCalls();
            state.setKeyspace(SystemKeyspace.NAME);
            this.queryState = new QueryState(state);
        }
    }

    private static QueryState internalQueryState()
    {
        return InternalStateInstance.INSTANCE.queryState;
    }

    private QueryProcessor()
    {
        MigrationManager.instance.register(new MigrationSubscriber());
    }

    public ParsedStatement.Prepared getPrepared(MD5Digest id)
    {
        return preparedStatements.get(id);
    }

    public ParsedStatement.Prepared getPreparedForThrift(Integer id)
    {
        return thriftPreparedStatements.get(id);
    }

    public static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new InvalidRequestException("Key may not be empty");
        }
        if (key == ByteBufferUtil.UNSET_BYTE_BUFFER)
            throw new InvalidRequestException("Key may not be unset");

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
        }
    }

    public ResultMessage processStatement(CQLStatement statement, QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        logger.trace("Process {} @CL.{}", statement, options.getConsistency());
        ClientState clientState = queryState.getClientState();
        statement.checkAccess(clientState);
        statement.validate(clientState);

        ResultMessage result = statement.execute(queryState, options);
        return result == null ? new ResultMessage.Void() : result;
    }

    public static ResultMessage process(String queryString, ConsistencyLevel cl, QueryState queryState)
    throws RequestExecutionException, RequestValidationException
    {
        return instance.process(queryString, queryState, QueryOptions.forInternalCalls(cl, Collections.<ByteBuffer>emptyList()));
    }

    public ResultMessage process(String query,
                                 QueryState state,
                                 QueryOptions options,
                                 Map<String, ByteBuffer> customPayload)
                                         throws RequestExecutionException, RequestValidationException
    {
        return process(query, state, options);
    }

    public ResultMessage process(String queryString, QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        ParsedStatement.Prepared p = getStatement(queryString, queryState.getClientState());
        options.prepare(p.boundNames);
        CQLStatement prepared = p.statement;
        if (prepared.getBoundTerms() != options.getValues().size())
            throw new InvalidRequestException("Invalid amount of bind variables");

        if (!queryState.getClientState().isInternal)
            metrics.regularStatementsExecuted.inc();

        return processStatement(prepared, queryState, options);
    }

    public static ParsedStatement.Prepared parseStatement(String queryStr, QueryState queryState) throws RequestValidationException
    {
        return getStatement(queryStr, queryState.getClientState());
    }

    public static UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        return process(query, cl, Collections.<ByteBuffer>emptyList());
    }

    public static UntypedResultSet process(String query, ConsistencyLevel cl, List<ByteBuffer> values) throws RequestExecutionException
    {
        ResultMessage result = instance.process(query, QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, values));
        if (result instanceof ResultMessage.Rows)
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    private static QueryOptions makeInternalOptions(ParsedStatement.Prepared prepared, Object[] values)
    {
        return makeInternalOptions(prepared, values, ConsistencyLevel.ONE);
    }

    private static QueryOptions makeInternalOptions(ParsedStatement.Prepared prepared, Object[] values, ConsistencyLevel cl)
    {
        if (prepared.boundNames.size() != values.length)
            throw new IllegalArgumentException(String.format("Invalid number of values. Expecting %d but got %d", prepared.boundNames.size(), values.length));

        List<ByteBuffer> boundValues = new ArrayList<ByteBuffer>(values.length);
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            AbstractType type = prepared.boundNames.get(i).type;
            boundValues.add(value instanceof ByteBuffer || value == null ? (ByteBuffer)value : type.decompose(value));
        }
        return QueryOptions.forInternalCalls(cl, boundValues);
    }

    private static ParsedStatement.Prepared prepareInternal(String query) throws RequestValidationException
    {
        ParsedStatement.Prepared prepared = internalStatements.get(query);
        if (prepared != null)
            return prepared;

        // Note: if 2 threads prepare the same query, we'll live so don't bother synchronizing
        prepared = parseStatement(query, internalQueryState());
        prepared.statement.validate(internalQueryState().getClientState());
        internalStatements.putIfAbsent(query, prepared);
        return prepared;
    }

    public static UntypedResultSet executeInternal(String query, Object... values)
    {
        ParsedStatement.Prepared prepared = prepareInternal(query);
        ResultMessage result = prepared.statement.executeInternal(internalQueryState(), makeInternalOptions(prepared, values));
        if (result instanceof ResultMessage.Rows)
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    public static UntypedResultSet execute(String query, ConsistencyLevel cl, Object... values)
    throws RequestExecutionException
    {
        return execute(query, cl, internalQueryState(), values);
    }

    public static UntypedResultSet execute(String query, ConsistencyLevel cl, QueryState state, Object... values)
    throws RequestExecutionException
    {
        try
        {
            ParsedStatement.Prepared prepared = prepareInternal(query);
            ResultMessage result = prepared.statement.execute(state, makeInternalOptions(prepared, values, cl));
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
        ParsedStatement.Prepared prepared = prepareInternal(query);
        if (!(prepared.statement instanceof SelectStatement))
            throw new IllegalArgumentException("Only SELECTs can be paged");

        SelectStatement select = (SelectStatement)prepared.statement;
        QueryPager pager = select.getQuery(makeInternalOptions(prepared, values), FBUtilities.nowInSeconds()).getPager(null, Server.CURRENT_VERSION);
        return UntypedResultSet.create(select, pager, pageSize);
    }

    /**
     * Same than executeInternal, but to use for queries we know are only executed once so that the
     * created statement object is not cached.
     */
    public static UntypedResultSet executeOnceInternal(String query, Object... values)
    {
        ParsedStatement.Prepared prepared = parseStatement(query, internalQueryState());
        prepared.statement.validate(internalQueryState().getClientState());
        ResultMessage result = prepared.statement.executeInternal(internalQueryState(), makeInternalOptions(prepared, values));
        if (result instanceof ResultMessage.Rows)
            return UntypedResultSet.create(((ResultMessage.Rows)result).result);
        else
            return null;
    }

    public static UntypedResultSet resultify(String query, RowIterator partition)
    {
        return resultify(query, PartitionIterators.singletonIterator(partition));
    }

    public static UntypedResultSet resultify(String query, PartitionIterator partitions)
    {
        try (PartitionIterator iter = partitions)
        {
            SelectStatement ss = (SelectStatement) getStatement(query, null).statement;
            ResultSet cqlRows = ss.process(iter, FBUtilities.nowInSeconds());
            return UntypedResultSet.create(cqlRows);
        }
    }

    public ResultMessage.Prepared prepare(String query,
                                          QueryState state,
                                          Map<String, ByteBuffer> customPayload) throws RequestValidationException
    {
        return prepare(query, state);
    }

    public ResultMessage.Prepared prepare(String queryString, QueryState queryState)
    {
        ClientState cState = queryState.getClientState();
        return prepare(queryString, cState, cState instanceof ThriftClientState);
    }

    public static ResultMessage.Prepared prepare(String queryString, ClientState clientState, boolean forThrift)
    {
        ResultMessage.Prepared existing = getStoredPreparedStatement(queryString, clientState.getRawKeyspace(), forThrift);
        if (existing != null)
            return existing;

        ParsedStatement.Prepared prepared = getStatement(queryString, clientState);
        int boundTerms = prepared.statement.getBoundTerms();
        if (boundTerms > FBUtilities.MAX_UNSIGNED_SHORT)
            throw new InvalidRequestException(String.format("Too many markers(?). %d markers exceed the allowed maximum of %d", boundTerms, FBUtilities.MAX_UNSIGNED_SHORT));
        assert boundTerms == prepared.boundNames.size();

        return storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared, forThrift);
    }

    private static MD5Digest computeId(String queryString, String keyspace)
    {
        String toHash = keyspace == null ? queryString : keyspace + queryString;
        return MD5Digest.compute(toHash);
    }

    private static Integer computeThriftId(String queryString, String keyspace)
    {
        String toHash = keyspace == null ? queryString : keyspace + queryString;
        return toHash.hashCode();
    }

    private static ResultMessage.Prepared getStoredPreparedStatement(String queryString, String keyspace, boolean forThrift)
    throws InvalidRequestException
    {
        if (forThrift)
        {
            Integer thriftStatementId = computeThriftId(queryString, keyspace);
            ParsedStatement.Prepared existing = thriftPreparedStatements.get(thriftStatementId);
            return existing == null ? null : ResultMessage.Prepared.forThrift(thriftStatementId, existing.boundNames);
        }
        else
        {
            MD5Digest statementId = computeId(queryString, keyspace);
            ParsedStatement.Prepared existing = preparedStatements.get(statementId);
            return existing == null ? null : new ResultMessage.Prepared(statementId, existing);
        }
    }

    private static ResultMessage.Prepared storePreparedStatement(String queryString, String keyspace, ParsedStatement.Prepared prepared, boolean forThrift)
    throws InvalidRequestException
    {
        // Concatenate the current keyspace so we don't mix prepared statements between keyspace (#5352).
        // (if the keyspace is null, queryString has to have a fully-qualified keyspace so it's fine.
        long statementSize = measure(prepared.statement);
        // don't execute the statement if it's bigger than the allowed threshold
        if (statementSize > MAX_CACHE_PREPARED_MEMORY)
            throw new InvalidRequestException(String.format("Prepared statement of size %d bytes is larger than allowed maximum of %d bytes.",
                                                            statementSize,
                                                            MAX_CACHE_PREPARED_MEMORY));
        if (forThrift)
        {
            Integer statementId = computeThriftId(queryString, keyspace);
            thriftPreparedStatements.put(statementId, prepared);
            return ResultMessage.Prepared.forThrift(statementId, prepared.boundNames);
        }
        else
        {
            MD5Digest statementId = computeId(queryString, keyspace);
            preparedStatements.put(statementId, prepared);
            return new ResultMessage.Prepared(statementId, prepared);
        }
    }

    public ResultMessage processPrepared(CQLStatement statement,
                                         QueryState state,
                                         QueryOptions options,
                                         Map<String, ByteBuffer> customPayload)
                                                 throws RequestExecutionException, RequestValidationException
    {
        return processPrepared(statement, state, options);
    }

    public ResultMessage processPrepared(CQLStatement statement, QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> variables = options.getValues();
        // Check to see if there are any bound variables to verify
        if (!(variables.isEmpty() && (statement.getBoundTerms() == 0)))
        {
            if (variables.size() != statement.getBoundTerms())
                throw new InvalidRequestException(String.format("there were %d markers(?) in CQL but %d bound variables",
                                                                statement.getBoundTerms(),
                                                                variables.size()));

            // at this point there is a match in count between markers and variables that is non-zero

            if (logger.isTraceEnabled())
                for (int i = 0; i < variables.size(); i++)
                    logger.trace("[{}] '{}'", i+1, variables.get(i));
        }

        metrics.preparedStatementsExecuted.inc();
        return processStatement(statement, queryState, options);
    }

    public ResultMessage processBatch(BatchStatement statement,
                                      QueryState state,
                                      BatchQueryOptions options,
                                      Map<String, ByteBuffer> customPayload)
                                              throws RequestExecutionException, RequestValidationException
    {
        return processBatch(statement, state, options);
    }

    public ResultMessage processBatch(BatchStatement batch, QueryState queryState, BatchQueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        ClientState clientState = queryState.getClientState();
        batch.checkAccess(clientState);
        batch.validate();
        batch.validate(clientState);
        return batch.execute(queryState, options);
    }

    public static ParsedStatement.Prepared getStatement(String queryStr, ClientState clientState)
    throws RequestValidationException
    {
        Tracing.trace("Parsing {}", queryStr);
        ParsedStatement statement = parseStatement(queryStr);

        // Set keyspace for statement that require login
        if (statement instanceof CFStatement)
            ((CFStatement)statement).prepareKeyspace(clientState);

        Tracing.trace("Preparing statement");
        return statement.prepare();
    }

    public static ParsedStatement parseStatement(String queryStr) throws SyntaxException
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

    private static long measure(Object key)
    {
        return meter.measureDeep(key);
    }

    /**
     * Clear our internal statmeent cache for test purposes.
     */
    @VisibleForTesting
    public static void clearInternalStatementsCache()
    {
        internalStatements.clear();
    }

    private static class MigrationSubscriber extends MigrationListener
    {
        private void removeInvalidPreparedStatements(String ksName, String cfName)
        {
            removeInvalidPreparedStatements(internalStatements.values().iterator(), ksName, cfName);
            removeInvalidPreparedStatements(preparedStatements.values().iterator(), ksName, cfName);
            removeInvalidPreparedStatements(thriftPreparedStatements.values().iterator(), ksName, cfName);
        }

        private void removeInvalidPreparedStatements(Iterator<ParsedStatement.Prepared> iterator, String ksName, String cfName)
        {
            while (iterator.hasNext())
            {
                if (shouldInvalidate(ksName, cfName, iterator.next().statement))
                    iterator.remove();
            }
        }

        private boolean shouldInvalidate(String ksName, String cfName, CQLStatement statement)
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
            if (Schema.instance.getKSMetaData(ksName).functions.get(new FunctionName(ksName, functionName)).size() > 1)
                removeAllInvalidPreparedStatementsForFunction(ksName, functionName);
        }

        public void onUpdateColumnFamily(String ksName, String cfName, boolean affectsStatements)
        {
            logger.trace("Column definitions for {}.{} changed, invalidating related prepared statements", ksName, cfName);
            if (affectsStatements)
                removeInvalidPreparedStatements(ksName, cfName);
        }

        public void onUpdateFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            // Updating a function may imply we've changed the body of the function, so we need to invalid statements so that
            // the new definition is picked (the function is resolved at preparation time).
            // TODO: if the function has multiple overload, we could invalidate only the statement refering to the overload
            // that was updated. This requires a few changes however and probably doesn't matter much in practice.
            removeAllInvalidPreparedStatementsForFunction(ksName, functionName);
        }

        public void onUpdateAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
        {
            // Updating a function may imply we've changed the body of the function, so we need to invalid statements so that
            // the new definition is picked (the function is resolved at preparation time).
            // TODO: if the function has multiple overload, we could invalidate only the statement refering to the overload
            // that was updated. This requires a few changes however and probably doesn't matter much in practice.
            removeAllInvalidPreparedStatementsForFunction(ksName, aggregateName);
        }

        public void onDropKeyspace(String ksName)
        {
            logger.trace("Keyspace {} was dropped, invalidating related prepared statements", ksName);
            removeInvalidPreparedStatements(ksName, null);
        }

        public void onDropColumnFamily(String ksName, String cfName)
        {
            logger.trace("Table {}.{} was dropped, invalidating related prepared statements", ksName, cfName);
            removeInvalidPreparedStatements(ksName, cfName);
        }

        public void onDropFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            removeAllInvalidPreparedStatementsForFunction(ksName, functionName);
        }

        public void onDropAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
        {
            removeAllInvalidPreparedStatementsForFunction(ksName, aggregateName);
        }

        private static void removeAllInvalidPreparedStatementsForFunction(String ksName, String functionName)
        {
            removeInvalidPreparedStatementsForFunction(internalStatements.values().iterator(), ksName, functionName);
            removeInvalidPreparedStatementsForFunction(preparedStatements.values().iterator(), ksName, functionName);
            removeInvalidPreparedStatementsForFunction(thriftPreparedStatements.values().iterator(), ksName, functionName);
        }

        private static void removeInvalidPreparedStatementsForFunction(Iterator<ParsedStatement.Prepared> statements,
                                                                       final String ksName,
                                                                       final String functionName)
        {
            Predicate<Function> matchesFunction = f -> ksName.equals(f.name().keyspace) && functionName.equals(f.name().name);
            Iterators.removeIf(statements, statement -> Iterables.any(statement.statement.getFunctions(), matchesFunction));
        }
    }
}
