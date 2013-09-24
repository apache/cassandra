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

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.antlr.runtime.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.SemanticVersion;

public class QueryProcessor
{
    public static final SemanticVersion CQL_VERSION = new SemanticVersion("3.1.1");

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

    public static final int MAX_CACHE_PREPARED = 100000; // Enough to keep buggy clients from OOM'ing us
    private static final Map<MD5Digest, CQLStatement> preparedStatements = new ConcurrentLinkedHashMap.Builder<MD5Digest, CQLStatement>()
                                                                               .maximumWeightedCapacity(MAX_CACHE_PREPARED)
                                                                               .build();

    private static final Map<Integer, CQLStatement> thriftPreparedStatements = new ConcurrentLinkedHashMap.Builder<Integer, CQLStatement>()
                                                                                   .maximumWeightedCapacity(MAX_CACHE_PREPARED)
                                                                                   .build();


    public static CQLStatement getPrepared(MD5Digest id)
    {
        return preparedStatements.get(id);
    }

    public static CQLStatement getPrepared(Integer id)
    {
        return thriftPreparedStatements.get(id);
    }

    public static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new InvalidRequestException("Key may not be empty");
        }

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
        }
    }

    public static void validateColumnNames(Iterable<ByteBuffer> columns)
    throws InvalidRequestException
    {
        for (ByteBuffer name : columns)
        {
            if (name.remaining() > Column.MAX_NAME_LENGTH)
                throw new InvalidRequestException(String.format("column name is too long (%s > %s)",
                                                                name.remaining(),
                                                                Column.MAX_NAME_LENGTH));
            if (name.remaining() == 0)
                throw new InvalidRequestException("zero-length column name");
        }
    }

    private static ResultMessage processStatement(CQLStatement statement, QueryState queryState, QueryOptions options)
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
        return process(queryString, queryState, new QueryOptions(cl, Collections.<ByteBuffer>emptyList()));
    }

    public static ResultMessage process(String queryString, QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        CQLStatement prepared = getStatement(queryString, queryState.getClientState()).statement;
        if (prepared.getBoundsTerms() != options.getValues().size())
            throw new InvalidRequestException("Invalid amount of bind variables");
        return processStatement(prepared, queryState, options);
    }

    public static CQLStatement parseStatement(String queryStr, QueryState queryState) throws RequestValidationException
    {
        return getStatement(queryStr, queryState.getClientState()).statement;
    }

    public static UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        try
        {
            ResultMessage result = process(query, QueryState.forInternalCalls(), new QueryOptions(cl, Collections.<ByteBuffer>emptyList()));
            if (result instanceof ResultMessage.Rows)
                return new UntypedResultSet(((ResultMessage.Rows)result).result);
            else
                return null;
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static UntypedResultSet processInternal(String query)
    {
        try
        {
            ClientState state = ClientState.forInternalCalls();
            QueryState qState = new QueryState(state);
            state.setKeyspace(Keyspace.SYSTEM_KS);
            CQLStatement statement = getStatement(query, state).statement;
            statement.validate(state);
            ResultMessage result = statement.executeInternal(qState);
            if (result instanceof ResultMessage.Rows)
                return new UntypedResultSet(((ResultMessage.Rows)result).result);
            else
                return null;
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException("Error validating " + query, e);
        }
    }

    public static UntypedResultSet resultify(String query, Row row)
    {
        try
        {
            SelectStatement ss = (SelectStatement) getStatement(query, null).statement;
            ResultSet cqlRows = ss.process(Collections.singletonList(row));
            return new UntypedResultSet(cqlRows);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e);
        }
    }

    public static ResultMessage.Prepared prepare(String queryString, ClientState clientState, boolean forThrift)
    throws RequestValidationException
    {
        ParsedStatement.Prepared prepared = getStatement(queryString, clientState);
        ResultMessage.Prepared msg = storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared, forThrift);

        assert prepared.statement.getBoundsTerms() == prepared.boundNames.size();
        return msg;
    }

    private static ResultMessage.Prepared storePreparedStatement(String queryString, String keyspace, ParsedStatement.Prepared prepared, boolean forThrift)
    {
        // Concatenate the current keyspace so we don't mix prepared statements between keyspace (#5352).
        // (if the keyspace is null, queryString has to have a fully-qualified keyspace so it's fine.
        String toHash = keyspace == null ? queryString : keyspace + queryString;
        if (forThrift)
        {
            int statementId = toHash.hashCode();
            thriftPreparedStatements.put(statementId, prepared.statement);
            logger.trace(String.format("Stored prepared statement #%d with %d bind markers",
                                       statementId,
                                       prepared.statement.getBoundsTerms()));
            return ResultMessage.Prepared.forThrift(statementId, prepared.boundNames);
        }
        else
        {
            MD5Digest statementId = MD5Digest.compute(toHash);
            logger.trace(String.format("Stored prepared statement %s with %d bind markers",
                                       statementId,
                                       prepared.statement.getBoundsTerms()));
            preparedStatements.put(statementId, prepared.statement);
            return new ResultMessage.Prepared(statementId, prepared);
        }
    }

    public static ResultMessage processPrepared(CQLStatement statement, QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> variables = options.getValues();
        // Check to see if there are any bound variables to verify
        if (!(variables.isEmpty() && (statement.getBoundsTerms() == 0)))
        {
            if (variables.size() != statement.getBoundsTerms())
                throw new InvalidRequestException(String.format("there were %d markers(?) in CQL but %d bound variables",
                                                                statement.getBoundsTerms(),
                                                                variables.size()));

            // at this point there is a match in count between markers and variables that is non-zero

            if (logger.isTraceEnabled())
                for (int i = 0; i < variables.size(); i++)
                    logger.trace("[{}] '{}'", i+1, variables.get(i));
        }

        return processStatement(statement, queryState, options);
    }

    public static ResultMessage processBatch(BatchStatement batch, ConsistencyLevel cl, QueryState queryState, List<List<ByteBuffer>> variables)
    throws RequestExecutionException, RequestValidationException
    {
        ClientState clientState = queryState.getClientState();
        batch.checkAccess(clientState);
        batch.validate(clientState);
        batch.executeWithPerStatementVariables(cl, queryState, variables);
        return new ResultMessage.Void();
    }

    private static ParsedStatement.Prepared getStatement(String queryStr, ClientState clientState)
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
            // Lexer and parser
            CharStream stream = new ANTLRStringStream(queryStr);
            CqlLexer lexer = new CqlLexer(stream);
            TokenStream tokenStream = new CommonTokenStream(lexer);
            CqlParser parser = new CqlParser(tokenStream);

            // Parse the query string to a statement instance
            ParsedStatement statement = parser.query();

            // The lexer and parser queue up any errors they may have encountered
            // along the way, if necessary, we turn them into exceptions here.
            lexer.throwLastRecognitionError();
            parser.throwLastRecognitionError();

            return statement;
        }
        catch (RuntimeException re)
        {
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
}
