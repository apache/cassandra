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

package org.apache.cassandra.harry.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.cql.DeleteHelper;
import org.apache.cassandra.harry.cql.SelectHelper;
import org.apache.cassandra.harry.cql.WriteHelper;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;

// TODO: this class can be substantially improved by removing internal mutable state
public class QueryBuildingVisitExecutor extends VisitExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(QueryBuildingVisitExecutor.class);
    protected final SchemaSpec schema;
    protected final WrapQueries wrapQueries;

    public QueryBuildingVisitExecutor(SchemaSpec schema, WrapQueries wrapQueries)
    {
        this.schema = schema;
        this.wrapQueries = wrapQueries;
    }

    public BuiltQuery compile(Visit visit)
    {
        beginLts(visit.lts);
        for (Operations.Operation op : visit.operations)
        {
            if (logger.isTraceEnabled())
                logger.trace("{} {}", visit.lts, op);
            operation(op);
        }

        // TODO: try inducing timeouts and checking non-propagation or discovery
        endLts(visit.lts);

        if (!statements.isEmpty())
        {
            Object[] bindingsArray = new Object[bindings.size()];
            bindings.toArray(bindingsArray);
            BuiltQuery query = new BuiltQuery(selects,
                                   wrapQueries.wrap(visit, String.join("\n ", statements)),
                                   bindingsArray);
            clear();
            return query;
        }

        Invariants.checkState(bindings.isEmpty() && visitedPds.isEmpty() && selects.isEmpty());
        return null;
    }

    public static class BuiltQuery extends CompiledStatement
    {
        protected final List<Operations.SelectStatement> selects;
        public BuiltQuery(List<Operations.SelectStatement> selects, String cql, Object... bindings)
        {
            super(cql, bindings);
            this.selects = selects;
        }
    }

    /**
     * Per-LTS state
     */
    private final List<String> statements = new ArrayList<>();
    private final List<Object> bindings = new ArrayList<>();
    private final Set<Long> visitedPds = new HashSet<>();

    private List<Operations.SelectStatement> selects = null;

    protected void beginLts(long lts)
    {
        statements.clear();
        bindings.clear();
        visitedPds.clear();
        selects = new ArrayList<>();
    }

    protected void endLts(long lts)
    {
        if (statements.isEmpty())
        {
            Invariants.checkState(bindings.isEmpty() && visitedPds.isEmpty() && selects.isEmpty());
            return;
        }

        assert visitedPds.size() == 1 : String.format("Token aware only works with a single value per token, but got %s", visitedPds);
    }

    private void clear()
    {
        statements.clear();
        bindings.clear();
        visitedPds.clear();
        selects = null;
    }

    protected void operation(Operations.Operation operation)
    {
        if (operation instanceof Operations.PartitionOperation)
            visitedPds.add(((Operations.PartitionOperation) operation).pd());
        CompiledStatement statement;
        switch (operation.kind())
        {
            case UPDATE:
                statement = WriteHelper.inflateUpdate((Operations.WriteOp) operation, schema, operation.lts());
                break;
            case INSERT:
                statement = WriteHelper.inflateInsert((Operations.WriteOp) operation, schema, operation.lts());
                break;
            case DELETE_RANGE:
                statement = DeleteHelper.inflateDelete((Operations.DeleteRange) operation, schema, operation.lts());
                break;
            case DELETE_PARTITION:
                statement = DeleteHelper.inflateDelete((Operations.DeletePartition) operation, schema, operation.lts());
                break;
            case DELETE_ROW:
                statement = DeleteHelper.inflateDelete((Operations.DeleteRow) operation, schema, operation.lts());
                break;
            case DELETE_COLUMNS:
                statement = DeleteHelper.inflateDelete((Operations.DeleteColumns) operation, schema, operation.lts());
                break;
            case SELECT_PARTITION:
                statement = SelectHelper.select((Operations.SelectPartition) operation, schema);
                selects.add((Operations.SelectStatement) operation);
                break;
            case SELECT_ROW:
                statement = SelectHelper.select((Operations.SelectRow) operation, schema);
                selects.add((Operations.SelectStatement) operation);
                break;
            case SELECT_RANGE:
                statement = SelectHelper.select((Operations.SelectRange) operation, schema);
                selects.add((Operations.SelectStatement) operation);
                break;
            case SELECT_CUSTOM:
                statement = SelectHelper.select((Operations.SelectCustom) operation, schema);
                selects.add((Operations.SelectStatement) operation);
                break;

            case CUSTOM:
                ((Operations.CustomRunnableOperation) operation).execute();
                return;
            default:
                throw new IllegalArgumentException();
        }
        statements.add(statement.cql());
        Collections.addAll(bindings, statement.bindings());
    }

    private static final String wrapInUnloggedBatchFormat = "BEGIN UNLOGGED BATCH\n" +
                                                            "  %s\n" +
                                                            "APPLY BATCH;";

    private static final String wrapInTxnFormat = "BEGIN TRANSACTION\n" +
                                                  "  %s\n" +
                                                  "COMMIT TRANSACTION;";

    public interface WrapQueries
    {
        WrapQueries UNLOGGED_BATCH = (visit, compiled) -> {
            if (visit.operations.length == 1)
                return compiled;
            return String.format(wrapInUnloggedBatchFormat, compiled);
        };

        WrapQueries TRANSACTION = (visit, compiled) -> String.format(wrapInTxnFormat, compiled);


        String wrap(Visit visit, String compiled);
    }
}
