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

package org.apache.cassandra.service.accord.txn;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import accord.primitives.Keys;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import accord.api.Key;
import accord.primitives.Txn;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.api.PartitionKey;

public class TxnBuilder
{
    private final List<TxnNamedRead> reads = new ArrayList<>();
    private final List<TxnWrite.Fragment> writes = new ArrayList<>();
    private final List<TxnCondition> conditions = new ArrayList<>();

    public static TxnBuilder builder()
    {
        return new TxnBuilder();
    }

    public TxnBuilder withRead(String name, String query)
    {
        return withRead(TxnDataName.user(name), query, VariableSpecifications.empty());
    }

    public TxnBuilder withRead(TxnDataName name, String query)
    {
        return withRead(name, query, VariableSpecifications.empty());
    }

    public TxnBuilder withRead(TxnDataName name, String query, VariableSpecifications bindVariables, Object... values)
    {
        SelectStatement.RawStatement parsed = (SelectStatement.RawStatement) QueryProcessor.parseStatement(query);
        // the parser will only let us define a ref name if we're parsing a transaction, which we're not
        // so we need to manually add it in the call, and confirm nothing got parsed
        Preconditions.checkState(parsed.parameters.refName == null);

        SelectStatement statement = parsed.prepare(bindVariables);
        QueryOptions queryOptions = QueryProcessor.makeInternalOptions(statement, values);
        ReadQuery readQuery = statement.getQuery(queryOptions, 0);
        SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) readQuery;
        reads.add(new TxnNamedRead(name, Iterables.getOnlyElement(selectQuery.queries)));
        return this;
    }

    public TxnBuilder withWrite(PartitionUpdate update, TxnReferenceOperations referenceOps)
    {
        int index = writes.size();
        writes.add(new TxnWrite.Fragment(PartitionKey.of(update), index, update, referenceOps));
        return this;
    }

    public TxnBuilder withWrite(String query, TxnReferenceOperations referenceOps, VariableSpecifications variables, Object... values)
    {
        ModificationStatement.Parsed parsed = (ModificationStatement.Parsed) QueryProcessor.parseStatement(query);
        ModificationStatement prepared = parsed.prepare(variables);
        QueryOptions options = QueryProcessor.makeInternalOptions(prepared, values);
        return withWrite(prepared.getTxnUpdate(ClientState.forInternalCalls(), options), referenceOps);
    }

    public TxnBuilder withWrite(String query)
    {
        return withWrite(query, TxnReferenceOperations.empty(), VariableSpecifications.empty());
    }

    static TxnReference reference(TxnDataName name, String column)
    {
        ColumnMetadata metadata = null;
        if (column != null)
        {
            String[] parts = column.split("\\.");
            Preconditions.checkArgument(parts.length == 3);
            TableMetadata table = Schema.instance.getTableMetadata(parts[0], parts[1]);
            Preconditions.checkArgument(table != null);
            metadata = table.getColumn(new ColumnIdentifier(parts[2], true));
            Preconditions.checkArgument(metadata != null);
        }
        return new TxnReference(name, metadata);
    }

    private TxnBuilder withCondition(TxnCondition condition)
    {
        conditions.add(condition);
        return this;
    }

    public TxnBuilder withValueCondition(TxnDataName name, String column, TxnCondition.Kind kind, ByteBuffer value)
    {
        return withCondition(new TxnCondition.Value(reference(name, column), kind, value));
    }

    public TxnBuilder withEqualsCondition(String name, String column, ByteBuffer value)
    {
        return withValueCondition(TxnDataName.user(name), column, TxnCondition.Kind.EQUAL, value);
    }

    private TxnBuilder withExistenceCondition(TxnDataName name, String column, TxnCondition.Kind kind)
    {
        return withCondition(new TxnCondition.Exists(reference(name, column), kind));
    }

    public TxnBuilder withIsNotNullCondition(TxnDataName name, String column)
    {
        return withExistenceCondition(name, column, TxnCondition.Kind.IS_NOT_NULL);
    }

    public TxnBuilder withIsNullCondition(TxnDataName name, String column)
    {
        return withExistenceCondition(name, column, TxnCondition.Kind.IS_NULL);
    }

    Keys toKeys(SortedSet<Key> keySet)
    {
        return new Keys(keySet);
    }

    public Txn build()
    {
        SortedSet<Key> keySet = new TreeSet<>();

        List<TxnNamedRead> namedReads = new ArrayList<>(reads.size());
        for (TxnNamedRead read : reads)
        {
            keySet.add(read.key());
            namedReads.add(read);
        }

        if (writes.isEmpty())
        {
            Preconditions.checkState(conditions.isEmpty());
            Keys txnKeys = toKeys(keySet);
            TxnRead read = new TxnRead(namedReads, txnKeys);
            return new Txn.InMemory(txnKeys, read, TxnQuery.ALL);
        }
        else
        {
            TxnCondition condition;
            if (conditions.isEmpty())
                condition = TxnCondition.none();
            else if (conditions.size() == 1)
                condition = conditions.get(0);
            else
                condition = new TxnCondition.BooleanGroup(TxnCondition.Kind.AND, conditions);

            writes.forEach(write -> keySet.add(write.key));
            TxnUpdate update = new TxnUpdate(writes, condition);
            Keys txnKeys = toKeys(keySet);
            TxnRead read = new TxnRead(namedReads, txnKeys);
            return new Txn.InMemory(txnKeys, read, TxnQuery.ALL, update);
        }
    }
}
