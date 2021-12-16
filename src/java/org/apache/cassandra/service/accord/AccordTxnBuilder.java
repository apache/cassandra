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

package org.apache.cassandra.service.accord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.txn.Keys;
import accord.txn.Txn;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.db.AccordQuery;
import org.apache.cassandra.service.accord.db.AccordRead;
import org.apache.cassandra.service.accord.db.AccordUpdate;
import org.apache.cassandra.service.accord.db.AccordUpdate.UpdatePredicate;
import org.apache.cassandra.service.accord.db.AccordUpdate.UpdatePredicate.Type;

public class AccordTxnBuilder
{
    private Set<AccordKey.PartitionKey> keys = new HashSet<>();
    private List<SinglePartitionReadCommand> reads = new ArrayList<>();
    private AccordQuery query = AccordQuery.ALL;
    private List<PartitionUpdate> updates = new ArrayList<>();
    private List<UpdatePredicate> predicates = new ArrayList<>();

    public AccordTxnBuilder withRead(String query, Object... values)
    {
        QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
        Preconditions.checkArgument(prepared.statement instanceof SelectStatement);

        SelectStatement select = (SelectStatement)prepared.statement;
        ReadQuery readQuery = select.getQuery(QueryProcessor.makeInternalOptions(prepared.statement, values), 0);
        SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) readQuery;
        for (SinglePartitionReadCommand command : selectQuery.queries)
        {
            keys.add(new AccordKey.PartitionKey(command.tableId(), command.partitionKey()));
            reads.add(command);
        }
        return this;
    }

    public AccordTxnBuilder withWrite(String query, Object... values)
    {
        QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
        Preconditions.checkArgument(prepared.statement instanceof ModificationStatement);
        ModificationStatement modification = (ModificationStatement) prepared.statement;
        // TODO: look into getting partition updates directly
        List<? extends IMutation> mutations = modification.getMutations(QueryProcessor.makeInternalOptions(prepared.statement, values), false, 0, 0, 0);
        for (IMutation mutation : mutations)
        {
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                keys.add(new AccordKey.PartitionKey(update.tableId(), update.partitionKey()));
                updates.add(update);
            }
        }
        return this;
    }

    private static ByteBuffer decompose(AbstractType<?> type, Object value)
    {
        return ((AbstractType<Object>) type).decompose(value);
    }

    public AccordTxnBuilder withCondition(String keyspace, String table, Object key, Object clustering, String column, Type type, Object value)
    {
        Preconditions.checkState(!updates.isEmpty());
        boolean isExistsPredicate = type == Type.EXISTS || type == Type.NOT_EXISTS;
        Preconditions.checkArgument((value != null && !isExistsPredicate) || (value == null && isExistsPredicate));

        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
        Preconditions.checkNotNull(metadata);

        DecoratedKey decoratedKey = metadata.partitioner.decorateKey(decompose(metadata.partitionKeyType, key));
        Clustering<ByteBuffer> clusteringBytes = Clustering.make(decompose(metadata.comparator.subtype(0), clustering));

        if (isExistsPredicate)
        {
            predicates.add(new AccordUpdate.ExistsPredicate(type, metadata, decoratedKey, clusteringBytes));
        }
        else
        {
            ColumnMetadata columnMetadata = metadata.getColumn(new ColumnIdentifier(column, true));
            Preconditions.checkNotNull(columnMetadata);
            ByteBuffer valueBytes = decompose(columnMetadata.type, value);
            predicates.add(new AccordUpdate.ValuePredicate(type, metadata, decoratedKey, clusteringBytes, columnMetadata, valueBytes));
        }

        return this;
    }

    public AccordTxnBuilder withCondition(String keyspace, String table, Object key, Object clustering, Type type)
    {
        return withCondition(keyspace, table, key, clustering, null, type, null);
    }

    public Txn build()
    {
        Key[] keyArray = keys.toArray(Key[]::new);
        Arrays.sort(keyArray, Comparator.naturalOrder());
        if (updates.isEmpty())
        {
            return new Txn(new Keys(keyArray), new AccordRead(reads), query);
        }
        else
        {
            List<UpdatePredicate> unsatisfiable = predicates.stream()
                                                            .filter(predicate -> !reads.stream().anyMatch(predicate::supportedByRead))
                                                            .collect(Collectors.toList());

            if (!unsatisfiable.isEmpty())
            {
                String msg = String.format("Some predicates are unsupported by a read, and are unsatisfiable: %s", unsatisfiable);
                throw new IllegalStateException(msg);
            }

            return new Txn(new Keys(keyArray), new AccordRead(reads), query, new AccordUpdate(updates, predicates));
        }
    }

}
