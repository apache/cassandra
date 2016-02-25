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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.utils.Pair;

/**
 * Processed CAS conditions and update on potentially multiple rows of the same partition.
 */
public class CQL3CasRequest implements CASRequest
{
    public final CFMetaData cfm;
    public final DecoratedKey key;
    public final boolean isBatch;
    private final PartitionColumns conditionColumns;
    private final boolean updatesRegularRows;
    private final boolean updatesStaticRow;
    private boolean hasExists; // whether we have an exist or if not exist condition

    // We index RowCondition by the clustering of the row they applied to for 2 reasons:
    //   1) this allows to keep things sorted to build the ColumnSlice array below
    //   2) this allows to detect when contradictory conditions are set (not exists with some other conditions on the same row)
    private final SortedMap<Clustering, RowCondition> conditions;

    private final List<RowUpdate> updates = new ArrayList<>();

    public CQL3CasRequest(CFMetaData cfm,
                          DecoratedKey key,
                          boolean isBatch,
                          PartitionColumns conditionColumns,
                          boolean updatesRegularRows,
                          boolean updatesStaticRow)
    {
        this.cfm = cfm;
        this.key = key;
        this.conditions = new TreeMap<>(cfm.comparator);
        this.isBatch = isBatch;
        this.conditionColumns = conditionColumns;
        this.updatesRegularRows = updatesRegularRows;
        this.updatesStaticRow = updatesStaticRow;
    }

    public void addRowUpdate(Clustering clustering, ModificationStatement stmt, QueryOptions options, long timestamp)
    {
        updates.add(new RowUpdate(clustering, stmt, options, timestamp));
    }

    public void addNotExist(Clustering clustering) throws InvalidRequestException
    {
        RowCondition previous = conditions.put(clustering, new NotExistCondition(clustering));
        if (previous != null && !(previous instanceof NotExistCondition))
        {
            // these should be prevented by the parser, but it doesn't hurt to check
            if (previous instanceof ExistCondition)
                throw new InvalidRequestException("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row");
            else
                throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
        }
        hasExists = true;
    }

    public void addExist(Clustering clustering) throws InvalidRequestException
    {
        RowCondition previous = conditions.put(clustering, new ExistCondition(clustering));
        // this should be prevented by the parser, but it doesn't hurt to check
        if (previous instanceof NotExistCondition)
            throw new InvalidRequestException("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row");
        hasExists = true;
    }

    public void addConditions(Clustering clustering, Collection<ColumnCondition> conds, QueryOptions options) throws InvalidRequestException
    {
        RowCondition condition = conditions.get(clustering);
        if (condition == null)
        {
            condition = new ColumnsConditions(clustering);
            conditions.put(clustering, condition);
        }
        else if (!(condition instanceof ColumnsConditions))
        {
            throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
        }
        ((ColumnsConditions)condition).addConditions(conds, options);
    }

    private PartitionColumns columnsToRead()
    {
        // If all our conditions are columns conditions (IF x = ?), then it's enough to query
        // the columns from the conditions. If we have a IF EXISTS or IF NOT EXISTS however,
        // we need to query all columns for the row since if the condition fails, we want to
        // return everything to the user. Static columns make this a bit more complex, in that
        // if an insert only static columns, then the existence condition applies only to the
        // static columns themselves, and so we don't want to include regular columns in that
        // case.
        if (hasExists)
        {
            PartitionColumns allColumns = cfm.partitionColumns();
            Columns statics = updatesStaticRow ? allColumns.statics : Columns.NONE;
            Columns regulars = updatesRegularRows ? allColumns.regulars : Columns.NONE;
            return new PartitionColumns(statics, regulars);
        }
        return conditionColumns;
    }

    public SinglePartitionReadCommand readCommand(int nowInSec)
    {
        assert !conditions.isEmpty();
        Slices.Builder builder = new Slices.Builder(cfm.comparator, conditions.size());
        // We always read CQL rows entirely as on CAS failure we want to be able to distinguish between "row exists
        // but all values for which there were conditions are null" and "row doesn't exists", and we can't rely on the
        // row marker for that (see #6623)
        for (Clustering clustering : conditions.keySet())
        {
            if (clustering != Clustering.STATIC_CLUSTERING)
                builder.add(Slice.make(clustering));
        }

        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(builder.build(), false);
        return SinglePartitionReadCommand.create(cfm, nowInSec, key, ColumnFilter.selection(columnsToRead()), filter);
    }

    public boolean appliesTo(FilteredPartition current) throws InvalidRequestException
    {
        for (RowCondition condition : conditions.values())
        {
            if (!condition.appliesTo(current))
                return false;
        }
        return true;
    }

    private PartitionColumns updatedColumns()
    {
        PartitionColumns.Builder builder = PartitionColumns.builder();
        for (RowUpdate upd : updates)
            builder.addAll(upd.stmt.updatedColumns());
        return builder.build();
    }

    public PartitionUpdate makeUpdates(FilteredPartition current) throws InvalidRequestException
    {
        PartitionUpdate update = new PartitionUpdate(cfm, key, updatedColumns(), conditions.size());
        for (RowUpdate upd : updates)
            upd.applyUpdates(current, update);

        Keyspace.openAndGetStore(cfm).indexManager.validate(update);
        return update;
    }

    /**
     * Due to some operation on lists, we can't generate the update that a given Modification statement does before
     * we get the values read by the initial read of Paxos. A RowUpdate thus just store the relevant information
     * (include the statement iself) to generate those updates. We'll have multiple RowUpdate for a Batch, otherwise
     * we'll have only one.
     */
    private class RowUpdate
    {
        private final Clustering clustering;
        private final ModificationStatement stmt;
        private final QueryOptions options;
        private final long timestamp;

        private RowUpdate(Clustering clustering, ModificationStatement stmt, QueryOptions options, long timestamp)
        {
            this.clustering = clustering;
            this.stmt = stmt;
            this.options = options;
            this.timestamp = timestamp;
        }

        public void applyUpdates(FilteredPartition current, PartitionUpdate updates) throws InvalidRequestException
        {
            Map<DecoratedKey, Partition> map = stmt.requiresRead() ? Collections.<DecoratedKey, Partition>singletonMap(key, current) : null;
            UpdateParameters params = new UpdateParameters(cfm, updates.columns(), options, timestamp, stmt.getTimeToLive(options), map);
            stmt.addUpdateForKey(updates, clustering, params);
        }
    }

    private static abstract class RowCondition
    {
        public final Clustering clustering;

        protected RowCondition(Clustering clustering)
        {
            this.clustering = clustering;
        }

        public abstract boolean appliesTo(FilteredPartition current) throws InvalidRequestException;
    }

    private static class NotExistCondition extends RowCondition
    {
        private NotExistCondition(Clustering clustering)
        {
            super(clustering);
        }

        public boolean appliesTo(FilteredPartition current)
        {
            return current == null || current.getRow(clustering) == null;
        }
    }

    private static class ExistCondition extends RowCondition
    {
        private ExistCondition(Clustering clustering)
        {
            super(clustering);
        }

        public boolean appliesTo(FilteredPartition current)
        {
            return current != null && current.getRow(clustering) != null;
        }
    }

    private static class ColumnsConditions extends RowCondition
    {
        private final Multimap<Pair<ColumnIdentifier, ByteBuffer>, ColumnCondition.Bound> conditions = HashMultimap.create();

        private ColumnsConditions(Clustering clustering)
        {
            super(clustering);
        }

        public void addConditions(Collection<ColumnCondition> conds, QueryOptions options) throws InvalidRequestException
        {
            for (ColumnCondition condition : conds)
            {
                ColumnCondition.Bound current = condition.bind(options);
                conditions.put(Pair.create(condition.column.name, current.getCollectionElementValue()), current);
            }
        }

        public boolean appliesTo(FilteredPartition current) throws InvalidRequestException
        {
            if (current == null)
                return conditions.isEmpty();

            for (ColumnCondition.Bound condition : conditions.values())
            {
                if (!condition.appliesTo(current.getRow(clustering)))
                    return false;
            }
            return true;
        }
    }
}
