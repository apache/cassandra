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
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.rows.Row;
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

    // Conditions on the static row. We keep it separate from 'conditions' as most things related to the static row are
    // special cases anyway.
    private RowCondition staticConditions;
    // We index RowCondition by the clustering of the row they applied to for 2 reasons:
    //   1) this allows to keep things sorted to build the read command below
    //   2) this allows to detect when contradictory conditions are set (not exists with some other conditions on the same row)
    private final TreeMap<Clustering, RowCondition> conditions;

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
        addExistsCondition(clustering, new NotExistCondition(clustering), true);
    }

    public void addExist(Clustering clustering) throws InvalidRequestException
    {
        addExistsCondition(clustering, new ExistCondition(clustering), false);
    }

    private void addExistsCondition(Clustering clustering, RowCondition condition, boolean isNotExist)
    {
        assert condition instanceof ExistCondition || condition instanceof NotExistCondition;
        RowCondition previous = getConditionsForRow(clustering);
        if (previous != null && !(previous.getClass().equals(condition.getClass())))
        {
            // these should be prevented by the parser, but it doesn't hurt to check
            throw (previous instanceof NotExistCondition || previous instanceof ExistCondition)
                ? new InvalidRequestException("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row")
                : new InvalidRequestException("Cannot mix IF conditions and IF " + (isNotExist ? "NOT " : "") + "EXISTS for the same row");
        }

        setConditionsForRow(clustering, condition);
        hasExists = true;
    }

    public void addConditions(Clustering clustering, Collection<ColumnCondition> conds, QueryOptions options) throws InvalidRequestException
    {
        RowCondition condition = getConditionsForRow(clustering);
        if (condition == null)
        {
            condition = new ColumnsConditions(clustering);
            setConditionsForRow(clustering, condition);
        }
        else if (!(condition instanceof ColumnsConditions))
        {
            throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
        }
        ((ColumnsConditions)condition).addConditions(conds, options);
    }

    private RowCondition getConditionsForRow(Clustering clustering)
    {
        return clustering == Clustering.STATIC_CLUSTERING ? staticConditions : conditions.get(clustering);
    }

    private void setConditionsForRow(Clustering clustering, RowCondition condition)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
        {
            assert staticConditions == null;
            staticConditions = condition;
        }
        else
        {
            RowCondition previous = conditions.put(clustering, condition);
            assert previous == null;
        }
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
        assert staticConditions != null || !conditions.isEmpty();

        // With only a static condition, we still want to make the distinction between a non-existing partition and one
        // that exists (has some live data) but has not static content. So we query the first live row of the partition.
        if (conditions.isEmpty())
            return SinglePartitionReadCommand.create(cfm,
                                                     nowInSec,
                                                     ColumnFilter.selection(columnsToRead()),
                                                     RowFilter.NONE,
                                                     DataLimits.cqlLimits(1),
                                                     key,
                                                     new ClusteringIndexSliceFilter(Slices.ALL, false));

        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(conditions.navigableKeySet(), false);
        return SinglePartitionReadCommand.create(cfm, nowInSec, key, ColumnFilter.selection(columnsToRead()), filter);
    }

    /**
     * Checks whether the conditions represented by this object applies provided the current state of the partition on
     * which those conditions are.
     *
     * @param current the partition with current data corresponding to these conditions. More precisely, this must be
     * the result of executing the command returned by {@link #readCommand}. This can be empty but it should not be
     * {@code null}.
     * @return whether the conditions represented by this object applies or not.
     */
    public boolean appliesTo(FilteredPartition current) throws InvalidRequestException
    {
        if (staticConditions != null && !staticConditions.appliesTo(current))
            return false;

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

        if (isBatch)
            BatchStatement.verifyBatchSize(Collections.singleton(update));

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
            return current.getRow(clustering) == null;
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
            return current.getRow(clustering) != null;
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
            Row row = current.getRow(clustering);
            for (ColumnCondition.Bound condition : conditions.values())
            {
                if (!condition.appliesTo(row))
                    return false;
            }
            return true;
        }
    }
}
