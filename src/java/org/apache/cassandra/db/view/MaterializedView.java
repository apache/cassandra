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
package org.apache.cassandra.db.view;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.MaterializedViewDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.CFProperties;
import org.apache.cassandra.db.AbstractReadCommandBuilder.SinglePartitionSliceBuilder;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadOrderGroup;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.partitions.AbstractBTreePartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.pager.QueryPager;

/**
 * A Materialized View copies data from a base table into a view table which can be queried independently from the
 * base. Every update which targets the base table must be fed through the {@link MaterializedViewManager} to ensure
 * that if a view needs to be updated, the updates are properly created and fed into the view.
 *
 * This class does the job of translating the base row to the view row.
 *
 * It handles reading existing state and figuring out what tombstones need to be generated.
 *
 * createMutations below is the "main method"
 *
 */
public class MaterializedView
{
    /**
     * The columns should all be updated together, so we use this object as group.
     */
    private static class MVColumns
    {
        //These are the base column definitions in terms of the *views* partitioning.
        //Meaning we can see (for example) the partition key of the view contains a clustering key
        //from the base table.
        public final List<ColumnDefinition> partitionDefs;
        public final List<ColumnDefinition> primaryKeyDefs;
        public final List<ColumnDefinition> baseComplexColumns;

        private MVColumns(List<ColumnDefinition> partitionDefs, List<ColumnDefinition> primaryKeyDefs, List<ColumnDefinition> baseComplexColumns)
        {
            this.partitionDefs = partitionDefs;
            this.primaryKeyDefs = primaryKeyDefs;
            this.baseComplexColumns = baseComplexColumns;
        }
    }

    public final String name;

    private final ColumnFamilyStore baseCfs;
    private ColumnFamilyStore _viewCfs = null;

    private MVColumns columns;

    private final boolean viewHasAllPrimaryKeys;
    private final boolean includeAll;
    private MaterializedViewBuilder builder;

    public MaterializedView(MaterializedViewDefinition definition,
                            ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;

        name = definition.viewName;
        includeAll = definition.includeAll;

        viewHasAllPrimaryKeys = updateDefinition(definition);
    }

    /**
     * Lazily fetch the CFS instance for the view.
     * We do this lazily to avoid initilization issues.
     *
     * @return The views CFS instance
     */
    public ColumnFamilyStore getViewCfs()
    {
        if (_viewCfs == null)
            _viewCfs = Keyspace.openAndGetStore(Schema.instance.getCFMetaData(baseCfs.keyspace.getName(), name));

        return _viewCfs;
    }


    /**
     * Lookup column definitions in the base table that correspond to the view columns (should be 1:1)
     *
     * Notify caller if all primary keys in the view are ALL primary keys in the base. We do this to simplify
     * tombstone checks.
     *
     * @param columns a list of columns to lookup in the base table
     * @param definitions lists to populate for the base table definitions
     * @return true if all view PKs are also Base PKs
     */
    private boolean resolveAndAddColumns(Iterable<ColumnIdentifier> columns, List<ColumnDefinition>... definitions)
    {
        boolean allArePrimaryKeys = true;
        for (ColumnIdentifier identifier : columns)
        {
            ColumnDefinition cdef = baseCfs.metadata.getColumnDefinition(identifier);
            assert cdef != null : "Could not resolve column " + identifier.toString();

            for (List<ColumnDefinition> list : definitions)
            {
                list.add(cdef);
            }

            allArePrimaryKeys = allArePrimaryKeys && cdef.isPrimaryKeyColumn();
        }

        return allArePrimaryKeys;
    }

    /**
     * This updates the columns stored which are dependent on the base CFMetaData.
     *
     * @return true if the view contains only columns which are part of the base's primary key; false if there is at
     *         least one column which is not.
     */
    public boolean updateDefinition(MaterializedViewDefinition definition)
    {
        List<ColumnDefinition> partitionDefs = new ArrayList<>(definition.partitionColumns.size());
        List<ColumnDefinition> primaryKeyDefs = new ArrayList<>(definition.partitionColumns.size()
                                                                + definition.clusteringColumns.size());
        List<ColumnDefinition> baseComplexColumns = new ArrayList<>();

        // We only add the partition columns to the partitions list, but both partition columns and clustering
        // columns are added to the primary keys list
        boolean partitionAllPrimaryKeyColumns = resolveAndAddColumns(definition.partitionColumns, primaryKeyDefs, partitionDefs);
        boolean clusteringAllPrimaryKeyColumns = resolveAndAddColumns(definition.clusteringColumns, primaryKeyDefs);

        for (ColumnDefinition cdef : baseCfs.metadata.allColumns())
        {
            if (cdef.isComplex())
            {
                baseComplexColumns.add(cdef);
            }
        }

        this.columns = new MVColumns(partitionDefs, primaryKeyDefs, baseComplexColumns);

        return partitionAllPrimaryKeyColumns && clusteringAllPrimaryKeyColumns;
    }

    /**
     * Check to see if the update could possibly modify a view. Cases where the view may be updated are:
     * <ul>
     *     <li>View selects all columns</li>
     *     <li>Update contains any range tombstones</li>
     *     <li>Update touches one of the columns included in the view</li>
     * </ul>
     *
     * If the update contains any range tombstones, there is a possibility that it will not touch a range that is
     * currently included in the view.
     *
     * @return true if {@param partition} modifies a column included in the view
     */
    public boolean updateAffectsView(AbstractBTreePartition partition)
    {
        // If we are including all of the columns, then any update will be included
        if (includeAll)
            return true;

        // If there are range tombstones, tombstones will also need to be generated for the materialized view
        // This requires a query of the base rows and generating tombstones for all of those values
        if (!partition.deletionInfo().isLive())
            return true;

        // Check each row for deletion or update
        for (Row row : partition)
        {
            if (row.hasComplexDeletion())
                return true;
            if (!row.deletion().isLive())
                return true;

            for (ColumnData data : row)
            {
                if (getViewCfs().metadata.getColumnDefinition(data.column().name) != null)
                    return true;
            }
        }

        return false;
    }

    /**
     * Creates the clustering columns for the view based on the specified row and resolver policy
     *
     * @param temporalRow The current row
     * @param resolver The policy to use when selecting versions of cells use
     * @return The clustering object to use for the view
     */
    private Clustering viewClustering(TemporalRow temporalRow, TemporalRow.Resolver resolver)
    {
        CFMetaData viewCfm = getViewCfs().metadata;
        int numViewClustering = viewCfm.clusteringColumns().size();
        CBuilder clustering = CBuilder.create(getViewCfs().getComparator());
        for (int i = 0; i < numViewClustering; i++)
        {
            ColumnDefinition definition = viewCfm.clusteringColumns().get(i);
            clustering.add(temporalRow.clusteringValue(definition, resolver));
        }

        return clustering.build();
    }

    /**
     * @return Mutation containing a range tombstone for a base partition key and TemporalRow.
     */
    private PartitionUpdate createTombstone(TemporalRow temporalRow,
                                            DecoratedKey partitionKey,
                                            DeletionTime deletionTime,
                                            TemporalRow.Resolver resolver,
                                            int nowInSec)
    {
        CFMetaData viewCfm = getViewCfs().metadata;
        Row.Builder builder = BTreeRow.unsortedBuilder(nowInSec);
        builder.newRow(viewClustering(temporalRow, resolver));
        builder.addRowDeletion(deletionTime);
        return PartitionUpdate.singleRowUpdate(viewCfm, partitionKey, builder.build());
    }

    /**
     * @return PartitionUpdate containing a complex tombstone for a TemporalRow, and the collection's column identifier.
     */
    private PartitionUpdate createComplexTombstone(TemporalRow temporalRow,
                                                   DecoratedKey partitionKey,
                                                   ColumnDefinition deletedColumn,
                                                   DeletionTime deletionTime,
                                                   TemporalRow.Resolver resolver,
                                                   int nowInSec)
    {

        CFMetaData viewCfm = getViewCfs().metadata;
        Row.Builder builder = BTreeRow.unsortedBuilder(nowInSec);
        builder.newRow(viewClustering(temporalRow, resolver));
        builder.addComplexDeletion(deletedColumn, deletionTime);
        return PartitionUpdate.singleRowUpdate(viewCfm, partitionKey, builder.build());
    }

    /**
     * @return View's DecoratedKey or null, if one of the view's primary key components has an invalid resolution from
     *         the TemporalRow and its Resolver
     */
    private DecoratedKey viewPartitionKey(TemporalRow temporalRow, TemporalRow.Resolver resolver)
    {
        List<ColumnDefinition> partitionDefs = this.columns.partitionDefs;
        Object[] partitionKey = new Object[partitionDefs.size()];

        for (int i = 0; i < partitionKey.length; i++)
        {
            ByteBuffer value = temporalRow.clusteringValue(partitionDefs.get(i), resolver);

            if (value == null)
                return null;

            partitionKey[i] = value;
        }

        CFMetaData metadata = getViewCfs().metadata;
        return metadata.decorateKey(CFMetaData.serializePartitionKey(metadata
                                                                     .getKeyValidatorAsClusteringComparator()
                                                                     .make(partitionKey)));
    }

    /**
     * @return mutation which contains the tombstone for the referenced TemporalRow, or null if not necessary.
     * TemporalRow's can reference at most one view row; there will be at most one row to be tombstoned, so only one
     * mutation is necessary
     */
    private PartitionUpdate createRangeTombstoneForRow(TemporalRow temporalRow)
    {
        // Primary Key and Clustering columns do not generate tombstones
        if (viewHasAllPrimaryKeys)
            return null;

        boolean hasUpdate = false;
        List<ColumnDefinition> primaryKeyDefs = this.columns.primaryKeyDefs;
        for (ColumnDefinition viewPartitionKeys : primaryKeyDefs)
        {
            if (!viewPartitionKeys.isPrimaryKeyColumn() && temporalRow.clusteringValue(viewPartitionKeys, TemporalRow.oldValueIfUpdated) != null)
                hasUpdate = true;
        }

        if (!hasUpdate)
            return null;

        TemporalRow.Resolver resolver = TemporalRow.earliest;
        return createTombstone(temporalRow,
                               viewPartitionKey(temporalRow, resolver),
                               new DeletionTime(temporalRow.viewClusteringTimestamp(), temporalRow.nowInSec),
                               resolver,
                               temporalRow.nowInSec);
    }

    /**
     * @return Mutation which is the transformed base table mutation for the materialized view.
     */
    private PartitionUpdate createUpdatesForInserts(TemporalRow temporalRow)
    {
        TemporalRow.Resolver resolver = TemporalRow.latest;

        DecoratedKey partitionKey = viewPartitionKey(temporalRow, resolver);
        ColumnFamilyStore viewCfs = getViewCfs();

        if (partitionKey == null)
        {
            // Not having a partition key means we aren't updating anything
            return null;
        }

        Row.Builder regularBuilder = BTreeRow.unsortedBuilder(temporalRow.nowInSec);

        CBuilder clustering = CBuilder.create(viewCfs.getComparator());
        for (int i = 0; i < viewCfs.metadata.clusteringColumns().size(); i++)
        {
            clustering.add(temporalRow.clusteringValue(viewCfs.metadata.clusteringColumns().get(i), resolver));
        }
        regularBuilder.newRow(clustering.build());
        regularBuilder.addPrimaryKeyLivenessInfo(LivenessInfo.create(viewCfs.metadata,
                                                                     temporalRow.viewClusteringTimestamp(),
                                                                     temporalRow.viewClusteringTtl(),
                                                                     temporalRow.viewClusteringLocalDeletionTime()));

        for (ColumnDefinition columnDefinition : viewCfs.metadata.allColumns())
        {
            if (columnDefinition.isPrimaryKeyColumn())
                continue;

            for (Cell cell : temporalRow.values(columnDefinition, resolver))
            {
                regularBuilder.addCell(cell);
            }
        }

        return PartitionUpdate.singleRowUpdate(viewCfs.metadata, partitionKey, regularBuilder.build());
    }

    /**
     * @param partition Update which possibly contains deletion info for which to generate view tombstones.
     * @return    View Tombstones which delete all of the rows which have been removed from the base table with
     *            {@param partition}
     */
    private Collection<Mutation> createForDeletionInfo(TemporalRow.Set rowSet, AbstractBTreePartition partition)
    {
        final TemporalRow.Resolver resolver = TemporalRow.earliest;

        DeletionInfo deletionInfo = partition.deletionInfo();

        List<Mutation> mutations = new ArrayList<>();

        // Check the complex columns to see if there are any which may have tombstones we need to create for the view
        if (!columns.baseComplexColumns.isEmpty())
        {
            for (Row row : partition)
            {
                if (!row.hasComplexDeletion())
                    continue;

                TemporalRow temporalRow = rowSet.getClustering(row.clustering());

                assert temporalRow != null;

                for (ColumnDefinition definition : columns.baseComplexColumns)
                {
                    ComplexColumnData columnData = row.getComplexColumnData(definition);

                    if (columnData != null)
                    {
                        DeletionTime time = columnData.complexDeletion();
                        if (!time.isLive())
                        {
                            DecoratedKey targetKey = viewPartitionKey(temporalRow, resolver);
                            if (targetKey != null)
                                mutations.add(new Mutation(createComplexTombstone(temporalRow, targetKey, definition, time, resolver, temporalRow.nowInSec)));
                        }
                    }
                }
            }
        }

        ReadCommand command = null;

        if (!deletionInfo.isLive())
        {
            // We have to generate tombstones for all of the affected rows, but we don't have the information in order
            // to create them. This requires that we perform a read for the entire range that is being tombstoned, and
            // generate a tombstone for each. This may be slow, because a single range tombstone can cover up to an
            // entire partition of data which is not distributed on a single partition node.
            DecoratedKey dk = rowSet.dk;

            if (deletionInfo.hasRanges())
            {
                SinglePartitionSliceBuilder builder = new SinglePartitionSliceBuilder(baseCfs, dk);
                Iterator<RangeTombstone> tombstones = deletionInfo.rangeIterator(false);
                while (tombstones.hasNext())
                {
                    RangeTombstone tombstone = tombstones.next();

                    builder.addSlice(tombstone.deletedSlice());
                }

                command = builder.build();
            }
            else
            {
                command = SinglePartitionReadCommand.fullPartitionRead(baseCfs.metadata, rowSet.nowInSec, dk);
            }
        }

        if (command == null)
        {
            SinglePartitionSliceBuilder builder = null;
            for (Row row : partition)
            {
                if (!row.deletion().isLive())
                {
                    if (builder == null)
                        builder = new SinglePartitionSliceBuilder(baseCfs, rowSet.dk);
                    builder.addSlice(Slice.make(row.clustering()));
                }
            }

            if (builder != null)
                command = builder.build();
        }

        if (command != null)
        {

            //We may have already done this work for
            //another MV update so check

            if (!rowSet.hasTombstonedExisting())
            {
                QueryPager pager = command.getPager(null);

                // Add all of the rows which were recovered from the query to the row set
                while (!pager.isExhausted())
                {
                    try (ReadOrderGroup orderGroup = pager.startOrderGroup();
                         PartitionIterator iter = pager.fetchPageInternal(128, orderGroup))
                    {
                        if (!iter.hasNext())
                            break;

                        try (RowIterator rowIterator = iter.next())
                        {
                            while (rowIterator.hasNext())
                            {
                                Row row = rowIterator.next();
                                rowSet.addRow(row, false);
                            }
                        }
                    }
                }

                //Incase we fetched nothing, avoid re checking on another MV update
                rowSet.setTombstonedExisting();
            }

            // If the temporal row has been deleted by the deletion info, we generate the corresponding range tombstone
            // for the view.
            for (TemporalRow temporalRow : rowSet)
            {
                DeletionTime deletionTime = temporalRow.deletionTime(partition);
                if (!deletionTime.isLive())
                {
                    DecoratedKey value = viewPartitionKey(temporalRow, resolver);
                    if (value != null)
                    {
                        PartitionUpdate update = createTombstone(temporalRow, value, deletionTime, resolver, temporalRow.nowInSec);
                        if (update != null)
                            mutations.add(new Mutation(update));
                    }
                }
            }
        }

        return !mutations.isEmpty() ? mutations : null;
    }

    /**
     * Read and update temporal rows in the set which have corresponding values stored on the local node
     */
    private void readLocalRows(TemporalRow.Set rowSet)
    {
        SinglePartitionSliceBuilder builder = new SinglePartitionSliceBuilder(baseCfs, rowSet.dk);

        for (TemporalRow temporalRow : rowSet)
            builder.addSlice(temporalRow.baseSlice());

        QueryPager pager = builder.build().getPager(null);

        while (!pager.isExhausted())
        {
            try (ReadOrderGroup orderGroup = pager.startOrderGroup();
                 PartitionIterator iter = pager.fetchPageInternal(128, orderGroup))
            {
                while (iter.hasNext())
                {
                    try (RowIterator rows = iter.next())
                    {
                        while (rows.hasNext())
                        {
                            rowSet.addRow(rows.next(), false);
                        }
                    }
                }
            }
        }
    }

    /**
     * @return Set of rows which are contained in the partition update {@param partition}
     */
    private TemporalRow.Set separateRows(AbstractBTreePartition partition, Set<ColumnIdentifier> viewPrimaryKeyCols)
    {

        TemporalRow.Set rowSet = new TemporalRow.Set(baseCfs, viewPrimaryKeyCols, partition.partitionKey().getKey());

        for (Row row : partition)
            rowSet.addRow(row, true);

        return rowSet;
    }

    /**
     * Splits the partition update up and adds the existing state to each row.
     * This data can be reused for multiple MV updates on the same base table
     *
     * @param partition the mutation
     * @param isBuilding If the view is currently being built, we do not query the values which are already stored,
     *                   since all of the update will already be present in the base table.
     * @return The set of temoral rows contained in this update
     */
    public TemporalRow.Set getTemporalRowSet(AbstractBTreePartition partition, TemporalRow.Set existing, boolean isBuilding)
    {
        if (!updateAffectsView(partition))
            return null;

        Set<ColumnIdentifier> columns = new HashSet<>(this.columns.primaryKeyDefs.size());
        for (ColumnDefinition def : this.columns.primaryKeyDefs)
            columns.add(def.name);

        TemporalRow.Set rowSet = null;
        if (existing == null)
        {
            rowSet = separateRows(partition, columns);

            // If we are building the view, we do not want to add old values; they will always be the same
            if (!isBuilding)
                readLocalRows(rowSet);
        }
        else
        {
            rowSet = existing.withNewViewPrimaryKey(columns);
        }

        return rowSet;
    }


    /**
     * @param isBuilding If the view is currently being built, we do not query the values which are already stored,
     *                   since all of the update will already be present in the base table.
     * @return View mutations which represent the changes necessary as long as previously created mutations for the view
     *         have been applied successfully. This is based solely on the changes that are necessary given the current
     *         state of the base table and the newly applying partition data.
     */
    public Collection<Mutation> createMutations(AbstractBTreePartition partition, TemporalRow.Set rowSet, boolean isBuilding)
    {
        if (!updateAffectsView(partition))
            return null;

        Collection<Mutation> mutations = null;
        for (TemporalRow temporalRow : rowSet)
        {
            // If we are building, there is no need to check for partition tombstones; those values will not be present
            // in the partition data
            if (!isBuilding)
            {
                PartitionUpdate partitionTombstone = createRangeTombstoneForRow(temporalRow);
                if (partitionTombstone != null)
                {
                    if (mutations == null) mutations = new LinkedList<>();
                    mutations.add(new Mutation(partitionTombstone));
                }
            }

            PartitionUpdate insert = createUpdatesForInserts(temporalRow);
            if (insert != null)
            {
                if (mutations == null) mutations = new LinkedList<>();
                mutations.add(new Mutation(insert));
            }
        }

        if (!isBuilding)
        {
            Collection<Mutation> deletion = createForDeletionInfo(rowSet, partition);
            if (deletion != null && !deletion.isEmpty())
            {
                if (mutations == null) mutations = new LinkedList<>();
                mutations.addAll(deletion);
            }
        }

        return mutations;
    }

    public synchronized void build()
    {
        if (this.builder != null)
        {
            this.builder.stop();
            this.builder = null;
        }

        this.builder = new MaterializedViewBuilder(baseCfs, this);
        CompactionManager.instance.submitMaterializedViewBuilder(builder);
    }

    @Nullable
    public static CFMetaData findBaseTable(String keyspace, String view)
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspace);
        if (ksm == null)
            return null;

        for (CFMetaData cfm : ksm.tables)
            if (cfm.getMaterializedViews().get(view).isPresent())
                return cfm;

        return null;
    }

    /**
     * @return CFMetaData which represents the definition given
     */
    public static CFMetaData getCFMetaData(MaterializedViewDefinition definition,
                                           CFMetaData baseCf,
                                           CFProperties properties)
    {
        CFMetaData.Builder viewBuilder = CFMetaData.Builder
                                         .createView(baseCf.ksName, definition.viewName);

        ColumnDefinition nonPkTarget = null;

        for (ColumnIdentifier targetIdentifier : definition.partitionColumns)
        {
            ColumnDefinition target = baseCf.getColumnDefinition(targetIdentifier);
            if (!target.isPartitionKey())
                nonPkTarget = target;

            viewBuilder.addPartitionKey(target.name, properties.getReversableType(targetIdentifier, target.type));
        }

        Collection<ColumnDefinition> included = new ArrayList<>();
        for(ColumnIdentifier identifier : definition.included)
        {
            ColumnDefinition cfDef = baseCf.getColumnDefinition(identifier);
            assert cfDef != null;
            included.add(cfDef);
        }

        boolean includeAll = included.isEmpty();

        for (ColumnIdentifier ident : definition.clusteringColumns)
        {
            ColumnDefinition column = baseCf.getColumnDefinition(ident);
            viewBuilder.addClusteringColumn(ident, properties.getReversableType(ident, column.type));
        }

        for (ColumnDefinition column : baseCf.partitionColumns().regulars)
        {
            if (column != nonPkTarget && (includeAll || included.contains(column)))
            {
                viewBuilder.addRegularColumn(column.name, column.type);
            }
        }

        //Add any extra clustering columns
        for (ColumnDefinition column : Iterables.concat(baseCf.partitionKeyColumns(), baseCf.clusteringColumns()))
        {
            if ( (!definition.partitionColumns.contains(column.name) && !definition.clusteringColumns.contains(column.name)) &&
                 (includeAll || included.contains(column)) )
            {
                viewBuilder.addRegularColumn(column.name, column.type);
            }
        }

        return viewBuilder.build().params(properties.properties.asNewTableParams());
    }
}
