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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import accord.api.Update;
import accord.primitives.Keys;
import accord.primitives.Txn;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.accord.txn.TxnCondition;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnDataKeyValue;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnReference;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnValidationRejection;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.service.StorageProxy.ConsensusAttemptResult;
import static org.apache.cassandra.service.StorageProxy.ConsensusAttemptResult.RETRY_NEW_PROTOCOL;
import static org.apache.cassandra.service.StorageProxy.ConsensusAttemptResult.casResult;
import static org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind.CAS_READ;
import static org.apache.cassandra.service.accord.txn.TxnData.txnDataName;
import static org.apache.cassandra.service.accord.txn.TxnResult.Kind.retry_new_protocol;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.getTableMetadata;

/**
 * Processed CAS conditions and update on potentially multiple rows of the same partition.
 */
public class CQL3CasRequest
{
    public final TableMetadata metadata;
    public final DecoratedKey key;
    private final RegularAndStaticColumns conditionColumns;
    private final boolean updatesRegularRows;
    private final boolean updatesStaticRow;
    private boolean hasExists; // whether we have an exist or if not exist condition

    // Conditions on the static row. We keep it separate from 'conditions' as most things related to the static row are
    // special cases anyway.
    private RowCondition staticConditions;
    // We index RowCondition by the clustering of the row they applied to for 2 reasons:
    //   1) this allows to keep things sorted to build the read command below
    //   2) this allows to detect when contradictory conditions are set (not exists with some other conditions on the same row)
    private final TreeMap<Clustering<?>, RowCondition> conditions;

    private final List<TxnWrite.Fragment> writeFragments = new ArrayList<>();

    public CQL3CasRequest(TableMetadata metadata,
                          DecoratedKey key,
                          RegularAndStaticColumns conditionColumns,
                          boolean updatesRegularRows,
                          boolean updatesStaticRow)
    {
        this.metadata = metadata;
        this.key = key;
        this.conditions = new TreeMap<>(metadata.comparator);
        this.conditionColumns = conditionColumns;
        this.updatesRegularRows = updatesRegularRows;
        this.updatesStaticRow = updatesStaticRow;
    }

    public Dispatcher.RequestTime requestTime()
    {
        return Dispatcher.RequestTime.forImmediateExecution();
    }


    void addWriteFragment(ModificationStatement stmt, QueryOptions options, ClientState clientState, long nowInSeconds)
    {
        // Create TxnWrite.Fragment directly using existing pattern
        PartitionKey partitionKey = new PartitionKey(metadata.id, key);
        List<TxnWrite.Fragment> fragments = stmt.forTxn().getTxnWriteFragment(
            writeFragments.size(), clientState, options, partitionKey, nowInSeconds);
        writeFragments.addAll(fragments);
    }

    public void addNotExist(Clustering<?> clustering) throws InvalidRequestException
    {
        addExistsCondition(clustering, new NotExistCondition(clustering), true);
    }

    public void addExist(Clustering<?> clustering) throws InvalidRequestException
    {
        addExistsCondition(clustering, new ExistCondition(clustering), false);
    }

    private void addExistsCondition(Clustering<?> clustering, RowCondition condition, boolean isNotExist)
    {
        assert condition instanceof ExistCondition || condition instanceof NotExistCondition;
        RowCondition previous = getConditionsForRow(clustering);
        if (previous != null)
        {
            if (previous.getClass().equals(condition.getClass()))
            {
                // We can get here if a BATCH has 2 different statements on the same row with the same "exist" condition.
                // For instance (assuming 'k' is the full PK):
                //   BEGIN BATCH
                //      INSERT INTO t(k, v1) VALUES (0, 'foo') IF NOT EXISTS;
                //      INSERT INTO t(k, v2) VALUES (0, 'bar') IF NOT EXISTS;
                //   APPLY BATCH;
                // Of course, those can be trivially rewritten by the user as a single INSERT statement, but we still don't
                // want this to be a problem (see #12867 in particular), so we simply return (the condition itself has
                // already be set).
                assert hasExists; // We shouldn't have a previous condition unless hasExists has been set already.
                return;
            }
            else
            {
                // these should be prevented by the parser, but it doesn't hurt to check
                throw (previous instanceof NotExistCondition || previous instanceof ExistCondition)
                    ? new InvalidRequestException("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row")
                    : new InvalidRequestException("Cannot mix IF conditions and IF " + (isNotExist ? "NOT " : "") + "EXISTS for the same row");
            }
        }

        setConditionsForRow(clustering, condition);
        hasExists = true;
    }

    public void addConditions(Clustering<?> clustering, Collection<ColumnCondition> conds, QueryOptions options) throws InvalidRequestException
    {
        RowCondition condition = getConditionsForRow(clustering);
        if (condition == null)
        {
            condition = new ColumnsConditions(clustering);
            setConditionsForRow(clustering, condition);
        }
        else if (!(condition instanceof ColumnsConditions))
        {
            throw new InvalidRequestException("Cannot mix IF conditions and " + ((ToCQL) condition).toCQL() + " for the same row");
        }
        ((ColumnsConditions)condition).addConditions(conds, options);
    }

    private RowCondition getConditionsForRow(Clustering<?> clustering)
    {
        return clustering == Clustering.STATIC_CLUSTERING ? staticConditions : conditions.get(clustering);
    }

    private void setConditionsForRow(Clustering<?> clustering, RowCondition condition)
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

    private RegularAndStaticColumns columnsToRead()
    {
        RegularAndStaticColumns allColumns = metadata.regularAndStaticColumns();

        // If we update static row, we won't have any conditions on regular rows.
        // If we update regular row, we have to fetch all regular rows (which would satisfy column condition) and
        // static rows that take part in column condition.
        // In both cases, we're fetching enough rows to distinguish between "all conditions are nulls" and "row does not exist".
        // We have to do this as we can't rely on row marker for that (see #6623)
        Columns statics = updatesStaticRow ? allColumns.statics : conditionColumns.statics;
        Columns regulars = updatesRegularRows ? allColumns.regulars : conditionColumns.regulars;
        return new RegularAndStaticColumns(statics, regulars);
    }

    /**
     * The command to use to fetch the value to compare for the CAS.
     */
    public SinglePartitionReadCommand readCommand(long nowInSec)
    {
        assert staticConditions != null || !conditions.isEmpty();

        // Fetch all columns, but query only the selected ones
        ColumnFilter columnFilter = ColumnFilter.selection(columnsToRead());

        // With only a static condition, we still want to make the distinction between a non-existing partition and one
        // that exists (has some live data) but has not static content. So we query the first live row of the partition.
        if (conditions.isEmpty())
            return SinglePartitionReadCommand.create(metadata,
                                                     nowInSec,
                                                     columnFilter,
                                                     RowFilter.none(),
                                                     DataLimits.cqlLimits(1),
                                                     key,
                                                     new ClusteringIndexSliceFilter(Slices.ALL, false),
                                                     PotentialTxnConflicts.ALLOW);

        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(conditions.navigableKeySet(), false);
        return SinglePartitionReadCommand.create(metadata, nowInSec, key, columnFilter, filter);
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

    private RegularAndStaticColumns updatedColumns()
    {
        RegularAndStaticColumns.Builder builder = RegularAndStaticColumns.builder();
        for (TxnWrite.Fragment fragment : writeFragments)
        {
            builder.addAll(fragment.baseUpdate.columns());
            if (!fragment.referenceOps.isEmpty())
            {
                // Add columns from reference operations
                fragment.referenceOps.getStatics().forEach(op -> builder.add(op.receiver()));
                fragment.referenceOps.getRegulars().forEach(op -> builder.add(op.receiver()));
            }
        }
        return builder.build();
    }

    /**
     * The updates to perform of a CAS success. The values fetched using the readFilter()
     * are passed as argument.
     */
    public PartitionUpdate makeUpdates(FilteredPartition current, ClientState clientState, Ballot ballot) throws InvalidRequestException
    {
        if (writeFragments.isEmpty())
            return PartitionUpdate.emptyUpdate(metadata, key);

        PartitionUpdate.Builder updateBuilder = new PartitionUpdate.Builder(
            metadata, key, updatedColumns(), writeFragments.size());

        // Create TxnData from read results
        TxnDataKeyValue txnDataValue = new TxnDataKeyValue(current.rowIterator(false));
        TxnData txnData = TxnData.of(txnDataName(CAS_READ), txnDataValue);

        for (TxnWrite.Fragment fragment : writeFragments)
            fragment.completeToBuilder(updateBuilder, txnData, ballot, current, clientState);

        PartitionUpdate partitionUpdate = updateBuilder.build();
        IndexRegistry.obtain(metadata).validate(partitionUpdate, clientState);
        return partitionUpdate;
    }

    private static abstract class RowCondition
    {
        public final Clustering<?> clustering;

        protected RowCondition(Clustering<?> clustering)
        {
            this.clustering = clustering;
        }

        public abstract boolean appliesTo(FilteredPartition current) throws InvalidRequestException;

        public abstract TxnCondition asTxnCondition();
    }

    private interface ToCQL
    {
        String toCQL();
    }

    private static class NotExistCondition extends RowCondition implements ToCQL
    {
        private NotExistCondition(Clustering<?> clustering)
        {
            super(clustering);
        }

        public boolean appliesTo(FilteredPartition current)
        {
            return current.getRow(clustering) == null;
        }

        @Override
        public String toCQL()
        {
            return "IF NOT EXISTS";
        }

        public TxnCondition asTxnCondition()
        {
            TxnReference txnReference = TxnReference.row(txnDataName(CAS_READ));
            return new TxnCondition.Exists(txnReference, TxnCondition.Kind.IS_NULL);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NotExistCondition that = (NotExistCondition) o;
            return Objects.equals(clustering, that.clustering);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(clustering);
        }
    }

    private static class ExistCondition extends RowCondition implements ToCQL
    {
        private ExistCondition(Clustering<?> clustering)
        {
            super(clustering);
        }

        public boolean appliesTo(FilteredPartition current)
        {
            return current.getRow(clustering) != null;
        }

        @Override
        public String toCQL()
        {
            return "IF EXISTS";
        }

        public TxnCondition asTxnCondition()
        {
            TxnReference txnReference = TxnReference.row(txnDataName(CAS_READ));
            return new TxnCondition.Exists(txnReference, TxnCondition.Kind.IS_NOT_NULL);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExistCondition that = (ExistCondition) o;
            return Objects.equals(clustering, that.clustering);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(clustering);
        }
    }

    private static class ColumnsConditions extends RowCondition
    {
        private final Set<ColumnCondition.Bound> conditions = new HashSet<>();

        private ColumnsConditions(Clustering<?> clustering)
        {
            super(clustering);
        }

        public void addConditions(Collection<ColumnCondition> conds, QueryOptions options) throws InvalidRequestException
        {
            for (ColumnCondition condition : conds)
            {
                conditions.add(condition.bind(options));
            }
        }

        public boolean appliesTo(FilteredPartition current) throws InvalidRequestException
        {
            Row row = current.getRow(clustering);
            for (ColumnCondition.Bound condition : conditions)
            {
                if (!condition.appliesTo(row))
                    return false;
            }
            return true;
        }

        @Override
        public TxnCondition asTxnCondition()
        {
            return new TxnCondition.ColumnConditionsAdapter(clustering, conditions);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnsConditions that = (ColumnsConditions) o;
            return Objects.equals(clustering, that.clustering) &&
                   Objects.equals(conditions, that.conditions);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(clustering, conditions);
        }
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CQL3CasRequest that = (CQL3CasRequest) o;
        return updatesRegularRows == that.updatesRegularRows &&
               updatesStaticRow == that.updatesStaticRow &&
               hasExists == that.hasExists &&
               Objects.equals(metadata.id, that.metadata.id) && // Compare table IDs instead of full metadata
               Objects.equals(key, that.key) &&
               Objects.equals(conditionColumns, that.conditionColumns) &&
               Objects.equals(staticConditions, that.staticConditions) &&
               Objects.equals(conditions, that.conditions) &&
               Objects.equals(writeFragments, that.writeFragments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(metadata.id, key, conditionColumns, updatesRegularRows, updatesStaticRow,
                           hasExists, staticConditions, conditions, writeFragments);
    }

    public Txn toAccordTxn(ClusterMetadata cm, ConsistencyLevel consistencyLevel, ConsistencyLevel commitConsistencyLevel, ClientState clientState, long nowInSecs)
    {
        SinglePartitionReadCommand readCommand = readCommand(nowInSecs);
        TableMetadata metadata = getTableMetadata(cm, this.metadata.id);
        TableMetadatas.Complete tables = TableMetadatas.of(metadata);
        TableMetadatasAndKeys tablesAndKeys = new TableMetadatasAndKeys(tables, Keys.of(new PartitionKey(metadata.id, readCommand.partitionKey())));
        Update update = createUpdate(cm, tables, clientState, commitConsistencyLevel);
        // If the write strategy is sending all writes through Accord there is no need to use the supplied consistency
        // level since Accord will manage reading safely
        TableParams tableParams = tables.getMetadata(metadata.id).params;
        consistencyLevel = tableParams.transactionalMode.readCLForMode(tableParams.transactionalMigrationFrom, consistencyLevel, cm, metadata.id, readCommand.partitionKey().getToken());
        TxnRead read = TxnRead.createCasRead(readCommand, consistencyLevel, tablesAndKeys);
        // In a CAS requesting only one key is supported and writes
        // can't be dependent on any data that is read (only conditions)
        // so the only relevant keys are the read key
        return new Txn.InMemory(read.keys(), read, TxnQuery.CONDITION, update, tablesAndKeys);
    }

    private Update createUpdate(ClusterMetadata cm, TableMetadatas.Complete tables, ClientState clientState, ConsistencyLevel commitConsistencyLevel)
    {
        // Potentially ignore commit consistency level if TransactionalMode is full
        // since it is safe to match what non-SERIAL writes do
        TableMetadata tableMetadata = tables.getMetadata(metadata.id);
        TableParams tableParams = tableMetadata.params;
        commitConsistencyLevel = tableParams.transactionalMode.commitCLForMode(tableParams.transactionalMigrationFrom, commitConsistencyLevel, cm, tableMetadata.id, key.getToken());
        // CAS requires using the new txn timestamp to correctly linearize some kinds of updates
        return new TxnUpdate(tables, writeFragments, createCondition(), commitConsistencyLevel, PreserveTimestamp.no);
    }

    private TxnCondition createCondition()
    {
        List<TxnCondition> txnConditions = new ArrayList<>(conditions.size() + (staticConditions == null ? 0 : 1));
        if (staticConditions != null)
        {
            txnConditions.add(staticConditions.asTxnCondition());
        }
        for (RowCondition condition : conditions.values())
            txnConditions.add(condition.asTxnCondition());
        // CAS forbids empty conditions
        checkState(!txnConditions.isEmpty());
        return conditions.size() == 1 ? txnConditions.get(0) : new TxnCondition.BooleanGroup(TxnCondition.Kind.AND, txnConditions);
    }

    public ConsensusAttemptResult toCasResult(TxnResult txnResult)
    {
        if (txnResult.kind() == retry_new_protocol)
            return RETRY_NEW_PROTOCOL;
        TxnValidationRejection.maybeThrow(txnResult);
        TxnData txnData = (TxnData)txnResult;
        TxnDataKeyValue partition = (TxnDataKeyValue)txnData.get(txnDataName(CAS_READ));
        return casResult(partition != null ? partition.rowIterator(false) : null);
    }

    public static final Serializer serializer = new Serializer();
    /**
     * IVersionedSerializer for CQL3CasRequest to enable CAS forwarding between coordinators.
     *
     */
    public static class Serializer implements IVersionedSerializer<CQL3CasRequest>
    {
        private static final int UPDATES_REGULAR_ROWS = 0x01;
        private static final int UPDATES_STATIC_ROW = 0x02;
        private static final int HAS_EXISTS = 0x04;

        private static final byte CONDITION_NULL = 0;
        private static final byte CONDITION_NOT_EXIST = 1;
        private static final byte CONDITION_EXIST = 2;
        private static final byte CONDITION_COLUMNS = 3;

        @Override
        public void serialize(CQL3CasRequest request, DataOutputPlus out, int version) throws IOException
        {
            int flags = (request.updatesRegularRows ? UPDATES_REGULAR_ROWS : 0)
                      | (request.updatesStaticRow   ? UPDATES_STATIC_ROW : 0)
                      | (request.hasExists          ? HAS_EXISTS : 0)
                      ;
            out.write(flags);

            request.metadata.id.serializeCompact(out);
            DecoratedKey.serializer.serialize(request.key, out, version);

            Columns.serializer.serialize(request.conditionColumns.statics, out);
            Columns.serializer.serialize(request.conditionColumns.regulars, out);

            serializeRowCondition(request.staticConditions, out, version);

            out.writeUnsignedVInt32(request.conditions.size());
            for (Map.Entry<Clustering<?>, RowCondition> entry : request.conditions.entrySet())
            {
                Clustering.serializer.serialize(entry.getKey(), out, version, request.metadata.comparator.subtypes());
                serializeRowCondition(entry.getValue(), out, version);
            }

            out.writeUnsignedVInt32(request.writeFragments.size());
            TableMetadatas tableMetadatas = TableMetadatas.of(request.metadata);
            for (TxnWrite.Fragment fragment : request.writeFragments)
                TxnWrite.Fragment.serializer.serialize(fragment, tableMetadatas, out, Version.findBestMatchForMessagingVersion(version));
        }

        @Override
        public CQL3CasRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            int flags = in.readUnsignedByte();
            boolean updatesRegularRows = (flags & UPDATES_REGULAR_ROWS) != 0;
            boolean updatesStaticRow = (flags & UPDATES_STATIC_ROW) != 0;
            boolean hasExists = (flags & HAS_EXISTS) != 0;

            TableId tableId = TableId.deserializeCompact(in);
            TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
            if (metadata == null)
                throw new IOException("Unknown table ID in CQL3CasRequest deserialization: " + tableId);

            DecoratedKey key = (DecoratedKey) DecoratedKey.serializer.deserialize(in, version);

            Columns statics = Columns.serializer.deserialize(in, metadata);
            Columns regulars = Columns.serializer.deserialize(in, metadata);
            RegularAndStaticColumns conditionColumns = new RegularAndStaticColumns(statics, regulars);

            CQL3CasRequest request = new CQL3CasRequest(metadata, key, conditionColumns, updatesRegularRows, updatesStaticRow);
            request.hasExists = hasExists;

            request.staticConditions = deserializeRowCondition(in, version, metadata, Clustering.STATIC_CLUSTERING);

            int conditionsCount = in.readUnsignedVInt32();
            for (int i = 0; i < conditionsCount; i++)
            {
                Clustering<?> clustering = Clustering.serializer.deserialize(in, version, metadata.comparator.subtypes());
                RowCondition condition = deserializeRowCondition(in, version, metadata, clustering);
                request.conditions.put(clustering, condition);
            }

            int fragmentCount = in.readUnsignedVInt32();
            TableMetadatas tableMetadatas = TableMetadatas.of(metadata);
            for (int i = 0; i < fragmentCount; i++)
            {
                PartitionKey partitionKey = new PartitionKey(metadata.id, request.key);
                TxnWrite.Fragment fragment = TxnWrite.Fragment.serializer.deserialize(partitionKey, tableMetadatas, in, Version.findBestMatchForMessagingVersion(version));
                request.writeFragments.add(fragment);
            }

            return request;
        }

        @Override
        public long serializedSize(CQL3CasRequest request, int version)
        {
            // Flags byte
            long size = 1;

            size += request.metadata.id.serializedCompactSize();
            size += DecoratedKey.serializer.serializedSize(request.key, version);
            size += Columns.serializer.serializedSize(request.conditionColumns.statics);
            size += Columns.serializer.serializedSize(request.conditionColumns.regulars);
            size += rowConditionSize(request.staticConditions, version);

            size += TypeSizes.sizeofUnsignedVInt(request.conditions.size());
            for (Map.Entry<Clustering<?>, RowCondition> entry : request.conditions.entrySet())
            {
                size += Clustering.serializer.serializedSize(entry.getKey(), version, request.metadata.comparator.subtypes());
                size += rowConditionSize(entry.getValue(), version);
            }

            size += TypeSizes.sizeofUnsignedVInt(request.writeFragments.size());
            for (TxnWrite.Fragment fragment : request.writeFragments)
                size += TxnWrite.Fragment.serializer.serializedSize(fragment, TableMetadatas.of(request.metadata), Version.findBestMatchForMessagingVersion(version));

            return size;
        }

        private void serializeRowCondition(RowCondition condition, DataOutputPlus out, int version) throws IOException
        {
            if (condition == null)
            {
                out.writeByte(CONDITION_NULL);
            }
            else if (condition instanceof NotExistCondition)
            {
                out.writeByte(CONDITION_NOT_EXIST);
                // Don't serialize clustering here - it's already serialized in the conditions map
            }
            else if (condition instanceof ExistCondition)
            {
                out.writeByte(CONDITION_EXIST);
                // Don't serialize clustering here - it's already serialized in the conditions map
            }
            else if (condition instanceof ColumnsConditions)
            {
                out.writeByte(CONDITION_COLUMNS);
                ColumnsConditions cc = (ColumnsConditions) condition;
                out.writeUnsignedVInt32(cc.conditions.size());

                // Serialize each ColumnCondition.Bound using adapted pattern
                for (ColumnCondition.Bound bound : cc.conditions)
                    ColumnCondition.Bound.serializer.serialize(bound, TableMetadatas.of(bound.table), out);
            }
            else
            {
                throw new IOException("Unknown RowCondition type: " + condition.getClass());
            }
        }

        private RowCondition deserializeRowCondition(DataInputPlus in, int version, TableMetadata metadata, Clustering<?> clustering) throws IOException
        {
            byte type = in.readByte();
            switch (type)
            {
                case CONDITION_NULL:
                    return null;
                case CONDITION_NOT_EXIST:
                    return new NotExistCondition(clustering);
                case CONDITION_EXIST:
                    return new ExistCondition(clustering);
                case CONDITION_COLUMNS:
                    int conditionsCount = in.readUnsignedVInt32();
                    ColumnsConditions columnsConditions = new ColumnsConditions(clustering);

                    // Deserialize each ColumnCondition.Bound
                    for (int i = 0; i < conditionsCount; i++)
                    {
                        ColumnCondition.Bound bound = ColumnCondition.Bound.serializer.deserialize(TableMetadatas.of(metadata), in);
                        columnsConditions.conditions.add(bound);
                    }

                    return columnsConditions;
                default:
                    throw new IOException("Unknown RowCondition type: " + type);
            }
        }

        private long rowConditionSize(RowCondition condition, int version)
        {
            long size = 1; // type byte

            if (condition instanceof ColumnsConditions)
            {
                ColumnsConditions cc = (ColumnsConditions) condition;
                size += TypeSizes.sizeofUnsignedVInt(cc.conditions.size());
                // Calculate size for each ColumnCondition.Bound
                for (ColumnCondition.Bound bound : cc.conditions)
                    size += ColumnCondition.Bound.serializer.serializedSize(bound, TableMetadatas.of(bound.table));
            }

            return size;
        }
    }
}
