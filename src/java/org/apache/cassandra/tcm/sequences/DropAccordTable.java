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

package org.apache.cassandra.tcm.sequences;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.FinishDropAccordTable;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.tcm.Transformation.Kind.FINISH_DROP_ACCORD_TABLE;
import static org.apache.cassandra.tcm.sequences.SequenceState.continuable;
import static org.apache.cassandra.tcm.sequences.SequenceState.error;
import static org.apache.cassandra.tcm.sequences.SequenceState.halted;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * A slightly atypical implementation as it consists of only a single step. To perform the drop of an
 * Accord table, we first commit a PrepareDropAccordTable transformation. Upon enactement, that
 * marks the table as pending drop, which blocks any new transactions from being started. It also
 * instantiates an instance of this operation and adds it to the set of in progress operations.
 *
 * The intention is to introduce a barrier which blocks until the Accord service acknowledges that
 * it was learned of the epoch in which the table was marked for deletion and that all prior transactions
 * are completed. Once this is complete, we can proceed to actually drop the table. The transformation
 * which performs that schema modification also removes this MSO from ClusterMetadata's in-flight set.
 * This obviates the need to 'advance' this MSO in the way that other implementations with more steps do.
 *
 */
public class DropAccordTable extends MultiStepOperation<Epoch>
{
    private static final Logger logger = LoggerFactory.getLogger(DropAccordTable.class);

    public static final Serializer serializer = new Serializer();

    public final TableReference table;

    public static DropAccordTable newSequence(TableReference table, Epoch preparedAt)
    {
        return new DropAccordTable(table, preparedAt);
    }

    /**
     * Used by factory method for external callers and by the serializer.
     * We don't need to include the serialized FinishDropAccordTable step in the serialization
     * of the MSO itself because they have no parameters other than the table reference and so
     * we can just construct a new one when we execute it
     */
    private DropAccordTable(TableReference table, Epoch latestModification)
    {
        super(0, latestModification);
        this.table = table;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DropAccordTable that = (DropAccordTable) o;
        return latestModification.equals(that.latestModification)
               && table.equals(that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(latestModification, table);
    }

    @Override
    public Kind kind()
    {
        return Kind.DROP_ACCORD_TABLE;
    }

    @Override
    protected SequenceKey sequenceKey()
    {
        return table;
    }

    @Override
    public MetadataSerializer<? extends SequenceKey> keySerializer()
    {
        return TableReference.serializer;
    }

    @Override
    public Transformation.Kind nextStep()
    {
        return FINISH_DROP_ACCORD_TABLE;
    }

    @Override
    public SequenceState executeNext()
    {
        try
        {
            SequenceState failure = awaitSafeFromAccord();
            if (failure != null) return failure;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Exception while waiting for Accord service to notify all table txns are complete", t);
            // this is actually continuable as we can simply retry
            return continuable();
        }
        try
        {
            // Now we're satisfied that all Accord txns have finished for the table,
            // go ahead and actually drop it
            ClusterMetadataService.instance().commit(new FinishDropAccordTable(table));
            return continuable();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Exception committing finish_drop_accord_table. " +
                        "Accord service has acknowledged the operation but table remains present in schema", t);
            return halted();
        }
    }

    private SequenceState awaitSafeFromAccord() throws ExecutionException, InterruptedException
    {
        // make sure that Accord sees the current epoch, which must necessarily follow the
        // one which marked the table as pending drop
        ClusterMetadata metadata = ClusterMetadata.current();
        // just for the sake of paranoia, assert that the table is actually marked as being dropped
        if (!verifyTableMarked(metadata.schema.getKeyspaces()))
            return error(new IllegalStateException(String.format("Table %s is in an invalid state to be dropped", table)));

        long startNanos = nanoTime();
        AccordService.instance().epochReady(metadata.epoch).get();
        long epochEndNanos = nanoTime();

        // As of this writing this logic is based off ExclusiveSyncPoints which is a bit heavy weight for what is needed, this could cause timeouts for clusters that have a lot of data.
        // There are retries baked into this call, but trying to handle timeouts more broadly is put on hold as there is active work to define a EpochSyncPoint that should be far cheaper
        // which would avoid the timeout issues
        // NOTE: ExclusiveSyncPoint must find all keys in the range, then make sure nothing is blocking them... this causes a lot of IO.  EpochSyncPoint just needs to validate that the last txn processed is in the newer epoch, this can work with in-memory state.
        AccordService.instance().awaitDone(table.id, metadata.epoch.getEpoch());
        long awaitEndNanos = nanoTime();
        logger.info("Wait for Accord to see the drop table was success.  " +
                    "Took {} to wait for Accord to learn about the change, then {} to process everything",
                    Duration.ofNanos(epochEndNanos - startNanos), Duration.ofNanos(awaitEndNanos - epochEndNanos));
        return null;
    }

    private boolean verifyTableMarked(Keyspaces keyspaces)
    {
        TableMetadata tm = keyspaces.getTableOrViewNullable(table.id);
        if (tm == null)
        {
            logger.warn("Unable to drop accord table {}, table not found", table);
            return false;
        }

        if (!tm.params.pendingDrop)
        {
            logger.warn("Unexpected state, table {} was not marked pending drop", table);
            return false;
        }

        return true;
    }

    @Override
    public Transformation.Result applyTo(ClusterMetadata metadata)
    {
        // note: that this will apply the finish drop transformation to the supplied metadata. It's
        // not used to actually execute the MSO, but to determine what the metadata state will/would
        // be if it were executed.
        return new FinishDropAccordTable(table).execute(metadata);
    }

    @Override
    public DropAccordTable advance(Epoch epoch)
    {
        // note: this isn't really used by this MSO impl as it consists of a single step so there's nothing
        // to advance. An action of the single step is to remove the MSO from the set of in progress sequences
        return new DropAccordTable(this.table, epoch);
    }

    @Override
    public ProgressBarrier barrier()
    {
        return ProgressBarrier.immediate();
    }

    public static class TableReference implements SequenceKey, Comparable<TableReference>
    {
        public static final Serializer serializer = new Serializer();

        public final TableId id;

        public TableReference(TableId id)
        {
            this.id = id;
        }

        public static TableReference from(TableMetadata metadata)
        {
            return new TableReference(metadata.id);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableReference that = (TableReference) o;
            return id.equals(that.id);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id);
        }

        @Override
        public int compareTo(TableReference o)
        {
            return id.compareTo(o.id);
        }

        @Override
        public String toString()
        {
            return "TableReference{id=" + id + '}';
        }

        public static class Serializer implements MetadataSerializer<TableReference>
        {
            @Override
            public void serialize(TableReference t, DataOutputPlus out, Version version) throws IOException
            {
                t.id.serialize(out);
            }

            @Override
            public TableReference deserialize(DataInputPlus in, Version version) throws IOException
            {
                TableId id = TableId.deserialize(in);
                return new TableReference(id);
            }

            @Override
            public long serializedSize(TableReference t, Version version)
            {
                return t.id.serializedSize();
            }
        }
    }

    public static class Serializer implements AsymmetricMetadataSerializer<MultiStepOperation<?>, DropAccordTable>
    {
        @Override
        public void serialize(MultiStepOperation<?> t, DataOutputPlus out, Version version) throws IOException
        {
            DropAccordTable plan = (DropAccordTable) t;
            Epoch.serializer.serialize(plan.latestModification, out, version);
            // This type of sequence only has a single step so no need to include the index in serde.
            // Similarly, the only parameter to that single step (FinishDropAccordTable) is the table
            // reference, so that's all we really need to include in the serialization.
            TableReference.serializer.serialize(plan.table, out, version);
        }

        @Override
        public DropAccordTable deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            TableReference table = TableReference.serializer.deserialize(in, version);
            return new DropAccordTable(table, lastModified);
        }

        @Override
        public long serializedSize(MultiStepOperation<?> t, Version version)
        {
            DropAccordTable plan = (DropAccordTable) t;
            long size = 0;
            size += Epoch.serializer.serializedSize(plan.latestModification, version);
            size += TableReference.serializer.serializedSize(plan.table, version);
            return size;
        }
    }
}
