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

package org.apache.cassandra.harry.model.reconciler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.util.DescriptorRanges;
import org.apache.cassandra.harry.util.StringUtils;
import org.apache.cassandra.harry.visitors.GeneratingVisitor;
import org.apache.cassandra.harry.visitors.LtsVisitor;
import org.apache.cassandra.harry.visitors.VisitExecutor;
import org.apache.cassandra.harry.gen.DataGenerators;

/**
 * A simple Cassandra-style reconciler for operations against model state.
 * <p>
 * It is useful both as a testing/debugging tool (to avoid starting Cassandra
 * cluster to get a result set), and as a quiescent model checker.
 *
 * TODO: it might be useful to actually record deletions instead of just removing values as we do right now.
 */
public class Reconciler
{
    private static final Logger logger = LoggerFactory.getLogger(Reconciler.class);

    public static long STATIC_CLUSTERING = DataGenerators.NIL_DESCR;

    private final OpSelectors.PdSelector pdSelector;
    private final SchemaSpec schema;

    private final Function<VisitExecutor, LtsVisitor> visitorFactory;

    public Reconciler(Run run)
    {
        this(run,
             (processor) -> new GeneratingVisitor(run, processor));
    }

    public Reconciler(Run run, Function<VisitExecutor, LtsVisitor> ltsVisitorFactory)
    {
        this(run.pdSelector, run.schemaSpec, ltsVisitorFactory);
    }

    public Reconciler(OpSelectors.PdSelector pdSelector, SchemaSpec schema, Function<VisitExecutor, LtsVisitor> ltsVisitorFactory)
    {
        this.pdSelector = pdSelector;
        this.schema = schema;
        this.visitorFactory = ltsVisitorFactory;
    }

    private final long debugCd = -1L;

    public PartitionState inflatePartitionState(final long pd, DataTracker tracker, Query query)
    {
        PartitionState partitionState = new PartitionState(pd, debugCd, schema);

        class Processor extends VisitExecutor
        {
            // Whether a partition deletion was encountered on this LTS.
            private boolean hadPartitionDeletion = false;
            private boolean hadTrackingRowWrite = false;
            private final List<DescriptorRanges.DescriptorRange> rangeDeletes = new ArrayList<>();
            private final List<Operation> writes = new ArrayList<>();
            private final List<Operation> columnDeletes = new ArrayList<>();

            @Override
            protected void operation(Operation operation)
            {
                if (hadPartitionDeletion)
                    return;

                long lts = operation.lts();
                assert pdSelector.pd(operation.lts(), schema) == operation.pd() : String.format("Computed partition descriptor (%d) does for the lts %d. Does not match actual descriptor %d",
                                                                                                pdSelector.pd(operation.lts(), schema),
                                                                                                operation.lts(),
                                                                                                operation.pd());

                if (operation.kind().hasVisibleVisit())
                    partitionState.visitedLts.add(operation.lts());

                if (schema.trackLts)
                    hadTrackingRowWrite = true;

                switch (operation.kind())
                {
                    case DELETE_RANGE:
                    case DELETE_SLICE:
                        DescriptorRanges.DescriptorRange range = ((DeleteOp) operation).relations().toRange(lts);
                        rangeDeletes.add(range);
                        partitionState.delete(range, lts);
                        break;
                    case DELETE_ROW:
                        long cd = ((DeleteRowOp) operation).cd();
                        range = new DescriptorRanges.DescriptorRange(cd, cd, true, true, lts);
                        rangeDeletes.add(range);
                        partitionState.delete(cd, lts);
                        break;
                    case DELETE_PARTITION:
                        partitionState.deletePartition(lts);
                        rangeDeletes.clear();
                        writes.clear();
                        columnDeletes.clear();
                        hadPartitionDeletion = true;
                        break;
                    case INSERT_WITH_STATICS:
                    case INSERT:
                    case UPDATE:
                    case UPDATE_WITH_STATICS:
                        writes.add(operation);
                        break;
                    case DELETE_COLUMN_WITH_STATICS:
                    case DELETE_COLUMN:
                        columnDeletes.add(operation);
                        break;
                    default:
                        throw new IllegalStateException();
                }
            }

            @Override
            protected void beforeLts(long lts, long pd)
            {
                rangeDeletes.clear();
                writes.clear();
                columnDeletes.clear();
                hadPartitionDeletion = false;
            }

            @Override
            protected void afterLts(long lts, long pd)
            {
                if (hadPartitionDeletion)
                    return;

                outer: for (Operation op : writes)
                {
                    WriteOp writeOp = (WriteOp) op;
                    long opId = op.opId();
                    long cd = writeOp.cd();

                    if (hadTrackingRowWrite)
                    {
                        long[] statics = new long[schema.staticColumns.size()];
                        Arrays.fill(statics, DataGenerators.UNSET_DESCR);
                        partitionState.writeStaticRow(statics, lts);
                    }

                    switch (op.kind())
                    {
                        case INSERT_WITH_STATICS:
                        case UPDATE_WITH_STATICS:
                            WriteStaticOp writeStaticOp = (WriteStaticOp) op;
                            // We could apply static columns during the first iteration, but it's more convenient
                            // to reconcile static-level deletions.
                            partitionState.writeStaticRow(writeStaticOp.sds(), lts);
                        case INSERT:
                        case UPDATE:
                            if (!query.matchCd(cd))
                            {
                                if (debugCd != -1 && cd == debugCd)
                                    logger.info("Hiding {} at {}/{} because there was no query match", debugCd, lts, opId);
                                continue outer;
                            }

                            for (DescriptorRanges.DescriptorRange range : rangeDeletes)
                            {
                                if (range.timestamp >= lts && range.contains(cd))
                                {
                                    if (debugCd != -1 && cd == debugCd)
                                        logger.info("Hiding {} at {}/{} because of range tombstone {}", debugCd, lts, opId, range);
                                    continue outer;
                                }
                            }

                            partitionState.write(cd,
                                                 writeOp.vds(),
                                                 lts,
                                                 op.kind() == OpSelectors.OperationKind.INSERT || op.kind() == OpSelectors.OperationKind.INSERT_WITH_STATICS);
                            break;
                        default:
                            throw new IllegalStateException(op.kind().toString());
                    }
                }

                outer: for (Operation op : columnDeletes)
                {
                    DeleteColumnsOp deleteColumnsOp = (DeleteColumnsOp) op;
                    long opId = op.opId();
                    long cd = deleteColumnsOp.cd();

                    switch (op.kind())
                    {
                        case DELETE_COLUMN_WITH_STATICS:
                            partitionState.deleteStaticColumns(lts,
                                                               schema.staticColumnsOffset,
                                                               deleteColumnsOp.columns(), // descriptorSelector.columnMask(pd, lts, opId, op.opKind())
                                                               schema.staticColumnsMask());
                        case DELETE_COLUMN:
                            if (!query.matchCd(cd))
                            {
                                if (debugCd != -1 && cd == debugCd)
                                    logger.info("Hiding {} at {}/{} because there was no query match", debugCd, lts, opId);
                                continue outer;
                            }

                            for (DescriptorRanges.DescriptorRange range : rangeDeletes)
                            {
                                if (range.timestamp >= lts && range.contains(cd))
                                {
                                    if (debugCd != -1 && cd == debugCd)
                                        logger.info("Hiding {} at {}/{} because of range tombstone {}", debugCd, lts, opId, range);
                                    continue outer;
                                }
                            }

                            partitionState.deleteRegularColumns(lts,
                                                                cd,
                                                                schema.regularColumnsOffset,
                                                                deleteColumnsOp.columns(),
                                                                schema.regularColumnsMask());
                            break;
                    }
                }
            }

            @Override
            public void shutdown() throws InterruptedException {}
        }

        LtsVisitor visitor = visitorFactory.apply(new Processor());

        long currentLts = pdSelector.minLtsFor(pd);
        long maxStarted = tracker.maxStarted();
        while (currentLts <= maxStarted && currentLts >= 0)
        {
            if (tracker.isFinished(currentLts))
            {
                visitor.visit(currentLts);
            }
            else
            {
                partitionState.skippedLts.add(currentLts);
            }

            currentLts = pdSelector.nextLts(currentLts);
        }

        return partitionState;
    }

    public static long[] arr(int length, long fill)
    {
        long[] arr = new long[length];
        Arrays.fill(arr, fill);
        return arr;
    }

    public static class RowState
    {
        public boolean hasPrimaryKeyLivenessInfo = false;

        public final PartitionState partitionState;
        public final long cd;
        public final long[] vds;
        public final long[] lts;

        public RowState(PartitionState partitionState,
                        long cd,
                        long[] vds,
                        long[] lts)
        {
            this.partitionState = partitionState;
            this.cd = cd;
            this.vds = vds;
            this.lts = lts;
        }

        public RowState clone()
        {
            RowState rowState = new RowState(partitionState, cd, Arrays.copyOf(vds, vds.length), Arrays.copyOf(lts, lts.length));
            rowState.hasPrimaryKeyLivenessInfo = hasPrimaryKeyLivenessInfo;
            return rowState;
        }

        public String toString()
        {
            return toString(null);
        }

        public String toString(SchemaSpec schema)
        {
            if (cd == STATIC_CLUSTERING)
            {
                return " rowStateRow("
                       + partitionState.pd +
                       "L, " + cd + "L" +
                       ", statics(" + StringUtils.toString(partitionState.staticRow.vds) + ")" +
                       ", lts(" + StringUtils.toString(partitionState.staticRow.lts) + ")";
            }
            else
            {
                return " rowStateRow("
                       + partitionState.pd +
                       "L, " + cd +
                       (partitionState.staticRow == null ? "" : ", statics(" + StringUtils.toString(partitionState.staticRow.vds) + ")") +
                       (partitionState.staticRow == null ? "" : ", lts(" + StringUtils.toString(partitionState.staticRow.lts) + ")") +
                       ", values(" + StringUtils.toString(vds) + ")" +
                       ", lts(" + StringUtils.toString(lts) + ")" +
                       (schema == null ? "" : ", clustering=" + Arrays.toString(schema.inflateClusteringKey(cd))) +
                       (schema == null ? "" : ", values=" + Arrays.toString(schema.inflateRegularColumns(vds))) +
                       ")";
            }
        }
    }
}