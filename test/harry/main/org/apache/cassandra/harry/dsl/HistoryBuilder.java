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

package org.apache.cassandra.harry.dsl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.apache.cassandra.harry.clock.ApproximateClock;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.MetricReporter;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.NoOpChecker;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.model.reconciler.Reconciler;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.sut.injvm.QuiescentLocalStateChecker;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.visitors.MutatingRowVisitor;
import org.apache.cassandra.harry.visitors.MutatingVisitor;
import org.apache.cassandra.harry.visitors.ReplayingVisitor;
import org.apache.cassandra.harry.visitors.VisitExecutor;

/**
 * History builder is a component for a simple yet flexible generation of arbitrary data. You can write queries
 * _as if_ you were writing them with primitive values (such as 0,1, and using for loops, and alike).
 *
 * You can create a history builder like:
 *
 *      HistoryBuilder historyBuilder = new HistoryBuilder(seed, maxPartitionSize, 10, schema);
 *
 * The core idea is that you use simple-to-remember numbers as placeholders for your values. For partition key,
 * the value (such as 0,1,2,...) would signify a distinct partition key for a given schema. You can not know in
 * advance the relative order of two generated partition keys (i.e. how they'd sort).
 *
 * For clustering keys, the value 0 signifies the smallest possible-to-generate clustering _for this partition_
 * (i.e. there may be other values that would sort LT relative to it, but they will never be generated in this
 * context. Similarly, `maxPartitionSize - 1` is going to be the largest possible-to-generate clustering _for this
 * partition_). All other values (i.e. between 0 and maxPartitionSize - 1) that will be generated are ordered in
 * the same way as the numbers you used to generate them. This is done for your convenience and being able to create
 * complex/interesting RT queries.
 *
 * You can also go arbitrarily deep into specifying details of your query. For example, calling
 *
 *     historyBuilder.insert();
 *
 * Will generate an INSERT query, according to the given schema, for a random partition, random clustering, with
 * random values. At the same time, calling:
 *
 *     historyBuilder.visitPartition(1).insert();
 *
 * Will generate an insert for a partition whose partition key is under index 1 (generating other writes prefixed
 * with `visitPartition(1)` will ensure operations are executed against the same partition). Clustering and
 * values are still going to be random. Calling:
 *
 *     historyBuilder.visitPartition(1).insert(2);
 *
 * Will generate an insert for a partition whose partition key is under index 1, and the clustering will be third-
 * largest possible clustering for this partition (remember, 0 is smallest, so 0,1,2 - third). Values inserted into
 * this row are still going to be random.
 *
 * Lastly, calling
 *
 *     historyBuilder.visitPartition(1).insert(2, new long[] { 1, 2 });
 *
 * Will generate an insert to 1st partition, 2nd row, and the values are going to be taken from the random
 * streams for the values for corresponding columns.
 *
 * Other possible operations are deleteRow, deleteColumns, deleteRowRange, deleteRowSlide, and deletePartition.
 *
 * HistoryBuilder also allows hardcoding/overriding clustering keys, regular, and static values, but _not_ for
 * partition keys as of now.
 *
 * Since clusterings are ordered according to their value, it is only possible to instruct generator to ensure
 * such value is going to be present. This is done by:
 *
 *     history.forPartition(1).ensureClustering(new Object[]{ "", "b", -1L, "c", "d" });
 *
 * For regular and static columns, overrides are done on the top level, not per-partition, so you can simply do:
 *
 *     history.valueOverrides().override(column.name, 1245, "overriden value");
 *
 *     history.visitPartition(1)
 *            .insert(1, new long[]{ 12345, 12345 });
 *
 *  This will insert "overriden value" for the 1st row of 1st partition, for two columns. In other words, the index
 *  12345 will now be associated with this overriden value. But all other / random values will still be, random.
 */
public class HistoryBuilder implements Iterable<ReplayingVisitor.Visit>, SingleOperationBuilder, BatchOperationBuilder
{
    protected final OverridingCkGenerator ckGenerator;

    protected final SchemaSpec schema;
    protected final TokenPlacementModel.ReplicationFactor rf;

    protected final OpSelectors.PureRng pureRng;
    protected final OpSelectors.DescriptorSelector descriptorSelector;
    protected final ValueHelper valueHelper;
    // TODO: would be great to have a very simple B-Tree here
    protected final Map<Long, ReplayingVisitor.Visit> log;

    // TODO: primitive array with a custom/noncopying growth strat
    protected final Map<Long, PartitionVisitStateImpl> partitionStates = new HashMap<>();
    protected final Set<Long> visitedPartitions = new HashSet<>();

    /**
     * A selector that is going to be used by the model checker.
     */
    protected final PresetPdSelector presetSelector;

    /**
     * Default selector will select every partition exactly once.
     */
    protected final OpSelectors.DefaultPdSelector defaultSelector;
    protected final OffsetClock clock;
    protected final int maxPartitionSize;

    public HistoryBuilder(long seed,
                          int maxPartitionSize,
                          int interleaveWindowSize,
                          SchemaSpec schema,
                          TokenPlacementModel.ReplicationFactor rf)
    {
        this.maxPartitionSize = maxPartitionSize;
        this.log = new HashMap<>();
        this.pureRng = new OpSelectors.PCGFast(seed);

        this.presetSelector = new PresetPdSelector();
        this.ckGenerator = OverridingCkGenerator.make(schema.ckGenerator);
        this.valueHelper = new ValueHelper(schema, pureRng);
        this.schema = schema.withCkGenerator(this.ckGenerator, this.ckGenerator.columns)
                            .withColumns(valueHelper.regularColumns, valueHelper.staticColumns);
        this.rf = rf;

        // TODO: make clock pluggable
        this.clock = new OffsetClock(ApproximateClock.START_VALUE,
                                     interleaveWindowSize,
                                     new JdkRandomEntropySource(seed));

        this.defaultSelector = new OpSelectors.DefaultPdSelector(pureRng, 1, 1);

        this.descriptorSelector = new Configuration.CDSelectorConfigurationBuilder()
                                  .setOperationsPerLtsDistribution(new Configuration.ConstantDistributionConfig(Integer.MAX_VALUE))
                                  .setMaxPartitionSize(maxPartitionSize)
                                  .build()
                                  .make(pureRng, schema);
    }

    public ValueOverrides valueOverrides()
    {
        return valueHelper;
    }

    public SchemaSpec schema()
    {
        return schema;
    }

    public int size()
    {
        return log.size();
    }

    public OpSelectors.Clock clock()
    {
        return clock;
    }

    /**
     * Visited partition descriptors _not_ in the order they were visited
     */
    public List<Long> visitedPds()
    {
        return new ArrayList<>(visitedPartitions);
    }

    @Override
    public Iterator<ReplayingVisitor.Visit> iterator()
    {
        return log.values().iterator();
    }

    protected SingleOperationVisitBuilder singleOpVisitBuilder()
    {
        long visitLts = clock.nextLts();
        return singleOpVisitBuilder(defaultSelector.pd(visitLts, schema), visitLts, (ps) -> {});
    }

    protected SingleOperationVisitBuilder singleOpVisitBuilder(long pd, long lts, Consumer<PartitionVisitState> setupPs)
    {
        PartitionVisitStateImpl partitionState = presetSelector.register(lts, pd, setupPs);
        return new SingleOperationVisitBuilder(partitionState, lts, pureRng, descriptorSelector, schema, valueHelper, (visit) -> {
            visitedPartitions.add(pd);
            log.put(visit.lts, visit);
        });
    }

    @Override
    public HistoryBuilder insert()
    {
        singleOpVisitBuilder().insert();
        return this;
    }

    @Override
    public HistoryBuilder insert(int rowId)
    {
        singleOpVisitBuilder().insert(rowId);
        return this;
    }

    @Override
    public HistoryBuilder insert(int rowId, long[] valueIdxs)
    {
        singleOpVisitBuilder().insert(rowId, valueIdxs);
        return this;
    }

    public SingleOperationBuilder insert(int rowIdx, long[] valueIdxs, long[] sValueIdxs)
    {
        singleOpVisitBuilder().insert(rowIdx, valueIdxs, sValueIdxs);
        return this;
    }

    @Override
    public HistoryBuilder deletePartition()
    {
        singleOpVisitBuilder().deletePartition();
        return this;
    }

    @Override
    public HistoryBuilder deleteRow()
    {
        singleOpVisitBuilder().deleteRow();
        return this;
    }

    @Override
    public HistoryBuilder deleteRow(int rowIdx)
    {
        singleOpVisitBuilder().deleteRow(rowIdx);
        return this;
    }

    @Override
    public HistoryBuilder deleteColumns()
    {
        singleOpVisitBuilder().deleteColumns();
        return this;
    }

    @Override
    public HistoryBuilder deleteRowRange()
    {
        singleOpVisitBuilder().deleteRowRange();
        return this;
    }

    @Override
    public HistoryBuilder deleteRowRange(int lowBoundRowIdx, int highBoundRowIdx, boolean isMinEq, boolean isMaxEq)
    {
        singleOpVisitBuilder().deleteRowRange(lowBoundRowIdx, highBoundRowIdx, isMinEq, isMaxEq);
        return this;
    }

    @Override
    public HistoryBuilder deleteRowSlice()
    {
        singleOpVisitBuilder().deleteRowSlice();
        return this;
    }

    @Override
    public BatchVisitBuilder beginBatch()
    {
        long visitLts = clock.nextLts();
        return batchVisitBuilder(defaultSelector.pd(visitLts, schema), visitLts);
    }

    /**
     * Begin batch for a partition descriptor at a specific index.
     *
     * Imagine all partition descriptors were longs in an array. Index of a descriptor
     * is a sequential number of the descriptor in this imaginary array.
     */
    @Override
    public BatchVisitBuilder beginBatch(long pdIdx)
    {
        long visitLts = clock.nextLts();
        return batchVisitBuilder(presetSelector.pdAtPosition(pdIdx), visitLts);
    }

    protected BatchVisitBuilder batchVisitBuilder(long pd, long lts)
    {
        PartitionVisitStateImpl partitionState = presetSelector.register(lts, pd, (ps) -> {});
        return new BatchVisitBuilder(this, partitionState, lts, pureRng, descriptorSelector, schema, valueHelper, (visit) -> {
            visitedPartitions.add(pd);
            log.put(visit.lts, visit);
        });
    }

    public SingleOperationBuilder visitPartition(long pdIdx)
    {
        long visitLts = clock.nextLts();
        long pd = presetSelector.pdAtPosition(pdIdx);
        return singleOpVisitBuilder(pd, visitLts, (ps) -> {});
    }

    public SingleOperationBuilder visitPartition(long pdIdx, Consumer<PartitionVisitState> setupPs)
    {
        long visitLts = clock.nextLts();
        long pd = presetSelector.pdAtPosition(pdIdx);
        return singleOpVisitBuilder(pd, visitLts, setupPs);
    }

    public PartitionVisitState forPartition(long pdIdx)
    {
        long pd = defaultSelector.pdAtPosition(pdIdx, schema);
        return partitionStates.computeIfAbsent(pd, (pd_) -> makePartitionVisitState(pd));
    }

    private PartitionVisitStateImpl makePartitionVisitState(long pd)
    {
        Long[] possibleCds = new Long[maxPartitionSize];
        for (int cdIdx = 0; cdIdx < possibleCds.length; cdIdx++)
        {
            long cd = descriptorSelector.cd(pd, 0, cdIdx, schema);
            possibleCds[cdIdx] = cd;
        }
        Arrays.sort(possibleCds, Long::compare);

        long[] primitiveArray = new long[maxPartitionSize];
        for (int i = 0; i < possibleCds.length; i++)
            primitiveArray[i] = possibleCds[i];

        // TODO: can we have something more efficient than a tree set here?
        return new PartitionVisitStateImpl(pd, primitiveArray, new TreeSet<>(), schema);
    }

    public PresetPdSelector pdSelector()
    {
        return presetSelector;
    }

    /**
     * This is an adapter HistoryBuilder is using to reproduce state for the reconciler.
     *
     * This class is inherently not thread-safe. The thinking behind this is that you should generate
     * operations in advance, and only after you have generated them, should you start execution.
     * If you would like to generate on the fly, you should use default Harry machinery and pure generators,
     * that walk LTS space without intermediate state. This set of primitives is intended to be used for much
     * smaller scale testing.
     */
    public class PresetPdSelector extends OpSelectors.PdSelector
    {
        // TODO: implement a primitive long map?
        private final Map<Long, Long> ltsToPd = new HashMap<>();

        public PartitionVisitStateImpl register(long lts, long pd, Consumer<PartitionVisitState> setup)
        {
            Long prev = ltsToPd.put(lts, pd);
            if (prev != null)
                throw new IllegalStateException(String.format("LTS %d. Was registered twice, first with %d, and then with %d", lts,  prev, pd));

            PartitionVisitStateImpl partitionState = partitionStates.computeIfAbsent(pd, (pd_) -> {
                PartitionVisitStateImpl partitionVisitState = makePartitionVisitState(pd);
                setup.accept(partitionVisitState);
                return partitionVisitState;
            });
            partitionState.visitedLts.add(lts);
            return partitionState;
        }

        protected long pd(long lts)
        {
            return ltsToPd.get(lts);
        }

        public long nextLts(long lts)
        {
            long pd = pd(lts);
            PartitionVisitStateImpl partitionState = partitionStates.get(pd);
            NavigableSet<Long> visitedLts = partitionState.visitedLts.subSet(lts, false, Long.MAX_VALUE, false);
            if (visitedLts.isEmpty())
                return -1;
            else
                return visitedLts.first();
        }

        public long prevLts(long lts)
        {
            long pd = pd(lts);
            PartitionVisitStateImpl partitionState = partitionStates.get(pd);
            NavigableSet<Long> visitedLts = partitionState.visitedLts.descendingSet().subSet(lts, false, 0L, false);
            if (visitedLts.isEmpty())
                return -1;
            else
                return visitedLts.first();
        }

        public long maxLtsFor(long pd)
        {
            PartitionVisitStateImpl partitionState = partitionStates.get(pd);
            if (partitionState == null)
                return -1;
            return partitionState.visitedLts.last();
        }

        public long minLtsFor(long pd)
        {
            PartitionVisitStateImpl partitionState = partitionStates.get(pd);
            if (partitionState == null)
                return -1;
            return partitionState.visitedLts.first();
        }

        public long pdAtPosition(long pdIdx)
        {
            return defaultSelector.pdAtPosition(pdIdx, schema);
        }

        public long minLtsAt(long position)
        {
            throw new IllegalArgumentException("not implemented");
        }

        public long maxPosition(long maxLts)
        {
            // since, unlike other PdSelectors, this one is not computational, we can answer which position is the largest just
            // by tracking the largest position
            return 0;
        }
    }

    public ReplayingVisitor visitor(DataTracker tracker, SystemUnderTest sut, SystemUnderTest.ConsistencyLevel cl)
    {
        if (schema.trackLts)
        {
            return visitor(new MutatingVisitor.LtsTrackingVisitExecutor(descriptorSelector,
                                                                        tracker,
                                                                        sut,
                                                                        schema,
                                                                        new MutatingRowVisitor(schema, clock, MetricReporter.NO_OP),
                                                                        cl));
        }
        else
        {
            return visitor(new MutatingVisitor.MutatingVisitExecutor(descriptorSelector,
                                                                     tracker,
                                                                     sut,
                                                                     schema,
                                                                     new MutatingRowVisitor(schema, clock, MetricReporter.NO_OP),
                                                                     cl));
        }
    }

    public Model noOpChecker(SystemUnderTest.ConsistencyLevel cl, SystemUnderTest sut)
    {
        return new NoOpChecker(sut, cl);
    }

    public Model quiescentChecker(DataTracker tracker, SystemUnderTest sut)
    {
        // TODO: CL for quiescent checker
        return new QuiescentChecker(clock, sut, tracker, schema,
                                    new Reconciler(presetSelector,
                                                   schema,
                                                   this::visitor));
    }

    public Model quiescentLocalChecker(DataTracker tracker, SystemUnderTest sut)
    {
        return new QuiescentLocalStateChecker(clock, presetSelector, sut, tracker, schema,
                                              new Reconciler(presetSelector,
                                                             schema,
                                                             this::visitor),
                                              rf);
    }

    public void validate(DataTracker tracker, SystemUnderTest sut, int... partitionIdxs)
    {
        validate(quiescentChecker(tracker, sut), partitionIdxs);
    }

    public void validate(Model model, int... partitionIdxs)
    {
        for (int partitionIdx : partitionIdxs)
        {
            long pd = presetSelector.pdAtPosition(partitionIdx);
            if (presetSelector.minLtsFor(pd) < 0)
                continue;
            model.validate(Query.selectAllColumns(schema, pd, false));
            model.validate(Query.selectAllColumns(schema, pd, true));
        }
    }

    public void validateAll(DataTracker tracker, SystemUnderTest sut)
    {
        validateAll(quiescentChecker(tracker, sut));
    }

    public void validateAll(Model model)
    {
        for (Long pd : partitionStates.keySet())
        {
            model.validate(Query.selectAllColumns(schema, pd, false));
            model.validate(Query.selectAllColumns(schema, pd, true));
        }
    }

    public void validateAll(Model model, Function<Long, List<Query>> queries)
    {
        for (Long pd : partitionStates.keySet())
        {
            for (Query query : queries.apply(pd))
                model.validate(query);
        }
    }

    public ReplayingVisitor visitor(VisitExecutor executor)
    {
        LongIterator replay = clock.replayAll();
        return new ReplayingVisitor(executor, replay)
        {
            public Visit getVisit(long lts)
            {
                long idx = lts - clock.base;
                Visit visit = log.get(idx);
                assert visit != null : String.format("Could not find a visit for LTS %d", lts);
                return visit;
            }

            public void replayAll()
            {
                while (replay.hasNext())
                    visit();
            }
        };
    }

    public interface LongIterator extends LongSupplier
    {
        boolean hasNext();
        long getAsLong();
    }

    /**
     * Non-monotonic version of OffsetClock.
     */
    public class OffsetClock implements OpSelectors.Clock
    {
        private long lowerBound;
        private long current;

        private final long base;
        private final long batchSize;
        private final Set<Long> returned;
        private final EntropySource entropySource;

        private final List<Long> visitOrder;

        public OffsetClock(long base, long batchSize, EntropySource entropySource)
        {
            this.lowerBound = base;
            this.base = base;
            this.batchSize = batchSize;
            this.returned = new HashSet<>();
            this.entropySource = entropySource;
            this.visitOrder = new ArrayList<>();
            this.current = computeNext();
        }

        /**
         * Visit Order - related methods
         */
        public LongIterator replayAll()
        {
            return new LongIterator()
            {
                private int visitedUpTo;

                public boolean hasNext()
                {
                    return visitedUpTo < visitOrder.size();
                }

                public long getAsLong()
                {
                    return visitOrder.get(visitedUpTo++);
                }
            };
        }

        private long computeNext()
        {
            if (returned.size() == batchSize)
            {
                returned.clear();
                lowerBound += batchSize;
            }

            long generated = entropySource.nextLong(lowerBound, lowerBound + batchSize);
            while (returned.contains(generated))
                generated = entropySource.nextLong(lowerBound, lowerBound + batchSize);

            returned.add(generated);
            return generated;
        }

        @Override
        public long rts(long lts)
        {
            return base + lts;
        }

        @Override
        public long lts(long rts)
        {
            return rts - base;
        }


        @Override
        public long nextLts()
        {
            long ret = current;
            current = computeNext();
            visitOrder.add(ret);
            return ret;
        }

        public long peek()
        {
            return current;
        }

        public Configuration.ClockConfiguration toConfig()
        {
            throw new RuntimeException("Not implemented");
        }
    }
}
