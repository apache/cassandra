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

package org.apache.cassandra.harry.model;

import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.harry.core.Configuration;
import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.Bytes;
import org.apache.cassandra.harry.gen.rng.PCGFastPure;
import org.apache.cassandra.harry.gen.rng.RngUtils;
import org.apache.cassandra.harry.gen.Surjections;
import org.apache.cassandra.harry.gen.distribution.Distribution;
import org.apache.cassandra.harry.runner.EarlyExitException;
import org.apache.cassandra.harry.util.BitSet;

import static org.apache.cassandra.harry.gen.DataGenerators.NIL_DESCR;
import static org.apache.cassandra.harry.gen.DataGenerators.UNSET_DESCR;

/**
 * Row (deflated) data selectors. Not calling them generators, since their output is entirely
 * deterministic, and for each input they are able to produce a single output.
 * <p>
 * This is more or less a direct translation of the formalization.
 * <p>
 * All functions implemented by this interface have to _always_ produce same outputs for same inputs.
 * Most of the functions, with the exception of real-time clock translations, should be pure.
 * <p>
 * Functions that are reverse of their counterparts are prefixed with "un"
 */
public interface OpSelectors
{
    interface PureRng
    {
        long randomNumber(long i, long stream);

        long sequenceNumber(long r, long stream);

        default long next(long r)
        {
            return next(r, 0);
        }

        long next(long r, long stream);

        long prev(long r, long stream);

        default long prev(long r)
        {
            return next(r, 0);
        }
    }

    /**
     * Clock is a component responsible for mapping _logical_ timestamps to _real-time_ ones.
     * When reproducing test failures, and for validation purposes, a snapshot of such clock can
     * be taken to map a real-time timestamp from the value retrieved from the database in order
     * to map it back to the logical timestamp of the operation that wrote this value.
     */
    interface Clock
    {
        /**
         * Returns a _real-time_ time timestamp given a _logical_ timestamp
         */
        long rts(long lts);
        /**
         * Returns a _logical_ time timestamp given a _real-time_ timestamp
         */
        long lts(long rts);

        /**
         * Returns the next/smallest logical timestamp that has not yet been visited, and
         * moves internal state to indicate that his timestamp has been taken.
         */
        long nextLts();

        /**
         * Same as #nextLts, but does not change internal state
         */
        long peek();

        Configuration.ClockConfiguration toConfig();
    }

    interface ClockFactory
    {
        Clock make();
    }

    /**
     * *Partition descriptor selector* controls how partitions is selected based on the current logical
     * timestamp. Default implementation is a sliding window of partition descriptors that will visit
     * one partition after the other in the window `slide_after_repeats` times. After that will
     * retire one partition descriptor, and pick one instead of it.
     */
    abstract class PdSelector
    {
        @VisibleForTesting
        protected abstract long pd(long lts);

        public long pd(long lts, SchemaSpec schema)
        {
            return schema.adjustPdEntropy(pd(lts));
        }

        // previous and next LTS with that will yield same pd
        public abstract long nextLts(long lts);

        public abstract long prevLts(long lts);

        public abstract long maxLtsFor(long pd);
        public abstract long minLtsAt(long position);

        public abstract long minLtsFor(long pd);
        public abstract long maxPosition(long maxLts);
    }

    interface PdSelectorFactory
    {
        PdSelector make(PureRng rng);
    }

    interface DescriptorSelectorFactory
    {
        DescriptorSelector make(PureRng rng, SchemaSpec schemaSpec);
    }

    /**
     * DescriptorSelector controls how clustering descriptors are picked within the partition:
     * how many rows there can be in a partition, how many rows will be visited for a logical timestamp,
     * how many operations there will be in batch, what kind of operations there will and how often
     * each kind of operation is going to occur.
     */
    abstract class DescriptorSelector
    {
        public abstract int operationsPerLts(long lts);

        public abstract int maxPartitionSize();

        public abstract boolean isCdVisitedBy(long pd, long lts, long cd);

        // clustering descriptor is calculated using operation id and not modification id, since
        // value descriptors are calculated using modification ids.
        public long cd(long pd, long lts, long opId, SchemaSpec schema)
        {
            return schema.adjustCdEntropy(cd(pd, lts, opId));
        }

        /**
         * Currently, we do not allow visiting the same row more than once per lts, which means that:
         * <p>
         * * `max(opId)` returned `cds` have to be unique for any `lts/pd` pair
         * * {@code max(opId) < maxPartitionSize}
         */
        @VisibleForTesting
        protected abstract long cd(long pd, long lts, long opId);

        public long randomCd(long pd, long entropy, SchemaSpec schema)
        {
            return schema.adjustCdEntropy(randomCd(pd, entropy));
        }

        public abstract long randomCd(long pd, long entropy);

        @VisibleForTesting
        protected abstract long vd(long pd, long cd, long lts, long opId, int col);

        public long[] vds(long pd, long cd, long lts, long opId, OperationKind opType, SchemaSpec schema)
        {
            BitSet setColumns = columnMask(pd, lts, opId, opType);
            return descriptors(pd, cd, lts, opId, schema.regularColumns, schema.regularColumnsMask(), setColumns, schema.regularColumnsOffset);
        }

        public long[] sds(long pd, long cd, long lts, long opId, OperationKind opType, SchemaSpec schema)
        {
            BitSet setColumns = columnMask(pd, lts, opId, opType);
            return descriptors(pd, cd, lts, opId, schema.staticColumns, schema.staticColumnsMask(), setColumns, schema.staticColumnsOffset);
        }

        public long[] descriptors(long pd, long cd, long lts, long opId, List<ColumnSpec<?>> columns, BitSet mask, BitSet setColumns, int offset)
        {
            assert opId < operationsPerLts(lts) : String.format("Operation id %d exceeds the maximum expected number of operations per lts %d",
                                                                opId, operationsPerLts(lts));
            long[] descriptors = new long[columns.size()];

            for (int i = 0; i < descriptors.length; i++)
            {
                int col = offset + i;
                if (setColumns.isSet(col, mask))
                {
                    ColumnSpec<?> spec = columns.get(i);
                    long vd = vd(pd, cd, lts, opId, col) & Bytes.bytePatternFor(spec.type.maxSize());
                    assert vd != UNSET_DESCR : "Ambiguous unset descriptor generated for the value";
                    assert vd != NIL_DESCR : "Ambiguous nil descriptor generated for the value";

                    descriptors[i] = vd;
                }
                else
                {
                    descriptors[i] = UNSET_DESCR;
                }
            }
            return descriptors;
        }

        public abstract OperationKind operationType(long pd, long lts, long opId);

        public abstract BitSet columnMask(long pd, long lts, long opId, OperationKind opType);

        public abstract long opId(long pd, long lts, long cd);

    }

    class PCGFast implements PureRng
    {
        private final long seed;

        public PCGFast(long seed)
        {
            this.seed = seed;
        }

        public long randomNumber(long i, long stream)
        {
            return PCGFastPure.shuffle(PCGFastPure.advanceState(seed, i, stream));
        }

        public long sequenceNumber(long r, long stream)
        {
            return PCGFastPure.distance(seed, PCGFastPure.unshuffle(r), stream);
        }

        public long next(long r, long stream)
        {
            return PCGFastPure.next(r, stream);
        }

        public long prev(long r, long stream)
        {
            return PCGFastPure.previous(r, stream);
        }
    }

    /**
     * Generates partition descriptors, based on LTS as if we had a sliding window.
     * <p>
     * Each {@code windowSize * switchAfter} steps, we move the window by one, effectively
     * expiring one partition descriptor, and adding one partition descriptor to the window.
     * <p>
     * For any LTS, we can calculate previous and next LTS on which it will visit the same
     * partition
     */
    class DefaultPdSelector extends OpSelectors.PdSelector
    {
        public final static long PARTITION_DESCRIPTOR_STREAM_ID = 0x706b;

        private final PureRng rng;

        private final long slideAfterRepeats;
        private final long switchAfter;
        private final long windowSize;

        private final long positionOffset;
        private final long positionWindowSize;

        public DefaultPdSelector(PureRng rng, long windowSize, long slideAfterRepeats)
        {
            this(rng, windowSize, slideAfterRepeats, 0L, Long.MAX_VALUE);
        }

        public DefaultPdSelector(PureRng rng, long windowSize, long slideAfterRepeats, long positionOffset, long positionWindowSize)
        {
            this.rng = rng;
            this.slideAfterRepeats = slideAfterRepeats;
            this.windowSize = windowSize;
            this.switchAfter = windowSize * slideAfterRepeats;
            this.positionOffset = positionOffset;
            this.positionWindowSize = positionWindowSize;
        }

        public Iterator<Long> allPds(long maxLts)
        {
            final long maxPosition = maxPosition(maxLts);
            return new Iterator<Long>()
            {
                private long position = 0;

                public boolean hasNext()
                {
                    return position <= maxPosition;
                }

                public Long next()
                {
                    long pd = rng.randomNumber(adjustPosition(position), PARTITION_DESCRIPTOR_STREAM_ID);
                    position++;
                    return pd;
                }
            };
        }

        protected long pd(long lts)
        {
            return rng.randomNumber(adjustPosition(positionFor(lts)), PARTITION_DESCRIPTOR_STREAM_ID);
        }

        public long minLtsAt(long position)
        {
            if (position < windowSize)
                return position;

            long windowStart = (position - (windowSize - 1)) * slideAfterRepeats * windowSize;
            return windowStart + windowSize - 1;
        }

        public long minLtsFor(long pd)
        {
            long position = positionForPd(pd);
            return minLtsAt(position);
        }

        public long randomVisitedPd(long maxLts, long visitLts, SchemaSpec schema)
        {
            long maxPosition = maxPosition(maxLts);
            if (maxPosition == 0)
                return pdAtPosition(0, schema);

            int idx = RngUtils.asInt(rng.randomNumber(visitLts, maxPosition), 0, (int) (maxPosition - 1));
            return pdAtPosition(idx, schema);
        }

        public long maxPosition(long maxLts)
        {
            long timesCycled = maxLts / switchAfter;
            long windowStart = timesCycled * windowSize;

            // We have cycled through _current_ window at least once
            if (maxLts > (windowStart * slideAfterRepeats) + windowSize)
                return windowStart + windowSize;
            return windowStart + maxLts % windowSize;
        }

        public long positionFor(long lts)
        {
            long windowStart = lts / switchAfter;
            return windowStart + lts % windowSize;
        }

        /**
         * We only adjust position right before we go to the PCG RNG to grab a partition id, since we want
         * the fact that PCG is actually offset to be hidden from the user and even the rest of this class.
         */
        private long adjustPosition(long position)
        {
            if (position > positionWindowSize)
                throw new EarlyExitException(String.format("Partition position has wrapped around, so can not be safely used. " +
                                                           "This runner has been given %d partitions, and if we wrap back to " +
                                                           "position 0, partition state is not going to be inflatable, since" +
                                                           "nextLts will not jump to the lts that is about to be visited. " +
                                                           "Increase rows per visit, batch size, or slideAfter repeats.",
                                                           positionWindowSize));
            return positionOffset + position;
        }

        /**
         * When computing position from pd or lts, we need to translate the offset back to the original, since
         * it is not aware (and should not be) of the fact that positions are offset.
         */
        private long unadjustPosition(long position)
        {
            return position - positionOffset;
        }

        public long positionForPd(long pd)
        {
            return unadjustPosition(rng.sequenceNumber(pd, PARTITION_DESCRIPTOR_STREAM_ID));
        }

        public long pdAtPosition(long position, SchemaSpec schemaSpec)
        {
            return schemaSpec.adjustCdEntropy(rng.randomNumber(adjustPosition(position), PARTITION_DESCRIPTOR_STREAM_ID));
        }

        public long nextLts(long lts)
        {
            long slideCount = lts / switchAfter;
            long positionInCycle = lts - slideCount * switchAfter;
            long nextRepeat = positionInCycle / windowSize + 1;

            if (nextRepeat > slideAfterRepeats ||
                (nextRepeat == slideAfterRepeats && (positionInCycle % windowSize) == 0))
                return -1;

            // last cycle before window slides; next window will have shifted by one
            if (nextRepeat == slideAfterRepeats)
                positionInCycle -= 1;

            return slideCount * switchAfter + windowSize + positionInCycle;
        }

        public long prevLts(long lts)
        {
            long slideCount = lts / switchAfter;
            long positionInCycle = lts - slideCount * switchAfter;
            long prevRepeat = positionInCycle / windowSize - 1;

            if (lts < windowSize ||
                prevRepeat < -1 ||
                (prevRepeat == -1 && (positionInCycle % windowSize) == (windowSize - 1)))
                return -1;

            // last cycle before window slides; next window will have shifted by one
            if (prevRepeat == -1)
                positionInCycle += 1;

            return slideCount * switchAfter - windowSize + positionInCycle;
        }

        public long maxLtsFor(long pd)
        {
            long position = positionForPd(pd);
            return position * switchAfter + (slideAfterRepeats - 1) * windowSize;
        }

        public String toString()
        {
            return "DefaultPdSelector{" +
                   "slideAfterRepeats=" + slideAfterRepeats +
                   ", windowSize=" + windowSize +
                   '}';
        }
    }

    static ColumnSelectorBuilder columnSelectorBuilder()
    {
        return new ColumnSelectorBuilder();
    }

    class ColumnSelectorBuilder
    {
        private Map<OperationKind, Surjections.Surjection<BitSet>> m;

        public ColumnSelectorBuilder()
        {
            this.m = new EnumMap<>(OperationKind.class);
        }

        public ColumnSelectorBuilder forAll(SchemaSpec schema)
        {
            return forAll(schema, BitSet.surjection(schema.allColumns.size()));
        }

        // TODO: change bitsets to take into account _all_ columns not only regulars
        public ColumnSelectorBuilder forAll(SchemaSpec schema, Surjections.Surjection<BitSet> orig)
        {
            for (OperationKind type : OperationKind.values())
            {
                Surjections.Surjection<BitSet> gen = orig;

                switch (type)
                {
                    case UPDATE_WITH_STATICS:
                    case DELETE_COLUMN_WITH_STATICS:
                        gen = (descriptor) -> {
                            long counter = 0;
                            while (counter <= 100)
                            {
                                BitSet bitSet = orig.inflate(descriptor);
                                if ((schema.regularColumns.isEmpty() || !bitSet.allUnset(schema.regularColumnsMask))
                                    && (schema.staticColumns.isEmpty() || !bitSet.allUnset(schema.staticColumnsMask)))
                                    return bitSet;

                                descriptor = RngUtils.next(descriptor);
                                counter++;
                            }
                            throw new RuntimeException(String.format("Could not generate a value after %d attempts.", counter));
                        };
                        break;
                    // Can not have an UPDATE statement without anything to update
                    case UPDATE:
                        gen = descriptor -> {
                            long counter = 0;
                            while (counter <= 100)
                            {
                                BitSet bitSet = orig.inflate(descriptor);

                                if (!bitSet.allUnset(schema.regularColumnsMask))
                                    return bitSet;

                                descriptor = RngUtils.next(descriptor);
                                counter++;
                            }
                            throw new RuntimeException(String.format("Could not generate a value after %d attempts.", counter));
                        };
                        break;
                    case DELETE_COLUMN:
                        gen = (descriptor) -> {
                            long counter = 0;
                            while (counter <= 100)
                            {
                                BitSet bitSet = orig.inflate(descriptor);
                                BitSet mask = schema.regularColumnsMask;

                                if (!bitSet.allUnset(mask))
                                    return bitSet;

                                descriptor = RngUtils.next(descriptor);
                                counter++;
                            }
                            throw new RuntimeException(String.format("Could not generate a value after %d attempts.", counter));
                        };
                        break;
                }
                this.m.put(type, gen);
            }
            return this;
        }

        public ColumnSelectorBuilder forWrite(Surjections.Surjection<BitSet> gen)
        {
            m.put(OperationKind.INSERT, gen);
            return this;
        }

        public ColumnSelectorBuilder forWrite(BitSet pickFrom)
        {
            return forWrite(Surjections.pick(pickFrom));
        }

        public ColumnSelectorBuilder forDelete(Surjections.Surjection<BitSet> gen)
        {
            m.put(OperationKind.DELETE_ROW, gen);
            return this;
        }

        public ColumnSelectorBuilder forDelete(BitSet pickFrom)
        {
            return forDelete(Surjections.pick(pickFrom));
        }

        public ColumnSelectorBuilder forColumnDelete(Surjections.Surjection<BitSet> gen)
        {
            m.put(OperationKind.DELETE_COLUMN, gen);
            return this;
        }

        public ColumnSelectorBuilder forColumnDelete(BitSet pickFrom)
        {
            return forColumnDelete(Surjections.pick(pickFrom));
        }

        public ColumnSelector build()
        {
            return (kind, descriptor) -> m.get(kind).inflate(descriptor);
        }
    }


    /**
     * ColumnSelector has to return BitSet specifying _all_ columns
     */
    interface ColumnSelector
    {
        public BitSet columnMask(OperationKind operationKind, long descriptor);
    }

    class HierarchicalDescriptorSelector extends DefaultDescriptorSelector
    {
        private final int[] fractions;

        public HierarchicalDescriptorSelector(PureRng rng,
                                              // how many parts (at most) each subsequent "level" should contain
                                              int[] fractions,
                                              ColumnSelector columnSelector,
                                              OperationSelector operationSelector,
                                              Distribution operationsPerLtsDistribution,
                                              int maxPartitionSize)
        {
            super(rng,
                  columnSelector,
                  operationSelector,
                  operationsPerLtsDistribution,
                  maxPartitionSize);
            this.fractions = fractions;
        }

        @Override
        public long cd(long pd, long lts, long opId, SchemaSpec schema)
        {
            if (schema.clusteringKeys.size() <= 1)
                return schema.adjustCdEntropy(super.cd(pd, lts, opId));

            int partitionSize = maxPartitionSize();
            int clusteringOffset = clusteringOffset(lts);
            long res;
            if (clusteringOffset == 0)
            {
                res = rng.prev(opId, pd);
            }
            else
            {
                int positionInPartition = (int) ((clusteringOffset + opId) % partitionSize);
                res = cd(positionInPartition, fractions, schema, rng, pd);
            }
            return schema.adjustCdEntropy(res);
        }

        @VisibleForTesting
        public static long cd(int positionInPartition, int[] fractions, SchemaSpec schema, PureRng rng, long pd)
        {
            long[] slices = new long[schema.clusteringKeys.size()];
            for (int i = 0; i < slices.length; i++)
            {
                int idx = i < fractions.length ? (positionInPartition % (fractions[i] - 1)) : positionInPartition;
                slices[i] = rng.prev(idx, rng.next(pd, i + 1));
            }

            return schema.ckGenerator.stitch(slices);
        }

        protected long cd(long pd, long lts, long opId)
        {
            throw new RuntimeException("Shouldn't be called");
        }
    }

    class DefaultDescriptorSelector extends DescriptorSelector
    {
        protected final static long OPERATIONS_PER_LTS_STREAM = 0x5e03812e293L;
        protected final static long BITSET_IDX_STREAM = 0x92eb607bef1L;

        public static OperationSelector DEFAULT_OP_SELECTOR = OperationSelector.weighted(Surjections.weights(45, 45, 3, 2, 2, 1, 1, 1),
                                                                                         OperationKind.INSERT,
                                                                                         OperationKind.INSERT_WITH_STATICS,
                                                                                         OperationKind.DELETE_ROW,
                                                                                         OperationKind.DELETE_COLUMN,
                                                                                         OperationKind.DELETE_COLUMN_WITH_STATICS,
                                                                                         OperationKind.DELETE_PARTITION,
                                                                                         OperationKind.DELETE_RANGE,
                                                                                         OperationKind.DELETE_SLICE);

        protected final PureRng rng;
        protected final OperationSelector operationSelector;
        protected final ColumnSelector columnSelector;
        protected final Distribution operationsPerLtsDistribution;
        protected final int maxPartitionSize;

        public DefaultDescriptorSelector(PureRng rng,
                                         ColumnSelector columnMaskSelector,
                                         OperationSelector operationSelector,
                                         Distribution operationsPerLtsDistribution,
                                         int maxPartitionSize)
        {
            this.rng = rng;

            this.operationSelector = operationSelector;
            this.columnSelector = columnMaskSelector;
            this.operationsPerLtsDistribution = operationsPerLtsDistribution;
            this.maxPartitionSize = maxPartitionSize;
        }

        public int operationsPerLts(long lts)
        {
            return (int) operationsPerLtsDistribution.skew(rng.randomNumber(lts, OPERATIONS_PER_LTS_STREAM));
        }

        public int maxPartitionSize()
        {
            return maxPartitionSize;
        }

        protected int clusteringOffset(long lts)
        {
            return RngUtils.asInt(lts, 0, maxPartitionSize() - 1);
        }

        // TODO: this won't work for entropy-adjusted CDs, at least the way they're implemented now
        public boolean isCdVisitedBy(long pd, long lts, long cd)
        {
            return opId(pd, lts, cd) < operationsPerLts(lts);
        }

        public long randomCd(long pd, long entropy)
        {
            long positionInPartition = Math.abs(rng.prev(entropy)) % maxPartitionSize();
            return rng.prev(positionInPartition, pd);
        }

        protected long cd(long pd, long lts, long opId)
        {
            int partitionSize = maxPartitionSize();
            int clusteringOffset = clusteringOffset(lts);
            if (clusteringOffset == 0)
                return rng.prev(opId, pd);

            int positionInPartition = (int) ((clusteringOffset + opId) % partitionSize);
            return rng.prev(positionInPartition, pd);
        }

        public long opId(long pd, long lts, long cd)
        {
            int partitionSize = maxPartitionSize();
            int clusteringOffset = clusteringOffset(lts);
            int positionInPartition = (int) rng.next(cd, pd);

            if (clusteringOffset == 0)
                return positionInPartition;

            if (positionInPartition == 0)
                return partitionSize - clusteringOffset;
            if (positionInPartition == clusteringOffset)
                return 0;
            else if (positionInPartition < clusteringOffset)
                return partitionSize - clusteringOffset + positionInPartition;
            else
                return positionInPartition - clusteringOffset;
        }

        public OperationKind operationType(long pd, long lts, long opId)
        {
            return operationType(pd, lts, opId, partitionLevelOperationsMask(pd, lts));
        }

        public BitSet partitionLevelOperationsMask(long pd, long lts)
        {
            int totalOps = operationsPerLts(lts);
            if (totalOps > 64)
            {
                throw new IllegalArgumentException("RngUtils#randomBits currently supports only up to 64 bits of entropy, so we can not " +
                                                   "split partition and row level operations for more than 64 operations at the moment.");
            }

            long seed = rng.randomNumber(pd, lts);

            int partitionLevelOps = (int) Math.ceil(operationSelector.partitionLevelThreshold * totalOps);
            long partitionLevelOpsMask = RngUtils.randomBits(partitionLevelOps, totalOps, seed);

            return BitSet.create(partitionLevelOpsMask, totalOps);
        }

        private OperationKind operationType(long pd, long lts, long opId, BitSet partitionLevelOperationsMask)
        {
            try
            {
                long descriptor = rng.randomNumber(pd ^ lts ^ opId, BITSET_IDX_STREAM);
                return operationSelector.inflate(descriptor, partitionLevelOperationsMask.isSet((int) opId));
            }
            catch (Throwable t)
            {
                throw new RuntimeException(String.format("Can not generate a random number with the following inputs: " +
                                                         "pd=%d lts=%d opId=%d partitionLevelOperationsMask=%s",
                                                         pd, lts, opId, partitionLevelOperationsMask));
            }
        }

        public BitSet columnMask(long pd, long lts, long opId, OperationKind opType)
        {
            long descriptor = rng.randomNumber(pd ^ lts ^ opId, BITSET_IDX_STREAM);
            return columnSelector.columnMask(opType, descriptor);
        }

        public long vd(long pd, long cd, long lts, long opId, int col)
        {
            return rng.randomNumber(opId + 1, pd ^ cd ^ lts ^ col);
        }
    }

    enum OperationKind
    {
        UPDATE(false),
        INSERT(false),
        UPDATE_WITH_STATICS(true),
        INSERT_WITH_STATICS(true),
        DELETE_PARTITION(true),
        DELETE_ROW(false),
        DELETE_COLUMN(false),
        DELETE_COLUMN_WITH_STATICS(true),
        DELETE_RANGE(false),
        DELETE_SLICE(false);

        public final boolean partititonLevel;

        OperationKind(boolean partitionLevel)
        {
            this.partititonLevel = partitionLevel;
        }

        /**
         * All operations apart from partiton delition, including partition hist are visible since if there is a
         * partition liveness marker, partition's static column is going to survive. We use this method to match
         * computed LTS with tracked ones.
         */
        public boolean hasVisibleVisit()
        {
            return this != OpSelectors.OperationKind.DELETE_PARTITION;
        }
    }

    public static class OperationSelector
    {
        public final Surjections.Surjection<OperationKind> partitionLevelOperationSelector;
        public final Surjections.Surjection<OperationKind> rowLevelOperationSelector;
        // TODO: start using partitionLevelThreshold
        public final double partitionLevelThreshold;

        public OperationSelector(Surjections.Surjection<OperationKind> partitionLevelOperationSelector,
                                 Surjections.Surjection<OperationKind> rowLevelOperationSelector,
                                 double partitionLevelThreshold)
        {
            this.partitionLevelOperationSelector = partitionLevelOperationSelector;
            this.rowLevelOperationSelector = rowLevelOperationSelector;
            this.partitionLevelThreshold = partitionLevelThreshold;
        }

        public OperationKind inflate(long descriptor, boolean partitionLevel)
        {
            OperationKind operationKind = partitionLevel ? partitionLevelOperationSelector.inflate(descriptor) : rowLevelOperationSelector.inflate(descriptor);
            assert operationKind.partititonLevel == partitionLevel : "Generated operation with an incorrect partition level. Check your generators.";
            return operationKind;
        }

        public static OperationSelector weighted(Map<OperationKind, Integer> weightsMap)
        {
            int[] weights = new int[weightsMap.size()];
            OperationKind[] operationKinds = new OperationKind[weightsMap.size()];
            int i = 0;
            for (Map.Entry<OperationKind, Integer> entry : weightsMap.entrySet())
            {
                weights[i] = entry.getValue();
                operationKinds[i] = entry.getKey();
                i++;
            }
            return weighted(Surjections.weights(weights), operationKinds);
        }

        public static OperationSelector weighted(long[] weights, OperationKind... operationKinds)
        {
            assert weights.length == operationKinds.length;

            Map<OperationKind, Integer> partitionLevel = new EnumMap<OperationKind, Integer>(OperationKind.class);
            Map<OperationKind, Integer> rowLevel = new EnumMap<OperationKind, Integer>(OperationKind.class);

            int partitionLevelSum = 0;
            int rowLevelSum = 0;
            for (int i = 0; i < weights.length; i++)
            {
                int v = (int) (weights[i] >> 32);
                if (operationKinds[i].partititonLevel)
                {
                    partitionLevel.put(operationKinds[i], v);
                    partitionLevelSum += v;
                }
                else
                {
                    rowLevel.put(operationKinds[i], v);
                    rowLevelSum += v;
                }
            }
            int sum = (partitionLevelSum + rowLevelSum);

            return new OperationSelector(Surjections.weighted(normalize(partitionLevel)),
                                         Surjections.weighted(normalize(rowLevel)),
                                         (partitionLevelSum * 1.0) / sum);
        }

        public static Map<OperationKind, Integer> normalize(Map<OperationKind, Integer> weights)
        {
            Map<OperationKind, Integer> normalized = new EnumMap<OperationKind, Integer>(OperationKind.class);
            int sum = 0;
            for (Integer value : weights.values())
                sum += value;

            for (OperationKind kind : weights.keySet())
            {
                double dbl = (sum * ((double) weights.get(kind)) / sum);
                normalized.put(kind, (int) Math.round(dbl));
            }

            return normalized;
        }
    }
}
