package org.apache.cassandra.stress.generate;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.stress.generate.values.Generator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;

// a partition is re-used to reduce garbage generation, as is its internal RowIterator
// TODO: we should batch the generation of clustering components so we can bound the time and size necessary to
// generate huge partitions with only a small number of clustering components; i.e. we should generate seeds for batches
// of a single component, and then generate the values within those batches as necessary. this will be difficult with
// generating sorted partitions, and may require generator support (e.g. we may need to support generating prefixes
// that are extended/suffixed to generate each batch, so that we can sort the prefixes)
public abstract class PartitionIterator implements Iterator<Row>
{

    abstract boolean reset(double useChance, double rowPopulationRatio, int targetCount, boolean isWrite, PartitionGenerator.Order order);
    // picks random (inclusive) bounds to iterate, and returns them
    public abstract Pair<Row, Row> resetToBounds(Seed seed, int clusteringComponentDepth);

    PartitionGenerator.Order order;
    long idseed;
    Seed seed;

    final PartitionGenerator generator;
    final SeedManager seedManager;

    // we reuse these objects to save garbage
    final Object[] partitionKey;
    final Row row;

    public static PartitionIterator get(PartitionGenerator generator, SeedManager seedManager)
    {
        if (generator.clusteringComponents.size() > 0)
            return new MultiRowIterator(generator, seedManager);
        else
            return new SingleRowIterator(generator, seedManager);
    }

    private PartitionIterator(PartitionGenerator generator, SeedManager seedManager)
    {
        this.generator = generator;
        this.seedManager = seedManager;
        this.partitionKey = new Object[generator.partitionKey.size()];
        this.row = new Row(partitionKey, new Object[generator.clusteringComponents.size() + generator.valueComponents.size()]);
    }

    void setSeed(Seed seed)
    {
        long idseed = 0;
        for (int i = 0 ; i < partitionKey.length ; i++)
        {
            Generator generator = this.generator.partitionKey.get(i);
            // set the partition key seed based on the current work item we're processing
            generator.setSeed(seed.seed);
            Object key = generator.generate();
            partitionKey[i] = key;
            // then contribute this value to the data seed
            idseed = seed(key, generator.type, idseed);
        }
        this.seed = seed;
        this.idseed = idseed;
    }

    public boolean reset(Seed seed, double useChance, double rowPopulationRatio, boolean isWrite)
    {
        setSeed(seed);
        this.order = generator.order;
        return reset(useChance, rowPopulationRatio, 0, isWrite, PartitionIterator.this.order);
    }

    public boolean reset(Seed seed, int targetCount, double rowPopulationRatio,  boolean isWrite)
    {
        setSeed(seed);
        this.order = generator.order;
        return reset(Double.NaN, rowPopulationRatio,targetCount, isWrite,PartitionIterator.this.order);
    }

    static class SingleRowIterator extends PartitionIterator
    {
        boolean done;
        boolean isWrite;
        double rowPopulationRatio;
        final double totalValueColumns;

        private SingleRowIterator(PartitionGenerator generator, SeedManager seedManager)
        {
            super(generator, seedManager);

            this.totalValueColumns = generator.valueComponents.size();
        }

        public Pair<Row, Row> resetToBounds(Seed seed, int clusteringComponentDepth)
        {
            assert clusteringComponentDepth == 0;
            setSeed(seed);
            reset(1d, 1d, 1, false, PartitionGenerator.Order.SORTED);
            return Pair.create(new Row(partitionKey), new Row(partitionKey));
        }

        boolean reset(double useChance, double rowPopulationRatio, int targetCount, boolean isWrite, PartitionGenerator.Order order)
        {
            done = false;
            this.isWrite = isWrite;
            this.rowPopulationRatio = rowPopulationRatio;
            return true;
        }

        public boolean hasNext()
        {
            return !done;
        }

        public Row next()
        {
            if (done)
                throw new NoSuchElementException();

            double valueColumn = 0.0;
            for (int i = 0 ; i < row.row.length ; i++)
            {
                if (generator.permitNulls(i) && (++valueColumn/totalValueColumns) > rowPopulationRatio)
                {
                    row.row[i] = null;
                }
                else
                {
                    Generator gen = generator.valueComponents.get(i);
                    gen.setSeed(idseed);
                    row.row[i] = gen.generate();
                }
            }
            done = true;
            if (isWrite)
            {
                seedManager.markFirstWrite(seed, true);
                seedManager.markLastWrite(seed, true);
            }
            return row;
        }
    }

    // permits iterating a random subset of the procedurally generated rows in this partition. this is the only mechanism for visiting rows.
    // we maintain a stack of clustering components and their seeds; for each clustering component we visit, we generate all values it takes at that level,
    // and then, using the average (total) number of children it takes we randomly choose whether or not we visit its children;
    // if we do, we generate all possible values the immediate children can take, and repeat the process. So at any one time we are using space proportional
    // to C.N, where N is the average number of values each clustering component takes, as opposed to N^C total values in the partition.
    // TODO : support first/last row, and constraining reads to rows we know are populated
    static class MultiRowIterator extends PartitionIterator
    {
        // the seed used to generate the current values for the clustering components at each depth;
        // used to save recalculating it for each row, so we only need to recalc from prior row.
        final long[] clusteringSeeds = new long[generator.clusteringComponents.size()];
        // the components remaining to be visited for each level of the current stack
        final Deque<Object>[] clusteringComponents = new ArrayDeque[generator.clusteringComponents.size()];

        // probability any single row will be generated in this iteration
        double useChance;
        double rowPopulationRatio;
        final double totalValueColumns;
        // we want our chance of selection to be applied uniformly, so we compound the roll we make at each level
        // so that we know with what chance we reached there, and we adjust our roll at that level by that amount
        final double[] chancemodifier = new double[generator.clusteringComponents.size()];
        final double[] rollmodifier = new double[generator.clusteringComponents.size()];

        // track where in the partition we are, and where we are limited to
        final int[] currentRow = new int[generator.clusteringComponents.size()];
        final int[] lastRow = new int[currentRow.length];
        boolean hasNext, isFirstWrite, isWrite;

        // reusable collections for generating unique and sorted clustering components
        final Set<Object> unique = new HashSet<>();
        final List<Object> tosort = new ArrayList<>();

        MultiRowIterator(PartitionGenerator generator, SeedManager seedManager)
        {
            super(generator, seedManager);
            for (int i = 0 ; i < clusteringComponents.length ; i++)
                clusteringComponents[i] = new ArrayDeque<>();
            rollmodifier[0] = 1f;
            chancemodifier[0] = generator.clusteringDescendantAverages[0];
            this.totalValueColumns = generator.valueComponents.size();
        }

        /**
         * initialise the iterator state
         *
         * if we're a write, the expected behaviour is that the requested
         * batch count is compounded with the seed's visit count to decide
         * how much we should return in one iteration
         *
         * @param useChance uniform chance of visiting any single row (NaN if targetCount provided)
         * @param targetCount number of rows we would like to visit (0 if useChance provided)
         * @param isWrite true if the action requires write semantics
         *
         * @return true if there is data to return, false otherwise
         */
        boolean reset(double useChance, double rowPopulationRatio, int targetCount, boolean isWrite, PartitionGenerator.Order order)
        {
            this.isWrite = isWrite;
            this.rowPopulationRatio = rowPopulationRatio;

            this.order = order;
            // set the seed for the first clustering component
            generator.clusteringComponents.get(0).setSeed(idseed);

            // calculate how many first clustering components we'll generate, and how many total rows this predicts
            int firstComponentCount = (int) generator.clusteringComponents.get(0).clusteringDistribution.next();
            int expectedRowCount;

            int position = seed.position();

            if (isWrite)
                expectedRowCount = firstComponentCount * generator.clusteringDescendantAverages[0];
            else if (position != 0)
                expectedRowCount = setLastRow(position - 1);
            else
                expectedRowCount = setNoLastRow(firstComponentCount);

            if (Double.isNaN(useChance))
                useChance = Math.max(0d, Math.min(1d, targetCount / (double) expectedRowCount));
            setUseChance(useChance);

            while (true)
            {
                // we loop in case we have picked an entirely non-existent range, in which case
                // we will reset the seed's position, then try again (until we exhaust it or find
                // some real range)

                for (Queue<?> q : clusteringComponents)
                    q.clear();
                fill(0);

                if (!isWrite)
                {
                    if (seek(0) != State.SUCCESS)
                        throw new IllegalStateException();
                    return true;
                }

                int count = seed.visits == 1 ? 1 + (int) generator.maxRowCount : Math.max(1, expectedRowCount / seed.visits);
                position = seed.moveForwards(count);
                isFirstWrite = position == 0;
                setLastRow(position + count - 1);

                // seek to our start position
                switch (seek(position))
                {
                    case END_OF_PARTITION:
                        return false;
                    case SUCCESS:
                        return true;
                }
            }
        }

        void setUseChance(double useChance)
        {
            if (this.useChance < 1d)
            {
                // we clear our prior roll-modifiers if the use chance was previously less-than zero
                Arrays.fill(rollmodifier, 1d);
                Arrays.fill(chancemodifier, 1d);
            }
            this.useChance = useChance;
        }

        public Pair<Row, Row> resetToBounds(Seed seed, int clusteringComponentDepth)
        {
            setSeed(seed);
            setUseChance(1d);
            if (clusteringComponentDepth == 0)
            {
                reset(1d, 1d, -1, false, PartitionGenerator.Order.SORTED);
                return Pair.create(new Row(partitionKey), new Row(partitionKey));
            }

            this.order = PartitionGenerator.Order.SORTED;
            assert clusteringComponentDepth <= clusteringComponents.length;
            for (Queue<?> q : clusteringComponents)
                q.clear();

            fill(0);
            Pair<int[], Object[]> bound1 = randomBound(clusteringComponentDepth);
            Pair<int[], Object[]> bound2 = randomBound(clusteringComponentDepth);
            if (compare(bound1.left, bound2.left) > 0) { Pair<int[], Object[]> tmp = bound1; bound1 = bound2; bound2 = tmp;}
            Arrays.fill(lastRow, 0);
            System.arraycopy(bound2.left, 0, lastRow, 0, bound2.left.length);
            Arrays.fill(currentRow, 0);
            System.arraycopy(bound1.left, 0, currentRow, 0, bound1.left.length);
            seekToCurrentRow();
            return Pair.create(new Row(partitionKey, bound1.right), new Row(partitionKey, bound2.right));
        }

        // returns expected row count
        private int setNoLastRow(int firstComponentCount)
        {
            Arrays.fill(lastRow, Integer.MAX_VALUE);
            return firstComponentCount * generator.clusteringDescendantAverages[0];
        }

        // sets the last row we will visit
        // returns expected distance from zero
        private int setLastRow(int position)
        {
            if (position < 0)
                throw new IllegalStateException();

            decompose(position, lastRow);
            int expectedRowCount = 0;
            for (int i = 0 ; i < lastRow.length ; i++)
            {
                int l = lastRow[i];
                expectedRowCount += l * generator.clusteringDescendantAverages[i];
            }
            return expectedRowCount + 1;
        }

        // returns 0 if we are currently on the last row we are allocated to visit; 1 if it is after, -1 if it is before
        // this is defined by _limit_, which is wired up from expected (mean) row counts
        // the last row is where position == lastRow, except the last index is 1 less;
        // OR if that row does not exist, it is the last row prior to it
        private int compareToLastRow(int depth)
        {
            int prev = 0;
            for (int i = 0 ; i <= depth ; i++)
            {
                int p = currentRow[i], l = lastRow[i], r = clusteringComponents[i].size();
                if (prev < 0)
                {
                    // if we're behind our last position in theory, and have known more items to visit in practice
                    // we're definitely behind our last row
                    if (r > 1)
                        return -1;
                    // otherwise move forwards to see if we might have more to visit
                }
                else if (p > l)
                {
                    // prev must be == 0, so if p > l, we're after our last row
                    return 1;
                }
                else if (p == l)
                {
                    // if we're equal to our last row up to our current depth, then we need to loop and look forwards
                }
                else if (r == 1)
                {
                    // if this is our last item in practice, store if we're behind our theoretical position
                    // and move forwards; if every remaining practical item is 1, we're at the last row
                    // otherwise we're before it
                    prev = p - l;
                }
                else
                {
                    // p < l, and r > 1, so we're definitely not at the end
                    return -1;
                }
            }
            return 0;
        }

        /**
         * Translate the scalar position into a tiered position based on mean expected counts
         * @param scalar scalar position
         * @param decomposed target container
         */
        private void decompose(int scalar, int[] decomposed)
        {
            for (int i = 0 ; i < decomposed.length ; i++)
            {
                int avg = generator.clusteringDescendantAverages[i];
                decomposed[i] = scalar / avg;
                scalar %= avg;
            }
            for (int i = lastRow.length - 1 ; i > 0 ; i--)
            {
                int avg = generator.clusteringComponentAverages[i];
                if (decomposed[i] >= avg)
                {
                    decomposed[i - 1] += decomposed[i] / avg;
                    decomposed[i] %= avg;
                }
            }
        }

        private static int compare(int[] l, int[] r)
        {
            for (int i = 0 ; i < l.length ; i++)
                if (l[i] != r[i])
                    return Integer.compare(l[i], r[i]);
            return 0;
        }

        static enum State
        {
            END_OF_PARTITION, AFTER_LIMIT, SUCCESS;
        }

        /**
         * seek to the provided position to initialise the iterator
         *
         * @param scalar scalar position
         * @return resultant iterator state
         */
        private State seek(int scalar)
        {
            if (scalar == 0)
            {
                this.currentRow[0] = -1;
                clusteringComponents[0].addFirst(this);
                return setHasNext(advance(0, true));
            }
            decompose(scalar, this.currentRow);
            return seekToCurrentRow();
        }
        private State seekToCurrentRow()
        {
            int[] position = this.currentRow;
            for (int i = 0 ; i < position.length ; i++)
            {
                if (i != 0)
                    fill(i);
                for (int c = position[i] ; c > 0 ; c--)
                    clusteringComponents[i].poll();

                // we can have started from a position that does not exist, in which
                // case we need to ascend back up our clustering components, advancing as we go
                if (clusteringComponents[i].isEmpty())
                {
                    int j = i;
                    while (true)
                    {
                        // if we've exhausted the whole partition, we're done
                        if (--j < 0)
                            return setHasNext(false);

                        clusteringComponents[j].poll();
                        if (!clusteringComponents[j].isEmpty())
                            break;
                    }

                    // we don't check here to see if we've exceeded our lastRow,
                    // because if we came to a non-existent position and generated a lastRow
                    // we want to at least find the next real position, and set it on the seed
                    // in this case we do then yield false and select a different seed to continue with
                    position[j]++;
                    Arrays.fill(position, j + 1, position.length, 0);
                    while (j < i)
                        fill(++j);
                }

                row.row[i] = clusteringComponents[i].peek();
            }

            if (compareToLastRow(currentRow.length - 1) > 0)
                return setHasNext(false);

            // call advance so we honour any select chance
            position[position.length - 1]--;
            clusteringComponents[position.length - 1].addFirst(this);
            return setHasNext(advance(position.length - 1, true));
        }

        // normal method for moving the iterator forward; maintains the row object, and delegates to advance(int)
        // to move the iterator to the next item
        Row advance()
        {
            // we are always at the leaf level when this method is invoked
            // so we calculate the seed for generating the row by combining the seed that generated the clustering components
            int depth = clusteringComponents.length - 1;
            long parentSeed = clusteringSeeds[depth];
            long rowSeed = seed(clusteringComponents[depth].peek(), generator.clusteringComponents.get(depth).type, parentSeed);

            Row result = row.copy();
            // and then fill the row with the _non-clustering_ values for the position we _were_ at, as this is what we'll deliver
            double valueColumn = 0.0;

            for (int i = clusteringSeeds.length ; i < row.row.length ; i++)
            {
                Generator gen = generator.valueComponents.get(i - clusteringSeeds.length);
                if (++valueColumn / totalValueColumns > rowPopulationRatio)
                {
                    result.row[i] = null;
                }
                else
                {
                    gen.setSeed(rowSeed);
                    result.row[i] = gen.generate();
                }
            }

            // then we advance the leaf level
            setHasNext(advance(depth, false));
            return result;
        }

        private boolean advance(int depth, boolean first)
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            // advance the leaf component
            clusteringComponents[depth].poll();
            currentRow[depth]++;
            while (true)
            {
                if (clusteringComponents[depth].isEmpty())
                {
                    // if we've run out of clustering components at this level, ascend
                    if (depth == 0)
                        return false;
                    depth--;
                    clusteringComponents[depth].poll();
                    if (++currentRow[depth] > lastRow[depth])
                        return false;
                    continue;
                }

                int compareToLastRow = compareToLastRow(depth);
                if (compareToLastRow > 0)
                {
                    assert !first;
                    return false;
                }
                boolean forceReturnOne = first && compareToLastRow == 0;

                // the chance of descending is the uniform usechance, multiplied by the number of children
                // we would on average generate (so if we have a 0.1 use chance, but should generate 10 children
                // then we will always descend), multiplied by 1/(compound roll), where (compound roll) is the
                // chance with which we reached this depth, i.e. if we already beat 50/50 odds, we double our
                // chance of beating this next roll
                double thischance = useChance * chancemodifier[depth];
                if (forceReturnOne || thischance > 0.99999f || thischance >= random.nextDouble())
                {
                    // if we're descending, we fill in our clustering component and increase our depth
                    row.row[depth] = clusteringComponents[depth].peek();
                    depth++;
                    if (depth == clusteringComponents.length)
                        return true;
                    // if we haven't reached the leaf, we update our probability statistics, fill in all of
                    // this level's clustering components, and repeat
                    if (useChance < 1d)
                    {
                        rollmodifier[depth] = rollmodifier[depth - 1] / Math.min(1d, thischance);
                        chancemodifier[depth] = generator.clusteringDescendantAverages[depth] * rollmodifier[depth];
                    }
                    currentRow[depth] = 0;
                    fill(depth);
                    continue;
                }

                if (compareToLastRow >= 0)
                    return false;

                // if we don't descend, we remove the clustering suffix we've skipped and continue
                clusteringComponents[depth].poll();
                currentRow[depth]++;
            }
        }

        private Pair<int[], Object[]> randomBound(int clusteringComponentDepth)
        {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            int[] position = new int[clusteringComponentDepth];
            Object[] bound = new Object[clusteringComponentDepth];
            position[0] = rnd.nextInt(clusteringComponents[0].size());
            bound[0] = Iterables.get(clusteringComponents[0], position[0]);
            for (int d = 1 ; d < clusteringComponentDepth ; d++)
            {
                fill(d);
                position[d] = rnd.nextInt(clusteringComponents[d].size());
                bound[d] = Iterables.get(clusteringComponents[d], position[d]);
            }
            for (int d = 1 ; d < clusteringComponentDepth ; d++)
                clusteringComponents[d].clear();
            return Pair.create(position, bound);
        }

        // generate the clustering components for the provided depth; requires preceding components
        // to have been generated and their seeds populated into clusteringSeeds
        void fill(int depth)
        {
            long seed = depth == 0 ? idseed : clusteringSeeds[depth - 1];
            Generator gen = generator.clusteringComponents.get(depth);
            gen.setSeed(seed);
            fill(clusteringComponents[depth], (int) gen.clusteringDistribution.next(), gen);
            clusteringSeeds[depth] = seed(clusteringComponents[depth].peek(), generator.clusteringComponents.get(depth).type, seed);
        }

        // generate the clustering components into the queue
        void fill(Queue<Object> queue, int count, Generator generator)
        {
            if (count == 1)
            {
                queue.add(generator.generate());
                return;
            }

            switch (order)
            {
                case SORTED:
                    if (Comparable.class.isAssignableFrom(generator.clazz))
                    {
                        tosort.clear();
                        for (int i = 0 ; i < count ; i++)
                            tosort.add(generator.generate());
                        Collections.sort((List<Comparable>) (List<?>) tosort);
                        for (int i = 0 ; i < count ; i++)
                            if (i == 0 || ((Comparable) tosort.get(i - 1)).compareTo(tosort.get(i)) < 0)
                                queue.add(tosort.get(i));
                        break;
                    }
                case ARBITRARY:
                    unique.clear();
                    for (int i = 0 ; i < count ; i++)
                    {
                        Object next = generator.generate();
                        if (unique.add(next))
                            queue.add(next);
                    }
                    break;
                case SHUFFLED:
                    unique.clear();
                    tosort.clear();
                    ThreadLocalRandom rand = ThreadLocalRandom.current();
                    for (int i = 0 ; i < count ; i++)
                    {
                        Object next = generator.generate();
                        if (unique.add(next))
                            tosort.add(next);
                    }
                    for (int i = 0 ; i < tosort.size() ; i++)
                    {
                        int index = rand.nextInt(i, tosort.size());
                        Object obj = tosort.get(index);
                        tosort.set(index, tosort.get(i));
                        queue.add(obj);
                    }
                    break;
                default:
                    throw new IllegalStateException();
            }
        }

        public boolean hasNext()
        {
            return hasNext;
        }

        public Row next()
        {
            if (!hasNext())
                throw new NoSuchElementException();
            return advance();
        }

        public boolean finishedPartition()
        {
            return clusteringComponents[0].isEmpty();
        }

        private State setHasNext(boolean hasNext)
        {
            this.hasNext = hasNext;
            if (!hasNext)
            {
                boolean isLast = finishedPartition();
                if (isWrite)
                {
                    boolean isFirst = isFirstWrite;
                    if (isFirst)
                        seedManager.markFirstWrite(seed, isLast);
                    if (isLast)
                        seedManager.markLastWrite(seed, isFirst);
                }
                return isLast ? State.END_OF_PARTITION : State.AFTER_LIMIT;
            }
            return State.SUCCESS;
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    // calculate a new seed based on the combination of a parent seed and the generated child, to generate
    // any children of this child
    static long seed(Object object, AbstractType type, long seed)
    {
        if (object instanceof ByteBuffer)
        {
            ByteBuffer buf = (ByteBuffer) object;
            for (int i = buf.position() ; i < buf.limit() ; i++)
                seed = (31 * seed) + buf.get(i);
            return seed;
        }
        else if (object instanceof String)
        {
            String str = (String) object;
            for (int i = 0 ; i < str.length() ; i++)
                seed = (31 * seed) + str.charAt(i);
            return seed;
        }
        else if (object instanceof Number)
        {
            return (seed * 31) + ((Number) object).longValue();
        }
        else if (object instanceof UUID)
        {
            return seed * 31 + (((UUID) object).getLeastSignificantBits() ^ ((UUID) object).getMostSignificantBits());
        }
        else if (object instanceof TimeUUID)
        {
            return seed * 31 + (((TimeUUID) object).lsb() ^ ((TimeUUID) object).msb());
        }
        else
        {
            return seed(type.decomposeUntyped(object), BytesType.instance, seed);
        }
    }

    public Object getPartitionKey(int i)
    {
        return partitionKey[i];
    }

    public String getKeyAsString()
    {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (Object key : partitionKey)
        {
            if (i > 0)
                sb.append("|");
            AbstractType type = generator.partitionKey.get(i++).type;
            sb.append(type.getString(type.decomposeUntyped(key)));
        }
        return sb.toString();
    }
}
