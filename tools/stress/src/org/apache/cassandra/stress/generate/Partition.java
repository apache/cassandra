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
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.stress.generate.values.Generator;

// a partition is re-used to reduce garbage generation, as is its internal RowIterator
// TODO: we should batch the generation of clustering components so we can bound the time and size necessary to
// generate huge partitions with only a small number of clustering components; i.e. we should generate seeds for batches
// of a single component, and then generate the values within those batches as necessary. this will be difficult with
// generating sorted partitions, and may require generator support (e.g. we may need to support generating prefixes
// that are extended/suffixed to generate each batch, so that we can sort the prefixes)
public class Partition
{

    private long idseed;
    private Seed seed;
    private final Object[] partitionKey;
    private final PartitionGenerator generator;
    private final RowIterator iterator;

    public Partition(PartitionGenerator generator)
    {
        this.generator = generator;
        this.partitionKey = new Object[generator.partitionKey.size()];
        if (generator.clusteringComponents.size() > 0)
            iterator = new MultiRowIterator();
        else
            iterator = new SingleRowIterator();
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

    public RowIterator iterator(double useChance, boolean isWrite)
    {
        iterator.reset(useChance, 0, 1, isWrite);
        return iterator;
    }

    public RowIterator iterator(int targetCount, boolean isWrite)
    {
        iterator.reset(Double.NaN, targetCount, 1, isWrite);
        return iterator;
    }

    class SingleRowIterator extends RowIterator
    {
        boolean done;

        void reset(double useChance, int targetCount, int batches, boolean isWrite)
        {
            done = false;
        }

        public Iterable<Row> next()
        {
            if (done)
                return Collections.emptyList();
            for (int i = 0 ; i < row.row.length ; i++)
            {
                Generator gen = generator.valueComponents.get(i);
                gen.setSeed(idseed);
                row.row[i] = gen.generate();
            }
            done = true;
            return Collections.singleton(row);
        }

        public boolean done()
        {
            return done;
        }

        public void markWriteFinished()
        {
            assert done;
            generator.seeds.markFinished(seed);
        }
    }

    public abstract class RowIterator
    {
        // we reuse the row object to save garbage
        final Row row = new Row(partitionKey, new Object[generator.clusteringComponents.size() + generator.valueComponents.size()]);

        public abstract Iterable<Row> next();
        public abstract boolean done();
        public abstract void markWriteFinished();
        abstract void reset(double useChance, int targetCount, int batches, boolean isWrite);

        public Partition partition()
        {
            return Partition.this;
        }
    }

    // permits iterating a random subset of the procedurally generated rows in this partition. this is the only mechanism for visiting rows.
    // we maintain a stack of clustering components and their seeds; for each clustering component we visit, we generate all values it takes at that level,
    // and then, using the average (total) number of children it takes we randomly choose whether or not we visit its children;
    // if we do, we generate all possible values the immediate children can take, and repeat the process. So at any one time we are using space proportional
    // to C.N, where N is the average number of values each clustering component takes, as opposed to N^C total values in the partition.
    // TODO : guarantee at least one row is always returned
    // TODO : support first/last row, and constraining reads to rows we know are populated
    class MultiRowIterator extends RowIterator
    {

        // probability any single row will be generated in this iteration
        double useChance;

        // the seed used to generate the current values for the clustering components at each depth;
        // used to save recalculating it for each row, so we only need to recalc from prior row.
        final long[] clusteringSeeds = new long[generator.clusteringComponents.size()];
        // the components remaining to be visited for each level of the current stack
        final Deque<Object>[] clusteringComponents = new ArrayDeque[generator.clusteringComponents.size()];

        // we want our chance of selection to be applied uniformly, so we compound the roll we make at each level
        // so that we know with what chance we reached there, and we adjust our roll at that level by that amount
        final double[] chancemodifier = new double[generator.clusteringComponents.size()];
        final double[] rollmodifier = new double[generator.clusteringComponents.size()];

        // track where in the partition we are, and where we are limited to
        final int[] position = new int[generator.clusteringComponents.size()];
        final int[] limit = new int[position.length];
        int batchSize;
        boolean returnedOne;
        boolean forceReturnOne;

        // reusable collections for generating unique and sorted clustering components
        final Set<Object> unique = new HashSet<>();
        final List<Comparable> tosort = new ArrayList<>();
        final Random random = new Random();

        MultiRowIterator()
        {
            for (int i = 0 ; i < clusteringComponents.length ; i++)
                clusteringComponents[i] = new ArrayDeque<>();
            rollmodifier[0] = 1f;
            chancemodifier[0] = generator.clusteringChildAverages[0];
        }

        // if we're a write, the expected behaviour is that the requested batch count is compounded with the seed's visit
        // count to decide how much we should return in one iteration
        void reset(double useChance, int targetCount, int batches, boolean isWrite)
        {
            if (this.useChance < 1d)
            {
                // we clear our prior roll-modifiers if the use chance was previously less-than zero
                Arrays.fill(rollmodifier, 1d);
                Arrays.fill(chancemodifier, 1d);
            }

            // set the seed for the first clustering component
            generator.clusteringComponents.get(0).setSeed(idseed);
            int[] position = seed.position;

            // calculate how many first clustering components we'll generate, and how many total rows this predicts
            int firstComponentCount = (int) generator.clusteringComponents.get(0).clusteringDistribution.next();
            int expectedRowCount;

            if (!isWrite && position != null)
            {
                expectedRowCount = 0;
                for (int i = 0 ; i < position.length ; i++)
                {
                    expectedRowCount += position[i] * generator.clusteringChildAverages[i];
                    limit[i] = position[i];
                }
            }
            else
            {
                expectedRowCount = firstComponentCount * generator.clusteringChildAverages[0];
                if (isWrite)
                    batches *= seed.visits;
                Arrays.fill(limit, Integer.MAX_VALUE);
            }

            batchSize = Math.max(1, expectedRowCount / batches);
            if (Double.isNaN(useChance))
                useChance = Math.max(0d, Math.min(1d, targetCount / (double) expectedRowCount));

            // clear any remnants of the last iteration, wire up our constants, and fill in the first clustering components
            this.useChance = useChance;
            this.returnedOne = false;
            for (Queue<?> q : clusteringComponents)
                q.clear();
            clusteringSeeds[0] = idseed;
            fill(clusteringComponents[0], firstComponentCount, generator.clusteringComponents.get(0));

            // seek to our start position
            seek(isWrite ? position : null);
        }

        // generate the clustering components for the provided depth; requires preceding components
        // to have been generated and their seeds populated into clusteringSeeds
        void fill(int depth)
        {
            long seed = clusteringSeeds[depth - 1];
            Generator gen = generator.clusteringComponents.get(depth);
            gen.setSeed(seed);
            clusteringSeeds[depth] = seed(clusteringComponents[depth - 1].peek(), generator.clusteringComponents.get(depth - 1).type, seed);
            fill(clusteringComponents[depth], (int) gen.clusteringDistribution.next(), gen);
        }

        // generate the clustering components into the queue
        void fill(Queue<Object> queue, int count, Generator generator)
        {
            if (count == 1)
            {
                queue.add(generator.generate());
                return;
            }

            switch (Partition.this.generator.order)
            {
                case SORTED:
                    if (Comparable.class.isAssignableFrom(generator.clazz))
                    {
                        tosort.clear();
                        for (int i = 0 ; i < count ; i++)
                            tosort.add((Comparable) generator.generate());
                        Collections.sort(tosort);
                        for (int i = 0 ; i < count ; i++)
                            queue.add(tosort.get(i));
                        break;
                    }
                    else
                    {
                        throw new RuntimeException("Generator class is not comparable: "+generator.clazz);
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
                    for (int i = 0 ; i < count ; i++)
                    {
                        Object next = generator.generate();
                        if (unique.add(next))
                            tosort.add(new RandomOrder(next));
                    }
                    Collections.sort(tosort);
                    for (Object o : tosort)
                        queue.add(((RandomOrder)o).value);
                    break;
                default:
                    throw new IllegalStateException();
            }
        }

        // seek to the provided position (or the first entry if null)
        private void seek(int[] position)
        {
            if (position == null)
            {
                this.position[0] = -1;
                clusteringComponents[0].addFirst(this);
                advance(0);
                return;
            }

            assert position.length == clusteringComponents.length;
            for (int i = 0 ; i < position.length ; i++)
            {
                if (i != 0)
                    fill(i);
                for (int c = position[i] ; c > 0 ; c--)
                    clusteringComponents[i].poll();
                row.row[i] = clusteringComponents[i].peek();
            }
            System.arraycopy(position, 0, this.position, 0, position.length);
        }

        // normal method for moving the iterator forward; maintains the row object, and delegates to advance(int)
        // to move the iterator to the next item
        void advance()
        {
            // we are always at the leaf level when this method is invoked
            // so we calculate the seed for generating the row by combining the seed that generated the clustering components
            int depth = clusteringComponents.length - 1;
            long parentSeed = clusteringSeeds[depth];
            long rowSeed = seed(clusteringComponents[depth].peek(), generator.clusteringComponents.get(depth).type, parentSeed);

            // and then fill the row with the _non-clustering_ values for the position we _were_ at, as this is what we'll deliver
            for (int i = clusteringSeeds.length ; i < row.row.length ; i++)
            {
                Generator gen = generator.valueComponents.get(i - clusteringSeeds.length);
                gen.setSeed(rowSeed);
                row.row[i] = gen.generate();
            }
            returnedOne = true;
            forceReturnOne = false;

            // then we advance the leaf level
            advance(depth);
        }

        private void advance(int depth)
        {
            // advance the leaf component
            clusteringComponents[depth].poll();
            position[depth]++;
            while (true)
            {
                if (clusteringComponents[depth].isEmpty())
                {
                    // if we've run out of clustering components at this level, ascend
                    if (depth == 0)
                        return;
                    depth--;
                    clusteringComponents[depth].poll();
                    position[depth]++;
                    continue;
                }

                if (depth == 0 && !returnedOne && clusteringComponents[0].size() == 1)
                    forceReturnOne = true;

                // the chance of descending is the uniform usechance, multiplied by the number of children
                // we would on average generate (so if we have a 0.1 use chance, but should generate 10 children
                // then we will always descend), multiplied by 1/(compound roll), where (compound roll) is the
                // chance with which we reached this depth, i.e. if we already beat 50/50 odds, we double our
                // chance of beating this next roll
                double thischance = useChance * chancemodifier[depth];
                if (forceReturnOne || thischance > 0.999f || thischance >= random.nextDouble())
                {
                    // if we're descending, we fill in our clustering component and increase our depth
                    row.row[depth] = clusteringComponents[depth].peek();
                    depth++;
                    if (depth == clusteringComponents.length)
                        break;
                    // if we haven't reached the leaf, we update our probability statistics, fill in all of
                    // this level's clustering components, and repeat
                    if (useChance < 1d)
                    {
                        rollmodifier[depth] = rollmodifier[depth - 1] / Math.min(1d, thischance);
                        chancemodifier[depth] = generator.clusteringChildAverages[depth] * rollmodifier[depth];
                    }
                    position[depth] = 0;
                    fill(depth);
                    continue;
                }

                // if we don't descend, we remove the clustering suffix we've skipped and continue
                clusteringComponents[depth].poll();
                position[depth]++;
            }
        }

        public Iterable<Row> next()
        {
            final int[] limit = position.clone();
            int remainingSize = batchSize;
            for (int i = 0 ; i < limit.length && remainingSize > 0 ; i++)
            {
                limit[i] += remainingSize / generator.clusteringChildAverages[i];
                remainingSize %= generator.clusteringChildAverages[i];
            }
            assert remainingSize == 0;
            for (int i = limit.length - 1 ; i > 0 ; i--)
            {
                if (limit[i] > generator.clusteringChildAverages[i])
                {
                    limit[i - 1] += limit[i] / generator.clusteringChildAverages[i];
                    limit[i] %= generator.clusteringChildAverages[i];
                }
            }
            for (int i = 0 ; i < limit.length ; i++)
            {
                if (limit[i] < this.limit[i])
                    break;
                limit[i] = Math.min(limit[i], this.limit[i]);
            }
            return new Iterable<Row>()
            {
                public Iterator<Row> iterator()
                {
                    return new Iterator<Row>()
                    {

                        public boolean hasNext()
                        {
                            if (done())
                                return false;
                            for (int i = 0 ; i < position.length ; i++)
                                if (position[i] < limit[i])
                                    return true;
                            return false;
                        }

                        public Row next()
                        {
                            advance();
                            return row;
                        }

                        public void remove()
                        {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        }

        public boolean done()
        {
            return clusteringComponents[0].isEmpty();
        }

        public void markWriteFinished()
        {
            if (done())
                generator.seeds.markFinished(seed);
            else
                generator.seeds.markVisited(seed, position.clone());
        }

        public Partition partition()
        {
            return Partition.this;
        }
    }

    private static class RandomOrder implements Comparable<RandomOrder>
    {
        final int order = ThreadLocalRandom.current().nextInt();
        final Object value;
        private RandomOrder(Object value)
        {
            this.value = value;
        }

        public int compareTo(RandomOrder that)
        {
            return Integer.compare(this.order, that.order);
        }
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
        else
        {
            return seed(type.decompose(object), BytesType.instance, seed);
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
            sb.append(type.getString(type.decompose(key)));
        }
        return sb.toString();
    }

    // used for thrift smart routing - if it's a multi-part key we don't try to route correctly right now
    public ByteBuffer getToken()
    {
        return generator.partitionKey.get(0).type.decompose(partitionKey[0]);
    }

}
