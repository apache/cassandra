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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.stress.generate.values.Generator;

// a partition is re-used to reduce garbage generation, as is its internal RowIterator
public class Partition
{

    private long idseed;
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

    void setSeed(long seed)
    {
        long idseed = 0;
        for (int i = 0 ; i < partitionKey.length ; i++)
        {
            Generator generator = this.generator.partitionKey.get(i);
            // set the partition key seed based on the current work item we're processing
            generator.setSeed(seed);
            Object key = generator.generate();
            partitionKey[i] = key;
            // then contribute this value to the data seed
            idseed = seed(key, generator.type, idseed);
        }
        this.idseed = idseed;
    }

    public RowIterator iterator(double useChance)
    {
        iterator.reset(useChance, 0);
        return iterator;
    }

    public RowIterator iterator(int targetCount)
    {
        iterator.reset(Double.NaN, targetCount);
        return iterator;
    }

    class SingleRowIterator extends RowIterator
    {
        boolean done;

        void reset(double useChance, int targetCount)
        {
            done = false;
        }

        public Iterable<Row> batch(double ratio)
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
    }

    public abstract class RowIterator
    {
        // we reuse the row object to save garbage
        final Row row = new Row(partitionKey, new Object[generator.clusteringComponents.size() + generator.valueComponents.size()]);

        public abstract Iterable<Row> batch(double ratio);
        abstract void reset(double useChance, int targetCount);

        public abstract boolean done();

        public Partition partition()
        {
            return Partition.this;
        }
    }

    // permits iterating a random subset of the procedurally generated rows in this partition;  this is the only mechanism for visiting rows
    // we maintain a stack of clustering components and their seeds; for each clustering component we visit, we generate all values it takes at that level,
    // and then, using the average (total) number of children it takes we randomly choose whether or not we visit its children;
    // if we do, we generate all possible values the children can take, and repeat the process. So at any one time we are using space proportional
    // to C.N, where N is the average number of values each clustering component takes, as opposed to N^C total values in the partition.
    class MultiRowIterator extends RowIterator
    {

        // probability any single row will be generated in this iteration
        double useChance;
        double expectedRowCount;

        // the current seed in use at any given level; used to save recalculating it for each row, so we only need to recalc
        // from prior row
        final long[] clusteringSeeds = new long[generator.clusteringComponents.size()];
        // the components remaining to be visited for each level of the current stack
        final Queue<Object>[] clusteringComponents = new ArrayDeque[generator.clusteringComponents.size()];

        // we want our chance of selection to be applied uniformly, so we compound the roll we make at each level
        // so that we know with what chance we reached there, and we adjust our roll at that level by that amount
        double[] chancemodifier = new double[generator.clusteringComponents.size()];
        double[] rollmodifier = new double[generator.clusteringComponents.size()];

        // reusable set for generating unique clustering components
        final Set<Object> unique = new HashSet<>();
        final Random random = new Random();

        MultiRowIterator()
        {
            for (int i = 0 ; i < clusteringComponents.length ; i++)
                clusteringComponents[i] = new ArrayDeque<>();
            rollmodifier[0] = 1f;
            chancemodifier[0] = generator.clusteringChildAverages[0];
        }

        void reset(double useChance, int targetCount)
        {
            generator.clusteringComponents.get(0).setSeed(idseed);
            int firstComponentCount = (int) generator.clusteringComponents.get(0).clusteringDistribution.next();
            this.expectedRowCount = firstComponentCount * generator.clusteringChildAverages[0];
            if (Double.isNaN(useChance))
                useChance = Math.max(0d, Math.min(1d, targetCount / expectedRowCount));

            for (Queue<?> q : clusteringComponents)
                q.clear();

            this.useChance = useChance;
            clusteringSeeds[0] = idseed;
            clusteringComponents[0].add(this);
            fill(clusteringComponents[0], firstComponentCount, generator.clusteringComponents.get(0));
            advance(0, 1f);
        }

        void fill(int component)
        {
            long seed = clusteringSeeds[component - 1];
            Generator gen = generator.clusteringComponents.get(component);
            gen.setSeed(seed);
            clusteringSeeds[component] = seed(clusteringComponents[component - 1].peek(), generator.clusteringComponents.get(component - 1).type, seed);
            fill(clusteringComponents[component], (int) gen.clusteringDistribution.next(), gen);
        }

        void fill(Queue<Object> queue, int count, Generator generator)
        {
            if (count == 1)
            {
                queue.add(generator.generate());
            }
            else
            {
                unique.clear();
                for (int i = 0 ; i < count ; i++)
                {
                    Object next = generator.generate();
                    if (unique.add(next))
                        queue.add(next);
                }
            }
        }

        private boolean advance(double continueChance)
        {
            // we always start at the leaf level
            int depth = clusteringComponents.length - 1;
            // fill the row with the position we *were* at (unless pre-start)
            for (int i = clusteringSeeds.length ; i < row.row.length ; i++)
            {
                Generator gen = generator.valueComponents.get(i - clusteringSeeds.length);
                long seed = clusteringSeeds[depth];
                seed = seed(clusteringComponents[depth].peek(), generator.clusteringComponents.get(depth).type, seed);
                gen.setSeed(seed);
                row.row[i] = gen.generate();
            }
            clusteringComponents[depth].poll();

            return advance(depth, continueChance);
        }

        private boolean advance(int depth, double continueChance)
        {
            // advance the leaf component
            clusteringComponents[depth].poll();
            while (true)
            {
                if (clusteringComponents[depth].isEmpty())
                {
                    if (depth == 0)
                        return false;
                    depth--;
                    clusteringComponents[depth].poll();
                    continue;
                }

                // the chance of descending is the uniform use chance, multiplied by the number of children
                // we would on average generate (so if we have a 0.1 use chance, but should generate 10 children
                // then we will always descend), multiplied by 1/(compound roll), where (compound roll) is the
                // chance with which we reached this depth, i.e. if we already beat 50/50 odds, we double our
                // chance of beating this next roll
                double thischance = useChance * chancemodifier[depth];
                if (thischance > 0.999f || thischance >= random.nextDouble())
                {
                    row.row[depth] = clusteringComponents[depth].peek();
                    depth++;
                    if (depth == clusteringComponents.length)
                        break;
                    rollmodifier[depth] = rollmodifier[depth - 1] / Math.min(1d, thischance);
                    chancemodifier[depth] = generator.clusteringChildAverages[depth] * rollmodifier[depth];
                    fill(depth);
                    continue;
                }

                clusteringComponents[depth].poll();
            }

            return continueChance >= 1.0d || continueChance >= random.nextDouble();
        }

        public Iterable<Row> batch(final double ratio)
        {
            final double continueChance = 1d - (Math.pow(ratio, expectedRowCount * useChance));
            return new Iterable<Row>()
            {
                public Iterator<Row> iterator()
                {
                    return new Iterator<Row>()
                    {
                        boolean hasNext = true;
                        public boolean hasNext()
                        {
                            return hasNext;
                        }

                        public Row next()
                        {
                            hasNext = advance(continueChance);
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

        public Partition partition()
        {
            return Partition.this;
        }
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

    // used for thrift smart routing - if it's a multi-part key we don't try to route correctly right now
    public ByteBuffer getToken()
    {
        return generator.partitionKey.get(0).type.decompose(partitionKey[0]);
    }

}
