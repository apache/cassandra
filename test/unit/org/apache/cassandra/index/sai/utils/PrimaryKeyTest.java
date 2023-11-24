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

package org.apache.cassandra.index.sai.utils;

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.dht.Murmur3Partitioner;

public class PrimaryKeyTest extends AbstractPrimaryKeyTester
{
    @Test
    public void singlePartitionTest()
    {
        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, simplePartition.comparator);
        int rows = nextInt(10, 100);
        PrimaryKey[] keys = new PrimaryKey[rows];
        for (int index = 0; index < rows; index++)
            keys[index] = factory.create(makeKey(simplePartition, index));

        Arrays.sort(keys);

        compareToAndEqualsTests(factory, keys);
    }

    @Test
    public void compositePartitionTest()
    {
        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, compositePartition.comparator);
        int rows = nextInt(10, 100);
        PrimaryKey[] keys = new PrimaryKey[rows];
        for (int index = 0; index < rows; index++)
            keys[index] = factory.create(makeKey(compositePartition, index, index + 1));

        Arrays.sort(keys);

        compareToAndEqualsTests(factory, keys);
    }

    @Test
    public void simplePartitonSingleClusteringAscTest()
    {
        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, simplePartitionSingleClusteringAsc.comparator);
        int rows = nextInt(10, 100);
        PrimaryKey[] keys = new PrimaryKey[rows];
        int partition = 0;
        int clustering = 0;
        for (int index = 0; index < rows; index++)
        {
            keys[index] = factory.create(makeKey(simplePartitionSingleClusteringAsc, partition),
                                         makeClustering(simplePartitionSingleClusteringAsc, Integer.toString(clustering++)));
            if (clustering == 5)
            {
                clustering = 0;
                partition++;
            }
        }

        Arrays.sort(keys);

        compareToAndEqualsTests(factory, keys);
    }

    @Test
    public void simplePartitonStaticAndSingleClusteringAscTest()
    {
        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, simplePartitionStaticAndSingleClusteringAsc.comparator);
//        int rows = nextInt(10, 100);
        int rows = 10;
        PrimaryKey[] keys = new PrimaryKey[rows];
        int partition = 0;
        int clustering = 0;
        for (int index = 0; index < rows; index++)
        {
            if (clustering == 0)
            {
                keys[index] = factory.create(makeKey(simplePartitionSingleClusteringAsc, partition), Clustering.STATIC_CLUSTERING);
                clustering++;
            }
            else
                keys[index] = factory.create(makeKey(simplePartitionSingleClusteringAsc, partition),
                                             makeClustering(simplePartitionSingleClusteringAsc, Integer.toString(clustering++)));
            if (clustering == 5)
            {
                clustering = 0;
                partition++;
            }
        }

        Arrays.sort(keys);

        compareToAndEqualsTests(factory, keys);
    }

    @Test
    public void simplePartitionMultipleClusteringAscTest()
    {
        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, simplePartitionMultipleClusteringAsc.comparator);
        int rows = nextInt(100, 1000);
        PrimaryKey[] keys = new PrimaryKey[rows];
        int partition = 0;
        int clustering1 = 0;
        int clustering2 = 0;
        for (int index = 0; index < rows; index++)
        {
            keys[index] = factory.create(makeKey(simplePartitionMultipleClusteringAsc, partition),
                                         makeClustering(simplePartitionMultipleClusteringAsc, Integer.toString(clustering1), Integer.toString(clustering2++)));
            if (clustering2 == 5)
            {
                clustering2 = 0;
                clustering1++;
                if (clustering1 == 5)
                {
                    clustering1 = 0;
                    partition++;
                }
            }
        }

        Arrays.sort(keys);

        compareToAndEqualsTests(factory, keys);
    }

    @Test
    public void simplePartitonSingleClusteringDescTest()
    {
        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, simplePartitionSingleClusteringDesc.comparator);
        int rows = nextInt(10, 100);
        PrimaryKey[] keys = new PrimaryKey[rows];
        int partition = 0;
        int clustering = 0;
        for (int index = 0; index < rows; index++)
        {
            keys[index] = factory.create(makeKey(simplePartitionSingleClusteringDesc, partition),
                                         makeClustering(simplePartitionSingleClusteringDesc, Integer.toString(clustering++)));
            if (clustering == 5)
            {
                clustering = 0;
                partition++;
            }
        }

        Arrays.sort(keys);

        compareToAndEqualsTests(factory, keys);
    }

    @Test
    public void simplePartitionMultipleClusteringDescTest()
    {
        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, simplePartitionMultipleClusteringDesc.comparator);
        int rows = nextInt(100, 1000);
        PrimaryKey[] keys = new PrimaryKey[rows];
        int partition = 0;
        int clustering1 = 0;
        int clustering2 = 0;
        for (int index = 0; index < rows; index++)
        {
            keys[index] = factory.create(makeKey(simplePartitionMultipleClusteringDesc, partition),
                                         makeClustering(simplePartitionMultipleClusteringDesc, Integer.toString(clustering1), Integer.toString(clustering2++)));
            if (clustering2 == 5)
            {
                clustering2 = 0;
                clustering1++;
                if (clustering1 == 5)
                {
                    clustering1 = 0;
                    partition++;
                }
            }
        }

        Arrays.sort(keys);

        compareToAndEqualsTests(factory, keys);
    }

    @Test
    public void compositePartitionSingleClusteringAscTest()
    {
        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, compositePartitionSingleClusteringAsc.comparator);
        int rows = nextInt(10, 100);
        PrimaryKey[] keys = new PrimaryKey[rows];
        int partition = 0;
        int clustering = 0;
        for (int index = 0; index < rows; index++)
        {
            keys[index] = factory.create(makeKey(compositePartitionSingleClusteringAsc, partition, partition + clustering),
                                         makeClustering(compositePartitionSingleClusteringAsc, Integer.toString(clustering++)));
            if (clustering == 5)
            {
                clustering = 0;
                partition += 5;
            }
        }

        Arrays.sort(keys);

        compareToAndEqualsTests(factory, keys);
    }

    @Test
    public void compositePartitionMultipleClusteringAscTest()
    {
        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, compositePartitionMultipleClusteringAsc.comparator);
        int rows = nextInt(100, 1000);
        PrimaryKey[] keys = new PrimaryKey[rows];
        int partition = 0;
        int clustering1 = 0;
        int clustering2 = 0;
        for (int index = 0; index < rows; index++)
        {
            keys[index] = factory.create(makeKey(compositePartitionMultipleClusteringAsc, partition, partition + clustering1 + clustering2),
                                         makeClustering(compositePartitionMultipleClusteringAsc, Integer.toString(clustering1), Integer.toString(clustering2++)));
            if (clustering2 == 5)
            {
                clustering2 = 0;
                clustering1++;
                if (clustering1 == 5)
                {
                    clustering1 = 0;
                    partition += 25;
                }
            }
        }

        Arrays.sort(keys);

        compareToAndEqualsTests(factory, keys);
    }

    @Test
    public void compositePartitionSingleClusteringDescTest()
    {
        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, compositePartitionSingleClusteringDesc.comparator);
        int rows = nextInt(10, 100);
        PrimaryKey[] keys = new PrimaryKey[rows];
        int partition = 0;
        int clustering = 0;
        for (int index = 0; index < rows; index++)
        {
            keys[index] = factory.create(makeKey(compositePartitionSingleClusteringDesc, partition, partition + clustering),
                                         makeClustering(compositePartitionSingleClusteringDesc, Integer.toString(clustering++)));
            if (clustering == 5)
            {
                clustering = 0;
                partition += 5;
            }
        }

        Arrays.sort(keys);

        compareToAndEqualsTests(factory, keys);
    }

    @Test
    public void compositePartitionMultipleClusteringDescTest()
    {
        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, compositePartitionMultipleClusteringDesc.comparator);
        int rows = nextInt(100, 1000);
        PrimaryKey[] keys = new PrimaryKey[rows];
        int partition = 0;
        int clustering1 = 0;
        int clustering2 = 0;
        for (int index = 0; index < rows; index++)
        {
            keys[index] = factory.create(makeKey(compositePartitionMultipleClusteringDesc, partition, partition + clustering1 + clustering2),
                                         makeClustering(compositePartitionMultipleClusteringDesc, Integer.toString(clustering1), Integer.toString(clustering2++)));
            if (clustering2 == 5)
            {
                clustering2 = 0;
                clustering1++;
                if (clustering1 == 5)
                {
                    clustering1 = 0;
                    partition += 25;
                }
            }
        }

        Arrays.sort(keys);

        compareToAndEqualsTests(factory, keys);
    }

    @Test
    public void simplePartitionMultipleClusteringMixedTest()
    {
        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, simplePartitionMultipleClusteringMixed.comparator);
        int rows = nextInt(100, 1000);
        PrimaryKey[] keys = new PrimaryKey[rows];
        int partition = 0;
        int clustering1 = 0;
        int clustering2 = 0;
        for (int index = 0; index < rows; index++)
        {
            keys[index] = factory.create(makeKey(simplePartitionMultipleClusteringMixed, partition),
                                         makeClustering(simplePartitionMultipleClusteringMixed, Integer.toString(clustering1), Integer.toString(clustering2++)));
            if (clustering2 == 5)
            {
                clustering2 = 0;
                clustering1++;
                if (clustering1 == 5)
                {
                    clustering1 = 0;
                    partition++;
                }
            }
        }

        Arrays.sort(keys);

        compareToAndEqualsTests(factory, keys);
    }

    @Test
    public void compositePartitionMultipleClusteringMixedTest()
    {
        PrimaryKey.Factory factory = new PrimaryKey.Factory(Murmur3Partitioner.instance, compositePartitionMultipleClusteringMixed.comparator);
        int rows = nextInt(100, 1000);
        PrimaryKey[] keys = new PrimaryKey[rows];
        int partition = 0;
        int clustering1 = 0;
        int clustering2 = 0;
        for (int index = 0; index < rows; index++)
        {
            keys[index] = factory.create(makeKey(compositePartitionMultipleClusteringMixed, partition, partition + clustering1 + clustering2),
                                         makeClustering(compositePartitionMultipleClusteringMixed, Integer.toString(clustering1), Integer.toString(clustering2++)));
            if (clustering2 == 5)
            {
                clustering2 = 0;
                clustering1++;
                if (clustering1 == 5)
                {
                    clustering1 = 0;
                    partition += 25;
                }
            }
        }

        Arrays.sort(keys);

        compareToAndEqualsTests(factory, keys);
    }

    private void compareToAndEqualsTests(PrimaryKey.Factory factory, PrimaryKey... keys)
    {
        for (int index = 0; index < keys.length - 1; index++)
        {
            PrimaryKey key = keys[index];
            PrimaryKey tokenOnlyKey = factory.create(key.token());

            assertCompareToAndEquals(tokenOnlyKey, key, 0);
            assertCompareToAndEquals(key, key, 0);
            assertCompareToAndEquals(tokenOnlyKey, tokenOnlyKey, 0);

            // StaticPrimaryKey is a special case. All other keys in the partition are equal to it
            boolean staticComparison = key.kind() == PrimaryKey.Kind.STATIC;
            boolean inPartition = staticComparison;
            for (int comparisonIndex = index + 1; comparisonIndex < keys.length; comparisonIndex++)
            {
                if (staticComparison && keys[comparisonIndex].kind() == PrimaryKey.Kind.STATIC)
                    inPartition = false;
                assertCompareToAndEquals(key, keys[comparisonIndex], inPartition ? 0 : -1);
                assertCompareToAndEquals(tokenOnlyKey, keys[comparisonIndex], tokenOnlyKey.token().equals(keys[comparisonIndex].token()) ? 0 : -1);
            }
        }
    }
}
