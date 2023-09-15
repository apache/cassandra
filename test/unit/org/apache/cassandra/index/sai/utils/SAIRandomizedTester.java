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

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;

public class SAIRandomizedTester extends SAITester
{
    @SuppressWarnings("unused")
    @BeforeClass
    public static void saveUncaughtExceptionHandler()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static final IndexInputLeakDetector indexInputLeakDetector;

    protected static final TemporaryFolder temporaryFolder;

    @ClassRule
    public static TestRule classRules = RuleChain.outerRule(indexInputLeakDetector = new IndexInputLeakDetector())
                                                 .around(temporaryFolder = new TemporaryFolder());

    public static IndexDescriptor newIndexDescriptor() throws IOException
    {
        String keyspace = randomSimpleString(5, 13);
        String table = randomSimpleString(3, 17);
        TableMetadata metadata = TableMetadata.builder(keyspace, table)
                                              .addPartitionKeyColumn(randomSimpleString(3, 15), Int32Type.instance)
                                              .partitioner(Murmur3Partitioner.instance)
                                              .build();
        return indexInputLeakDetector.newIndexDescriptor(new Descriptor(new File(temporaryFolder.newFolder()),
                                                                        randomSimpleString(5, 13),
                                                                        randomSimpleString(3, 17),
                                                                        new SequenceBasedSSTableId(getRandom().nextIntBetween(0, 128))),
                                                         metadata,
                                                         IndexFileUtils.DEFAULT_WRITER_OPTION);
    }

    public static IndexDescriptor newClusteringIndexDescriptor(TableMetadata metadata) throws IOException
    {
        return indexInputLeakDetector.newIndexDescriptor(new Descriptor(new File(temporaryFolder.newFolder()),
                                                                        randomSimpleString(5, 13),
                                                                        randomSimpleString(3, 17),
                                                                        new SequenceBasedSSTableId(getRandom().nextIntBetween(0, 128))),
                                                         metadata,
                                                         IndexFileUtils.DEFAULT_WRITER_OPTION);
    }

    public String newIndex()
    {
        return randomSimpleString(2, 29);
    }

    /**
     * Load a byte array with random bytes. Shortcut for getRandom() method
     */
    public static void nextBytes(byte[] bytes)
    {
        getRandom().nextBytes(bytes);
    }

    public static byte[] nextBytes(int min, int max)
    {
        byte[] bytes = new byte[nextInt(min, max)];
        nextBytes(bytes);
        return bytes;
    }

    //
    // Note: The nextXXX methods maintain the contract of ThreadLocalRandom
    // where the max value is exclusive. The between methods maintain
    // the contract where the max value is inclusive
    //

    public static int nextInt(int max)
    {
        return nextInt(0, max);
    }

    public static int nextInt(int min, int max)
    {
        return getRandom().nextIntBetween(min, max - 1);
    }

    public static long nextLong(long min, long max)
    {
        return between(min, max - 1);
    }

    public static int between(int min, int max)
    {
        return getRandom().nextIntBetween(min, max - 1);
    }

    public static long between(long min, long max)
    {
        return randomLongBetween(min, max);
    }

    public static long randomLongBetween(long min, long max)
    {
        if (min < 0) throw new IllegalArgumentException("min must be >= 0: " + min);
        if (min > max) throw new IllegalArgumentException("max must be >= min: " + min + ", " + max);
        return min == max ? min : (long) randomDoubleBetween((double) min, (double) max);
    }

    public static double randomDoubleBetween(double min, double max)
    {
        if (min < 0) throw new IllegalArgumentException("min must be >= 0: " + min);
        if (min > max) throw new IllegalArgumentException("max must be >= min: " + min + ", " + max);

        return min == max ? min : min + (max - min) * getRandom().nextDouble();
    }

    public static String randomSimpleString(int minLength, int maxLength)
    {
        Preconditions.checkArgument(minLength >= 0);
        Preconditions.checkArgument(maxLength >= 0);
        final int end = nextInt(minLength, maxLength);
        if (end == 0)
        {
            // allow 0 length
            return "";
        }
        final char[] buffer = new char[end];
        for (int i = 0; i < end; i++)
        {
            buffer[i] = (char) nextInt('a', 'z');
        }
        return new String(buffer, 0, end);
    }

    public static void assertPostingListEquals(PostingList expected, PostingList actual) throws IOException
    {
        long actualRowID, rowCounter = 0;
        while ((actualRowID = actual.nextPosting()) != PostingList.END_OF_STREAM)
        {
            assertEquals("Mismatch at pos: " + rowCounter, expected.nextPosting(), actualRowID);
            rowCounter++;
        }
        assertEquals(PostingList.END_OF_STREAM, expected.nextPosting());
    }

    public static void shuffle(int[] array)
    {
        for (int i=0; i< array.length; i++)
        {
            int randomPosition = getRandom().nextIntBetween(0, array.length - 1);
            int temp = array[i];
            array[i] = array[randomPosition];
            array[randomPosition] = temp;
        }
    }
}
