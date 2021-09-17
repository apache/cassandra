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
import java.util.Random;

import com.google.common.base.Preconditions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableUniqueIdentifier;
import org.apache.cassandra.io.util.SequentialWriterOption;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class NdiRandomizedTest extends RandomizedTest
{
    private static Thread.UncaughtExceptionHandler handler;

    @SuppressWarnings("unused")
    @BeforeClass
    private static void saveUncaughtExceptionHandler()
    {
        handler = Thread.getDefaultUncaughtExceptionHandler();
        DatabaseDescriptor.daemonInitialization();
    }

    @SuppressWarnings("unused")
    @AfterClass
    private static void restoreUncaughtExceptionHandler()
    {
        Thread.setDefaultUncaughtExceptionHandler(handler);
    }

    private static IndexComponentsLeakDetector indexComponentsLeakDetector;

    protected static TemporaryFolder temporaryFolder;

    @ClassRule
    public static TestRule classRules = RuleChain.outerRule(indexComponentsLeakDetector = new IndexComponentsLeakDetector())
                                                 .around(temporaryFolder = new TemporaryFolder());

    public IndexComponents newIndexComponents() throws IOException
    {
        return indexComponentsLeakDetector.newIndexComponents(randomSimpleString(7, 29),
                                                              new Descriptor(temporaryFolder.newFolder(),
                                                                             randomSimpleString(5, 13),
                                                                             randomSimpleString(3, 17),
                                                                             new SequenceBasedSSTableUniqueIdentifier(randomIntBetween(0, 128))),
                                                              SequentialWriterOption.newBuilder()
                                                                                    .bufferSize(randomIntBetween(17, 1 << 13))
                                                                                    .bufferType(randomBoolean() ? BufferType.ON_HEAP : BufferType.OFF_HEAP)
                                                                                    .trickleFsync(randomBoolean())
                                                                                    .trickleFsyncByteInterval(nextInt(1 << 10, 1 << 16))
                                                                                    .finishOnClose(true)
                                                                                    .build(), null);
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
    // the contract of RandomizedTest where the max value is inclusive
    //

    public static int nextInt(int max)
    {
        return nextInt(0, max);
    }

    public static int nextInt(int min, int max)
    {
        return between(min, max - 1);
    }

    public static long nextLong(long min, long max)
    {
        return between(min, max - 1);
    }

    public static long between(long min, long max)
    {
        return randomLongBetween(min, max);
    }

    public static int randomIntBetween(int min, int max)
    {
        if (min < 0) throw new IllegalArgumentException("min must be >= 0: " + min);
        if (min > max) throw new IllegalArgumentException("max must be >= min: " + min + ", " + max);
        return min == max ? min : (int) randomDoubleBetween((double) min, (double) max);
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

        return min == max ? min : min + (max - min) * randomDouble();
    }

    public static long scaledRandomLongBetween(long min, long max)
    {
        if (min < 0) throw new IllegalArgumentException("min must be >= 0: " + min);
        if (min > max) throw new IllegalArgumentException("max must be >= min: " + min + ", " + max);

        double point = Math.min(1, Math.abs(randomGaussian()) * 0.3) * multiplier();
        double range = max - min;
        long scaled = Math.round(Math.min(point * range, range));
        return isNightly() ? max - scaled : min + scaled;
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

    public static int[] shuffle(int[] array)
    {
        Random rgen = new Random();

        for (int i=0; i< array.length; i++)
        {
            int randomPosition = rgen.nextInt(array.length);
            int temp = array[i];
            array[i] = array[randomPosition];
            array[randomPosition] = temp;
        }

        return array;
    }
}
