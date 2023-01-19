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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.SAITester;

public class SAIRandomizedTester extends SAITester
{
    @SuppressWarnings("unused")
    @BeforeClass
    public static void saveUncaughtExceptionHandler()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    protected static TemporaryFolder temporaryFolder;

    @ClassRule
    public static TestRule classRules = RuleChain.outerRule(temporaryFolder = new TemporaryFolder());

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
}
