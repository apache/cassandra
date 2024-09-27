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

package accord.utils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class for creating seeds.  This class mostly matches the semantics of {@link java.util.Random} but makes the logic work
 * for any random source.  This class should be used in replacement of most seed methods, and should always replace {@link java.util.concurrent.ThreadLocalRandom}
 * as that randomness will have a bias twords the same seed after a restart (if you rerun randomized tests by restarting
 * the JVM you will run with the same seed over and over again).
 */
public class SeedProvider
{
    public static final SeedProvider instance = new SeedProvider();
    private final AtomicLong seedUniquifier = new AtomicLong(8682522807148012L);

    private long seedUniquifier()
    {
        // L'Ecuyer, "Tables of Linear Congruential Generators of
        // Different Sizes and Good Lattice Structure", 1999
        for (; ; )
        {
            long current = seedUniquifier.get();
            long next = current * 1181783497276652981L;
            if (seedUniquifier.compareAndSet(current, next))
                return next;
        }
    }

    public long nextSeed()
    {
        return seedUniquifier() ^ System.nanoTime();
    }
}
