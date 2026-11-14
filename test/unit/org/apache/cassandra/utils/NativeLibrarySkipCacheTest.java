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

package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.List;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * A skip-cache request longer than one fadvise call can take is split into ranges that follow each other.
 */
@RunWith(BMUnitRunner.class)
@BMRule(name = "record each fadvise range",
        targetClass = "org.apache.cassandra.utils.NativeLibrary",
        targetMethod = "trySkipCache(int, long, int, java.lang.String)",
        targetLocation = "AT ENTRY",
        action = "org.apache.cassandra.utils.NativeLibrarySkipCacheTest.record($2, $3)")
public class NativeLibrarySkipCacheTest
{
    private static final List<long[]> ranges = new ArrayList<>();

    public static synchronized void record(long offset, int len)
    {
        ranges.add(new long[]{ offset, len });
    }

    @Test
    public void rangesPastTwoGiBFollowEachOther()
    {
        long start = 1000;
        long len = 5L * Integer.MAX_VALUE + 7;
        synchronized (NativeLibrarySkipCacheTest.class)
        {
            ranges.clear();
        }
        // a negative descriptor makes each call return before it reaches fadvise
        NativeLibrary.trySkipCache(-1, start, len, "test");

        List<long[]> got;
        synchronized (NativeLibrarySkipCacheTest.class)
        {
            got = new ArrayList<>(ranges);
        }
        assertEquals(6, got.size());
        long expectedOffset = start;
        long total = 0;
        for (long[] range : got)
        {
            assertEquals("each range must start where the one before it ended", expectedOffset, range[0]);
            expectedOffset += range[1];
            total += range[1];
        }
        assertEquals(len, total);
    }
}
