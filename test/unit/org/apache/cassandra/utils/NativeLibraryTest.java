/**
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
 */
package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

public class NativeLibraryTest
{
    @Test
    public void testSkipCache()
    {
        File file = FileUtils.createDeletableTempFile("testSkipCache", "1");

        NativeLibrary.trySkipCache(file.path(), 0, 0);
    }

    @Test
    public void getPid()
    {
        long pid = NativeLibrary.getProcessID();
        Assert.assertTrue(pid > 0);
    }

    @Test
    public void testSkipCacheSplitsLargeRangeAtIncreasingOffsets()
    {
        long offset = 1024;
        long length = 2L * Integer.MAX_VALUE + 1;
        List<long[]> ranges = new ArrayList<>();

        NativeLibrary.trySkipCache(offset, length, (subOffset, subLength) -> ranges.add(new long[]{ subOffset, subLength }));

        Assert.assertEquals(3, ranges.size());
        assertRange(ranges.get(0), offset, Integer.MAX_VALUE);
        assertRange(ranges.get(1), offset + Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertRange(ranges.get(2), offset + 2L * Integer.MAX_VALUE, 1);
    }

    @Test
    public void testSkipCacheDoesNotSplitMaximumIntegerRange()
    {
        long offset = 4096;
        List<long[]> ranges = new ArrayList<>();

        NativeLibrary.trySkipCache(offset, Integer.MAX_VALUE, (subOffset, subLength) -> ranges.add(new long[]{ subOffset, subLength }));

        Assert.assertEquals(1, ranges.size());
        assertRange(ranges.get(0), offset, Integer.MAX_VALUE);
    }

    @Test
    public void testSkipCacheZeroLengthTargetsEntireFile()
    {
        List<long[]> ranges = new ArrayList<>();

        NativeLibrary.trySkipCache(4096, 0, (subOffset, subLength) -> ranges.add(new long[]{ subOffset, subLength }));

        Assert.assertEquals(1, ranges.size());
        assertRange(ranges.get(0), 0, 0);
    }

    private static void assertRange(long[] range, long offset, int length)
    {
        Assert.assertEquals(offset, range[0]);
        Assert.assertEquals(length, range[1]);
    }
}
