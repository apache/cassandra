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


import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

@RunWith(BMUnitRunner.class)
@BMRule(name = "Record fadvise ranges",
        targetClass = "NativeLibrary",
        targetMethod = "trySkipCache(int, long, int, String)",
        targetLocation = "AT ENTRY",
        action = "org.apache.cassandra.utils.NativeLibraryTest.recordFadvise($2, $3)")
public class NativeLibraryTest
{
    private static final List<List<Long>> fadviseRanges = new CopyOnWriteArrayList<>();

    public static void recordFadvise(long offset, int len)
    {
        fadviseRanges.add(List.of(offset, (long) len));
    }

    @Test
    public void testSkipCache()
    {
        File file = FileUtils.createDeletableTempFile("testSkipCache", "1");

        NativeLibrary.trySkipCache(file.path(), 0, 0);
    }

    @Test
    public void testSkipCacheSplitsLongRange()
    {
        long max = Integer.MAX_VALUE;
        long len = 2 * max;
        fadviseRanges.clear();
        NativeLibrary.trySkipCache(-1, 0, len, "test");
        Assert.assertEquals(List.of(List.of(0L, max), List.of(max, max)), fadviseRanges);
    }

    @Test
    public void testSkipCacheZeroLengthKeepsOffset()
    {
        fadviseRanges.clear();
        NativeLibrary.trySkipCache(-1, 4096, 0L, "test");
        Assert.assertEquals(List.of(List.of(4096L, 0L)), fadviseRanges);
    }

    @Test
    public void getPid()
    {
        long pid = NativeLibrary.getProcessID();
        Assert.assertTrue(pid > 0);
    }
}
