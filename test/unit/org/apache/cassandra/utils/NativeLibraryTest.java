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

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.junit.Assume.assumeTrue;

@RunWith(BMUnitRunner.class)
public class NativeLibraryTest
{
    private static final ThreadLocal<List<long[]>> advisedRanges = new ThreadLocal<>();

    @Test
    public void testSkipCache()
    {
        File file = FileUtils.createDeletableTempFile("testSkipCache", "1");

        NativeLibrary.trySkipCache(file.path(), 0, 0);
    }

    @Test
    @BMRule(name = "record large skip-cache ranges",
            targetClass = "org.apache.cassandra.utils.NativeLibraryLinux",
            targetMethod = "callPosixFadvise(int, long, int, int)",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.utils.NativeLibraryTest.isRecording()",
            action = "return org.apache.cassandra.utils.NativeLibraryTest.recordAdvice($2, $3)")
    public void testSkipCacheLargeRange()
    {
        long offset = 4096;
        // Chunks are aligned down to 2 MiB so no page straddles a boundary and is left in cache.
        long chunk = NativeLibrary.FADVISE_MAX_CHUNK;
        assertAdvisedRanges(offset, 2 * chunk + 17,
                            new long[][] { { offset, chunk },
                                           { offset + chunk, chunk },
                                           { offset + 2 * chunk, 17 } });
    }

    @Test
    @BMRule(name = "record zero-length skip-cache range",
            targetClass = "org.apache.cassandra.utils.NativeLibraryLinux",
            targetMethod = "callPosixFadvise(int, long, int, int)",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.utils.NativeLibraryTest.isRecording()",
            action = "return org.apache.cassandra.utils.NativeLibraryTest.recordAdvice($2, $3)")
    public void testSkipCacheZeroLength()
    {
        long offset = 4096;
        assertAdvisedRanges(offset, 0L, new long[][] { { offset, 0 } });
    }

    private static void assertAdvisedRanges(long offset, long length, long[][] expected)
    {
        assumeTrue("Range cache advice is Linux-only", FBUtilities.isLinux);
        List<long[]> actual = new ArrayList<>();
        advisedRanges.set(actual);
        try
        {
            // Record at the native wrapper, without allocating a file or issuing a syscall.
            NativeLibrary.trySkipCache(Integer.MAX_VALUE, offset, length, "recorded-skip-cache");
            Assert.assertEquals("Native advice call count", expected.length, actual.size());
            for (int i = 0; i < expected.length; i++)
                Assert.assertArrayEquals("Native advice range " + i, expected[i], actual.get(i));
        }
        finally
        {
            advisedRanges.remove();
        }
    }

    public static boolean isRecording()
    {
        return advisedRanges.get() != null;
    }

    public static int recordAdvice(long offset, int length)
    {
        advisedRanges.get().add(new long[] { offset, length });
        return 0;
    }

    @Test
    public void getPid()
    {
        long pid = NativeLibrary.getProcessID();
        Assert.assertTrue(pid > 0);
    }
}
