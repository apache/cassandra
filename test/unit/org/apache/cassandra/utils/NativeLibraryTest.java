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
    private static final ThreadLocal<List<long[]>> advisedCalls = new ThreadLocal<>();

    @Test
    public void testSkipCache()
    {
        File file = FileUtils.createDeletableTempFile("testSkipCache", "1");

        NativeLibrary.trySkipCache(file.path(), 0, 0);
    }

    @Test
    @BMRule(name = "record sequential advice",
            targetClass = "org.apache.cassandra.utils.NativeLibraryLinux",
            targetMethod = "callPosixFadvise(int, long, int, int)",
            targetLocation = "AT ENTRY",
            condition = "org.apache.cassandra.utils.NativeLibraryTest.isRecording()",
            action = "return org.apache.cassandra.utils.NativeLibraryTest.recordAdvice($2, $3, $4)")
    public void testSetSequential()
    {
        // POSIX_FADV_SEQUENTIAL (4th argument 2) is a mode switch on the open file description:
        // exactly one call, no byte range, and nothing at all for a closed descriptor.
        assertAdvice(Integer.MAX_VALUE, new long[][] { { 0, 0, 2 } });
        assertAdvice(-1, new long[0][]);
    }

    private static void assertAdvice(int fd, long[][] expected)
    {
        assumeTrue("Sequential cache advice is Linux-only", FBUtilities.isLinux);
        List<long[]> actual = new ArrayList<>();
        advisedCalls.set(actual);
        try
        {
            // Record at the native wrapper, without allocating a file or issuing a syscall.
            NativeLibrary.trySetSequential(fd, "recorded-sequential");
            Assert.assertEquals("Native advice call count", expected.length, actual.size());
            for (int i = 0; i < expected.length; i++)
                Assert.assertArrayEquals("Native advice " + i, expected[i], actual.get(i));
        }
        finally
        {
            advisedCalls.remove();
        }
    }

    public static boolean isRecording()
    {
        return advisedCalls.get() != null;
    }

    public static int recordAdvice(long offset, int length, int advice)
    {
        advisedCalls.get().add(new long[] { offset, length, advice });
        return 0;
    }

    @Test
    public void getPid()
    {
        long pid = NativeLibrary.getProcessID();
        Assert.assertTrue(pid > 0);
    }
}
