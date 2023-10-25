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

import java.util.Optional;

import org.junit.Assume;
import org.junit.Test;

import com.vdurmont.semver4j.Semver;
import oshi.PlatformEnum;

import static org.apache.cassandra.utils.SystemInfo.ADDRESS_SPACE_VIOLATION_MESSAGE;
import static org.apache.cassandra.utils.SystemInfo.NUMBER_OF_PROCESSES_VIOLATION_MESSAGE;
import static org.apache.cassandra.utils.SystemInfo.OPEN_FILES_VIOLATION_MESSAGE;
import static org.apache.cassandra.utils.SystemInfo.SWAP_VIOLATION_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SystemInfoTest
{
    private static class TestSystemInfo extends SystemInfo
    {
        @Override
        public PlatformEnum platform()
        {
            return PlatformEnum.LINUX;
        }

        @Override
        public long getMaxProcess()
        {
            return EXPECTED_MIN_NUMBER_OF_PROCESSES;
        }

        @Override
        public long getMaxOpenFiles()
        {
            return EXPECTED_MIN_NUMBER_OF_OPENED_FILES;
        }

        @Override
        public long getVirtualMemoryMax()
        {
            return EXPECTED_ADDRESS_SPACE;
        }

        @Override
        public long getSwapSize()
        {
            return 0;
        }
    }

    @Test
    public void testSystemInfo()
    {
        SystemInfo oldSystemInfo = FBUtilities.getSystemInfo();

        try
        {
            // valid testing system info does not violate anything
            TestSystemInfo testSystemInfo = new TestSystemInfo();
            assertFalse(testSystemInfo.isDegraded().isPresent());

            // platform

            assertDegradation(new TestSystemInfo()
            {
                @Override
                public PlatformEnum platform()
                {
                    return PlatformEnum.FREEBSD;
                }
            }, "System is running FREEBSD, Linux OS is recommended. ");

            // swap


            assertDegradation(new TestSystemInfo()
            {
                @Override
                public long getSwapSize()
                {
                    return 100;
                }
            }, SWAP_VIOLATION_MESSAGE);

            // address space

            assertDegradation(new TestSystemInfo()
            {
                @Override
                public long getVirtualMemoryMax()
                {
                    return 1234;
                }
            }, ADDRESS_SPACE_VIOLATION_MESSAGE);

            // expected minimal number of opened files

            assertDegradation(new TestSystemInfo()
            {
                @Override
                public long getMaxOpenFiles()
                {
                    return 10;
                }
            }, OPEN_FILES_VIOLATION_MESSAGE);

            // expected number of processes

            assertDegradation(new TestSystemInfo()
            {
                @Override
                public long getMaxProcess()
                {
                    return 5;
                }
            }, NUMBER_OF_PROCESSES_VIOLATION_MESSAGE);

            // test multiple violations

            assertDegradation(new TestSystemInfo()
            {
                @Override
                public PlatformEnum platform()
                {
                    return PlatformEnum.FREEBSD;
                }

                @Override
                public long getSwapSize()
                {
                    return 10;
                }
            }, "System is running FREEBSD, Linux OS is recommended. " + SWAP_VIOLATION_MESSAGE);
        }
        finally
        {
            FBUtilities.setSystemInfoSupplier(() -> oldSystemInfo);
        }
    }

    @Test
    public void testGetKernelVersion()
    {
        Assume.assumeTrue(FBUtilities.isLinux);
        Semver kernelVersion = FBUtilities.getSystemInfo().getKernelVersion();
        assertThat(kernelVersion).isGreaterThan(new Semver("0.0.0", Semver.SemverType.LOOSE))
                                 .isLessThan(new Semver("100.0.0", Semver.SemverType.LOOSE));
    }

    private void assertDegradation(final SystemInfo systemInfo, String expectedDegradation)
    {
        FBUtilities.setSystemInfoSupplier(() -> systemInfo);
        Optional<String> degradations = FBUtilities.getSystemInfo().isDegraded();

        assertTrue(degradations.isPresent());
        assertEquals(expectedDegradation, degradations.get());
    }
}
