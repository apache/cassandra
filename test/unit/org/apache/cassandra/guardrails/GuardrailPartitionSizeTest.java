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

package org.apache.cassandra.guardrails;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

public class GuardrailPartitionSizeTest extends GuardrailWarningOnSSTableWriteTester
{
    private static int partitionSizeThreshold;

    @Before
    public void setup()
    {
        partitionSizeThreshold = DatabaseDescriptor.getGuardrailsConfig().partition_size_warn_threshold_in_mb;
        DatabaseDescriptor.getGuardrailsConfig().partition_size_warn_threshold_in_mb = 1;
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.getGuardrailsConfig().partition_size_warn_threshold_in_mb = partitionSizeThreshold;
    }

    @Test
    public void testConfigValidation()
    {
        testValidationOfStrictlyPositiveProperty((c, v) -> c.partition_size_warn_threshold_in_mb = v.intValue(),
                                                 "partition_size_warn_threshold_in_mb");
    }

    @Test
    public void testCompactLargePartition() throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
        disableCompaction();

        // insert stuff into a single partition
        for (int i = 0; i < 23000; i++)
            assertValid("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 100, i, "long string for large partition test");

        assertWarnedOnCompact("Detected partition <redacted> of size 1.1MB is greater than the maximum recommended size (1MB)");
    }
}