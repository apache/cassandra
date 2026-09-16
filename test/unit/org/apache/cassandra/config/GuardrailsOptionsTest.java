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

package org.apache.cassandra.config;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests the guardrail thresholds a {@code cassandra.yaml} can carry, which {@link GuardrailsOptions} validates on
 * construction. See CASSANDRA-21517 for the treatment of zero.
 */
public class GuardrailsOptionsTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testZeroThresholdsAreAccepted()
    {
        Config config = new Config();
        config.tables_fail_threshold = 0;
        config.keyspaces_warn_threshold = 0;
        config.partition_tombstones_fail_threshold = 0;
        config.data_disk_usage_percentage_warn_threshold = 0;
        config.partition_size_fail_threshold = new DataStorageSpec.LongBytesBound("0B");
        config.column_value_size_warn_threshold = new DataStorageSpec.LongBytesBound("0B");

        GuardrailsOptions options = new GuardrailsOptions(config);

        assertEquals(0, options.getTablesFailThreshold());
        assertEquals(0, options.getKeyspacesWarnThreshold());
        assertEquals(0, options.getPartitionTombstonesFailThreshold());
        assertEquals(0, options.getDataDiskUsagePercentageWarnThreshold());
        assertEquals(0, options.getPartitionSizeFailThreshold().toBytes());
        assertEquals(0, options.getColumnValueSizeWarnThreshold().toBytes());
    }

    @Test
    public void testMinusOneAndNullDisableThresholds()
    {
        Config config = new Config();
        config.tables_fail_threshold = -1;
        config.partition_size_fail_threshold = null;

        GuardrailsOptions options = new GuardrailsOptions(config);

        assertEquals(-1, options.getTablesFailThreshold());
        assertNull(options.getPartitionSizeFailThreshold());
    }

    @Test
    public void testValuesBelowMinusOneAreRejected()
    {
        Config config = new Config();
        config.tables_fail_threshold = -2;

        Assertions.assertThatThrownBy(() -> new GuardrailsOptions(config))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("negative values are not allowed, outside of -1 which disables the guardrail");
    }

    @Test
    public void testPageSizeInBytesThresholds()
    {
        Config config = new Config();
        GuardrailsOptions options = new GuardrailsOptions(config);
        assertNull(options.getPageSizeInBytesWarnThreshold());
        assertNull(options.getPageSizeInBytesFailThreshold());

        config.page_size_in_bytes_warn_threshold = new DataStorageSpec.LongBytesBound("1KiB");
        config.page_size_in_bytes_fail_threshold = new DataStorageSpec.LongBytesBound("1MiB");
        options = new GuardrailsOptions(config);
        assertEquals(1024, options.getPageSizeInBytesWarnThreshold().toBytes());
        assertEquals(1048576, options.getPageSizeInBytesFailThreshold().toBytes());

        config.page_size_in_bytes_warn_threshold = new DataStorageSpec.LongBytesBound("0B");
        config.page_size_in_bytes_fail_threshold = new DataStorageSpec.LongBytesBound("0B");
        options = new GuardrailsOptions(config);
        assertEquals(0, options.getPageSizeInBytesWarnThreshold().toBytes());
        assertEquals(0, options.getPageSizeInBytesFailThreshold().toBytes());

        config.page_size_in_bytes_warn_threshold = new DataStorageSpec.LongBytesBound("1B");
        Assertions.assertThatThrownBy(() -> new GuardrailsOptions(config))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("page_size_in_bytes_warn_threshold should be lower than the fail threshold");
    }
}
