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

package org.apache.cassandra.tools.nodetool;

import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.junit.Assert.assertTrue;

public class SetValueForConfigTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
    }

    private class TestData
    {
        String[] cmds;
        String[] outputs;

        TestData(String[] cmds, String[] outputs)
        {
            this.cmds = cmds;
            this.outputs = outputs;
        }
    }

    @Test
    public void testSetValues() {
        TestData[] testData = {
            // Unknown field should fail
            new TestData(new String[]{"setvalueforconfig", "abc", "true"}, new String[]{"Unable to find type for field name: abc"}),
            // test boolean
            new TestData(new String[]{"setvalueforconfig", "start_native_transport", "true"}, new String[]{"Successfully set the value for start_native_transport to true"}),
            new TestData(new String[]{"setvalueforconfig", "start_native_transport", "false"}, new String[]{"Successfully set the value for start_native_transport to false"}),
            new TestData(new String[]{"setvalueforconfig", "start_native_transport", "Trues"}, new String[]{"Error parsing the given value Trues to type boolean for field start_native_transport"}),
            // test Boolean
            new TestData(new String[]{"setvalueforconfig", "compaction_ignore_disk_check", "true"}, new String[]{"Successfully set the value for compaction_ignore_disk_check to true"}),
            new TestData(new String[]{"setvalueforconfig", "compaction_ignore_disk_check", "false"}, new String[]{"Successfully set the value for compaction_ignore_disk_check to false"}),
            new TestData(new String[]{"setvalueforconfig", "compaction_ignore_disk_check", "Trues"}, new String[]{"Error parsing the given value Trues to type java.lang.Boolean for field compaction_ignore_disk_check"}),
            // test int
            new TestData(new String[]{"setvalueforconfig", "native_transport_max_threads", "1"}, new String[]{"Successfully set the value for native_transport_max_threads to 1"}),
            new TestData(new String[]{"setvalueforconfig", "native_transport_max_threads", "a8"}, new String[]{"Error parsing the given value a8 to type int for field native_transport_max_threads"}),
            // test integer
            new TestData(new String[]{"setvalueforconfig", "compaction_tombstone_warning_threshold", "1"}, new String[]{"Successfully set the value for compaction_tombstone_warning_threshold to 1"}),
            new TestData(new String[]{"setvalueforconfig", "compaction_tombstone_warning_threshold", "a8"}, new String[]{"Error parsing the given value a8 to type java.lang.Integer for field compaction_tombstone_warning_threshold"}),
            // test long
            new TestData(new String[]{"setvalueforconfig", "native_transport_max_concurrent_connections", "1"}, new String[]{"Successfully set the value for native_transport_max_concurrent_connections to 1"}),
            new TestData(new String[]{"setvalueforconfig", "native_transport_max_concurrent_connections", "a8"}, new String[]{"Error parsing the given value a8 to type long for field native_transport_max_concurrent_connections"}),
            // test Long
            new TestData(new String[]{"setvalueforconfig", "auth_check_interval_in_ms", "1"}, new String[]{"Successfully set the value for auth_check_interval_in_ms to 1"}),
            new TestData(new String[]{"setvalueforconfig", "auth_check_interval_in_ms", "a8"}, new String[]{"Error parsing the given value a8 to type java.lang.Long for field auth_check_interval_in_ms"}),
            // test double
            new TestData(new String[]{"setvalueforconfig", "phi_convict_threshold", "8.3"}, new String[]{"Successfully set the value for phi_convict_threshold to 8.3"}),
            new TestData(new String[]{"setvalueforconfig", "phi_convict_threshold", "a8"}, new String[]{"Error parsing the given value a8 to type double for field phi_convict_threshold"}),
            // test Double
            new TestData(new String[]{"setvalueforconfig", "bad_query_tracing_fraction", "8.3"}, new String[]{"Successfully set the value for bad_query_tracing_fraction to 8.3"}),
            new TestData(new String[]{"setvalueforconfig", "bad_query_tracing_fraction", "a8"}, new String[]{"Error parsing the given value a8 to type java.lang.Double for field bad_query_tracing_fraction"}),
            // test Float
            new TestData(new String[]{"setvalueforconfig", "memtable_cleanup_threshold", "8.3"}, new String[]{"Successfully set the value for memtable_cleanup_threshold to 8.3"}),
            new TestData(new String[]{"setvalueforconfig", "memtable_cleanup_threshold", "a8"}, new String[]{"Error parsing the given value a8 to type java.lang.Float for field memtable_cleanup_threshold"}),
            // test DurationSpec
            new TestData(new String[]{"setvalueforconfig", "validation_preview_purge_head_start", "1m"}, new String[]{"Successfully set the value for validation_preview_purge_head_start to 1m"}),
            // test duration too long will get error
            new TestData(new String[]{"setvalueforconfig", "permissions_validity", "100000d"}, new String[]{"Invalid duration: 100000d. It shouldn't be more than 2147483646 in milliseconds"}),
            // test DataStorageSpec
            new TestData(new String[]{"setvalueforconfig", "local_read_size_fail_threshold", "1KiB"}, new String[]{"Successfully set the value for local_read_size_fail_threshold to 1KiB"}),
            // test data storage value too small
            new TestData(new String[]{"setvalueforconfig", "index_summary_capacity", "1KiB"}, new String[]{"Invalid data storage: 1KiB Accepted units:[MEBIBYTES, GIBIBYTES]"}),
            // test DataRateSpec
            new TestData(new String[]{"setvalueforconfig", "compaction_throughput", "1MiB/s"}, new String[]{"Successfully set the value for compaction_throughput to 1MiB/s"}),
            // wrong unit
            new TestData(new String[]{"setvalueforconfig", "compaction_throughput", "1MiB"}, new String[]{"Invalid data rate: 1MiB Accepted units: MiB/s, KiB/s, B/s where case matters and only non-negative values are valid"}),
            // test Map (JSON string)
            new TestData(new String[]{"setvalueforconfig", "startup_checks", "{\"k\":\"v\"}"}, new String[]{"Successfully set the value for startup_checks to {k=v}"}),
            // test Map (JSON string with boolean value)
            new TestData(new String[]{"setvalueforconfig", "startup_checks", "{\"k\":true}"}, new String[]{"Successfully set the value for startup_checks to {k=true}"}),
            // test Map (JSON string with numeric value)
            new TestData(new String[]{"setvalueforconfig", "startup_checks", "{\"k\":1}"}, new String[]{"Successfully set the value for startup_checks to {k=1}"}),
            // test Map invalid JSON
            new TestData(new String[]{"setvalueforconfig", "startup_checks", "{\"k\":\"v\""}, new String[]{"Error parsing the given value {\"k\":\"v\" to type java.util.Map for field startup_checks"}),
            // test Nested Map (JSON with nested object)
            new TestData(new String[]{"setvalueforconfig", "startup_checks", "{\"outer\":{\"inner\":\"v\"}}"}, new String[]{"Successfully set the value for startup_checks to {outer={inner=v}}"}),
            // test Empty Map
            new TestData(new String[]{"setvalueforconfig", "startup_checks", "{}"}, new String[]{"Successfully set the value for startup_checks to {}"}),
            // test Empty string as map type config (invalid)
            new TestData(new String[]{"setvalueforconfig", "startup_checks", ""}, new String[]{"setvalueforconfig requires config field name and the value to be set."}),
        };

        for (int i = 0; i < testData.length; i++)
        {
            // run command
            ToolRunner.ToolResult result = invokeNodetool(testData[i].cmds);
            // check expected output
            for (int j = 0; j < testData[i].outputs.length; j++)
            {
                System.out.println(result.getStdout());
                assertTrue(result.getStdout().contains(testData[i].outputs[j]));
            }
        }
    }
}
