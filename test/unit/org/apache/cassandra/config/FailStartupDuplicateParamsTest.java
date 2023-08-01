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

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_DUPLICATE_CONFIG_KEYS;
import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_NEW_OLD_CONFIG_KEYS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class FailStartupDuplicateParamsTest
{
    private static final List<String> baseConfig = ImmutableList.of(
        "cluster_name: Test Cluster",
        "commitlog_sync: batch",
        "commitlog_directory: build/test/cassandra/commitlog",
        "hints_directory: build/test/cassandra/hints",
        "partitioner: org.apache.cassandra.dht.ByteOrderedPartitioner",
        "saved_caches_directory: build/test/cassandra/saved_caches",
        "data_file_directories:",
        "   - build/test/cassandra/data",
        "seed_provider:" ,
        "   - class_name: org.apache.cassandra.locator.SimpleSeedProvider",
        "parameters:",
        "   - seeds: \"127.0.0.1:7012\"",
        "endpoint_snitch: org.apache.cassandra.locator.SimpleSnitch");

    @Before
    public void before()
    {
        ALLOW_DUPLICATE_CONFIG_KEYS.setBoolean(true);
        ALLOW_NEW_OLD_CONFIG_KEYS.setBoolean(false);
    }

    @Test
    public void testDuplicateParamThrows() throws IOException
    {
        ALLOW_DUPLICATE_CONFIG_KEYS.setBoolean(false);
        testYaml("found duplicate key endpoint_snitch", true,
                 "endpoint_snitch: org.apache.cassandra.locator.RackInferringSnitch");
    }

    @Test
    public void testReplacementDupesOldFirst() throws IOException
    {
        testYaml("[enable_user_defined_functions -> user_defined_functions_enabled]", true,
                 "enable_user_defined_functions: true",
                 "user_defined_functions_enabled: false");

        testYaml("[enable_user_defined_functions -> user_defined_functions_enabled]", true,
                 "enable_user_defined_functions: true",
                 "user_defined_functions_enabled: true");
    }

    @Test
    public void testReplacementDupesNewFirst() throws IOException
    {
        testYaml("[enable_user_defined_functions -> user_defined_functions_enabled]", true,
                 "user_defined_functions_enabled: false",
                 "enable_user_defined_functions: true");

    }

    @Test
    public void testReplacementDupesMultiReplace() throws IOException
    {
        /*
        @Replaces(oldName = "internode_socket_send_buffer_size_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated = true)
        @Replaces(oldName = "internode_send_buff_size_in_bytes", converter = Converters.BYTES_DATASTORAGE, deprecated = true)
        public DataStorageSpec internode_socket_send_buffer_size = new DataStorageSpec("0B");
        */
        Predicate<String> predicate = (s) -> s.contains("[internode_send_buff_size_in_bytes -> internode_socket_send_buffer_size]") &&
                                             s.contains("[internode_socket_send_buffer_size_in_bytes -> internode_socket_send_buffer_size]");
        String message = " does not contain both [internode_send_buff_size_in_bytes] and [internode_socket_send_buffer_size_in_bytes]";

        testYaml(predicate, true,
                 message,
                 "internode_send_buff_size_in_bytes: 3",
                 "internode_socket_send_buffer_size_in_bytes: 2",
                 "internode_socket_send_buffer_size: 5B");

        // and new first:
        testYaml(predicate, true,
                 message,
                 "internode_socket_send_buffer_size: 5B",
                 "internode_socket_send_buffer_size_in_bytes: 2",
                 "internode_send_buff_size_in_bytes: 3");
    }

    private static void testYaml(String expected, boolean expectFailure, String ... toAdd) throws IOException
    {
        testYaml((s) -> s.contains(expected), expectFailure, "does not contain [" + expected + ']', toAdd);
    }

    private static void testYaml(Predicate<String> exceptionMsgPredicate, boolean expectFailure, String message, String ... toAdd) throws IOException
    {
        Path p = Files.createTempFile("config_dupes",".yaml");
        try
        {
            List<String> lines = new ArrayList<>(baseConfig);
            Collections.addAll(lines, toAdd);
            Files.write(p, lines);
            loadConfig(p.toUri().toURL(), message, exceptionMsgPredicate, expectFailure);
        }
        finally
        {
            Files.delete(p);
        }
    }

    private static void loadConfig(URL config, String message, Predicate<String> exceptionMsgPredicate, boolean expectFailure)
    {
        try
        {
            new YamlConfigurationLoader().loadConfig(config);
        }
        catch (Exception e)
        {
            assertTrue(expectFailure);
            e.printStackTrace(System.out);
            Throwable t = e;
            do
            {
                if (exceptionMsgPredicate.test(t.getMessage()))
                    return;
                t = t.getCause();
            } while (t != null);

            fail("Message\n["+e.getMessage()+ "]\n"+message);
        }
        assertFalse(expectFailure);
    }
}
