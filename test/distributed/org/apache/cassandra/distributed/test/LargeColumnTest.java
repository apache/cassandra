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

package org.apache.cassandra.distributed.test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;

// TODO: this test should be removed after running in-jvm dtests is set up via the shared API repository
public class LargeColumnTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(LargeColumnTest.class);

    private static String str(int length, Random random, long seed)
    {
        random.setSeed(seed);
        char[] chars = new char[length];
        int i = 0;
        int s = 0;
        long v = 0;
        while (i < length)
        {
            if (s == 0)
            {
                v = random.nextLong();
                s = 8;
            }
            chars[i] = (char) (((v & 127) + 32) & 127);
            v >>= 8;
            --s;
            ++i;
        }
        return new String(chars);
    }

    private void testLargeColumns(int nodes, int columnSize, int rowCount) throws Throwable
    {
        Random random = new Random();
        long seed = ThreadLocalRandom.current().nextLong();
        logger.info("Using seed {}", seed);

        try (ICluster cluster = init(builder()
                                     .withNodes(nodes)
                                     .withConfig(config ->
                                                 config.set("commitlog_segment_size", String.format("%dMiB",(columnSize * 3) >> 20))
                                                       .set("internode_application_send_queue_reserve_endpoint_capacity", String.format("%dB", (columnSize * 2)))
                                                       .set("internode_application_send_queue_reserve_global_capacity", String.format("%dB", (columnSize * 3)))
                                                       .set("write_request_timeout", "30s")
                                                       .set("read_request_timeout", "30s")
                                                       .set("memtable_heap_space", "1024MiB")
                                     )
                                     .start()))
        {
            cluster.schemaChange(String.format("CREATE TABLE %s.cf (k int, c text, PRIMARY KEY (k))", KEYSPACE));

            for (int i = 0; i < rowCount; ++i)
                cluster.coordinator(1).execute(String.format("INSERT INTO %s.cf (k, c) VALUES (?, ?);", KEYSPACE), ConsistencyLevel.ALL, i, str(columnSize, random, seed | i));

            for (int i = 0; i < rowCount; ++i)
            {
                Object[][] results = cluster.coordinator(1).execute(String.format("SELECT k, c FROM %s.cf WHERE k = ?;", KEYSPACE), ConsistencyLevel.ALL, i);
                Assert.assertTrue(str(columnSize, random, seed | i).equals(results[0][1]));
            }
        }
    }

    @Test
    public void test() throws Throwable
    {
        testLargeColumns(2, 16 << 20, 5);
    }
}
