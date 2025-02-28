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

package org.apache.cassandra.distributed.upgrade;

import org.junit.Test;

import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.fail;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class CompressionUpgradeTest extends UpgradeTestBase
{
    @Test
    public void testCompressors() throws Throwable
    {
        run(new String[][]{
        forCompressor(SnappyCompressor.class),
        forCompressor(DeflateCompressor.class),
        forCompressor(LZ4Compressor.class)
        });
    }

    private String[] forCompressor(Class<?> compressorClass)
    {
        String compressorName = compressorClass.getSimpleName();
        return new String[]{
        "CREATE TABLE " + KEYSPACE + ".tbl_" + compressorName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compression = {'class': '" + compressorName + "'}",
        "INSERT INTO " + KEYSPACE + ".tbl_" + compressorName + " (pk, ck, v) VALUES (1, 1, 1)",
        "SELECT * FROM " + KEYSPACE + ".tbl_" + compressorName + " WHERE pk = 1"
        };
    }

    private void run(String[][] queries) throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1)
        .withConfig((cfg) -> cfg.with(NETWORK, GOSSIP))
        .upgradesFrom(v3X)
        .setup((cluster) -> {
            for (int i = 0; i < queries.length; i++)
            {
                try
                {
                    cluster.schemaChange(queries[i][0]);
                    cluster.coordinator(1).execute(queries[i][1], ALL);
                }
                catch (Throwable t)
                {
                    fail(format("Detected error against table %s", queries[i][0]));
                }
            }
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            for (int i : new int[]{ 1, 2 })
                for (int j = 0; j < queries.length; j++)
                {
                    try
                    {
                        assertRows(cluster.coordinator(i).execute(queries[j][2], ALL), row(1, 1, 1));
                    }
                    catch (AssertionError e)
                    {
                        fail(format("Detected failed response from coordinator %s against table %s", i, queries[j][0]));
                    }
                }
        }).run();
    }
}
