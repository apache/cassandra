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

package org.apache.cassandra.io.sstable;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;

public class ApproximateKeyCountTest extends CQLTester
{
    // Regression test for CASSANDRA-21841, previously using the default merge for
    // sparse format HLL returned 140 distinct keys
    @Test
    public void testSSTablesSharingPartitions()
    {
        createTable("CREATE TABLE %s (pk bigint PRIMARY KEY, v int)");
        disableCompaction();

        Random random = new Random(1);
        List<Long> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++)
            keys.add(random.nextLong());

        insertAndFlush(keys);
        insertAndFlush(keys.subList(0, 50));

        long estimate = SSTableReader.getApproximateKeyCount(getCurrentColumnFamilyStore().getLiveSSTables());
        assertEquals(100, estimate, 2);
    }

    private void insertAndFlush(List<Long> keys)
    {
        for (long key : keys)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", key, 0);
        flush();
    }
}
