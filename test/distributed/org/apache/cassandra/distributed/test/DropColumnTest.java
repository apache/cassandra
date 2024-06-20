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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.DroppedColumn;

import static org.junit.Assert.assertEquals;

public class DropColumnTest extends TestBaseImpl
{
    @Test
    public void dropTest() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key, a int, b int, c int)"));
            cluster.filters().allVerbs().messagesMatching((i, i1, iMessage) -> {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                return false;
            }).drop();
            cluster.schemaChange(withKeyspace("alter table %s.tbl drop b"));
            cluster.filters().reset();

            long dropTimeNode1 = getDropTime(cluster.get(1));
            long dropTimeNode2 = getDropTime(cluster.get(2));

            assertEquals(dropTimeNode1, dropTimeNode2);

            cluster.get(2).shutdown().get();
            cluster.get(2).startup();

            assertEquals(dropTimeNode1, getDropTime(cluster.get(2)));
        }
    }

    @Test
    public void dropUsingTimestampTest() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key, a int, b int, c int)"));
            cluster.filters().allVerbs().messagesMatching((i, i1, iMessage) -> {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                return false;
            }).drop();
            cluster.schemaChange(withKeyspace("alter table %s.tbl drop b using timestamp 99999"));
            cluster.filters().reset();

            long dropTimeNode1 = getDropTime(cluster.get(1));
            long dropTimeNode2 = getDropTime(cluster.get(2));

            assertEquals(dropTimeNode1, dropTimeNode2);
            assertEquals(99999, dropTimeNode1);

            cluster.get(2).shutdown().get();
            cluster.get(2).startup();

            assertEquals(dropTimeNode1, getDropTime(cluster.get(2)));
        }
    }

    private long getDropTime(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> {
            Map<ByteBuffer, DroppedColumn> droppedColumns = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metadata().droppedColumns;
            assertEquals(1, droppedColumns.size());
            return droppedColumns.values().iterator().next().droppedTime;
        });
    }
}
