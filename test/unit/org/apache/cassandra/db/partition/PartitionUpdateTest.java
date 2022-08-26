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
package org.apache.cassandra.db.partition;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

import org.junit.Assert;

public class PartitionUpdateTest extends CQLTester
{
    @Test
    public void testOperationCount()
    {
        createTable("CREATE TABLE %s (key text, clustering int, a int, s int static, PRIMARY KEY(key, clustering))");
        TableMetadata cfm = currentTableMetadata();

        UpdateBuilder builder = UpdateBuilder.create(cfm, "key0");
        Assert.assertEquals(0, builder.build().operationCount());
        Assert.assertEquals(1, builder.newRow(1).add("a", 1).build().operationCount());

        builder = UpdateBuilder.create(cfm, "key0");
        Assert.assertEquals(1, builder.newRow().add("s", 1).build().operationCount());

        builder = UpdateBuilder.create(cfm, "key0");
        builder.newRow().add("s", 1);
        builder.newRow(1).add("a", 1);
        Assert.assertEquals(2, builder.build().operationCount());
    }

    @Test
    public void testMutationSize()
    {
        createTable("CREATE TABLE %s (key text, clustering int, a int, s int static, PRIMARY KEY(key, clustering))");
        TableMetadata cfm = currentTableMetadata();

        UpdateBuilder builder = UpdateBuilder.create(cfm, "key0");
        builder.newRow().add("s", 1);
        builder.newRow(1).add("a", 2);
        int size1 = builder.build().dataSize();
        Assert.assertEquals(102, size1);

        builder = UpdateBuilder.create(cfm, "key0");
        builder.newRow(1).add("a", 2);
        int size2 = builder.build().dataSize();
        Assert.assertTrue(size1 != size2);

        builder = UpdateBuilder.create(cfm, "key0");
        int size3 = builder.build().dataSize();
        Assert.assertTrue(size2 != size3);

    }

    @Test
    public void testUpdateAllTimestamp()
    {
        createTable("CREATE TABLE %s (key text, clustering int, a int, b int, c int, s int static, PRIMARY KEY(key, clustering))");
        TableMetadata cfm = currentTableMetadata();

        long timestamp = FBUtilities.timestampMicros();
        RowUpdateBuilder rub = new RowUpdateBuilder(cfm, timestamp, "key0").clustering(1).add("a", 1);
        PartitionUpdate pu = rub.buildUpdate();
        PartitionUpdate pu2 = new PartitionUpdate.Builder(pu, 0).updateAllTimestamp(0).build();

        Assert.assertTrue(pu.maxTimestamp() > 0);
        Assert.assertTrue(pu2.maxTimestamp() == 0);
    }
}
