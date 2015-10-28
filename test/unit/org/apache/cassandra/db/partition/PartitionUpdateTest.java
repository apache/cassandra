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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

import junit.framework.Assert;

public class PartitionUpdateTest extends CQLTester
{
    @Test
    public void testOperationCount()
    {
        createTable("CREATE TABLE %s (key text, clustering int, a int, s int static, PRIMARY KEY(key, clustering))");
        CFMetaData cfm = currentTableMetadata();

        long timestamp = FBUtilities.timestampMicros();
        PartitionUpdate update = new RowUpdateBuilder(cfm, timestamp, "key0").clustering(1).add("a", 1).buildUpdate();
        Assert.assertEquals(1, update.operationCount());

        update = new RowUpdateBuilder(cfm, timestamp, "key0").buildUpdate();
        Assert.assertEquals(0, update.operationCount());

        update = new RowUpdateBuilder(cfm, timestamp, "key0").add("s", 1).buildUpdate();
        Assert.assertEquals(1, update.operationCount());

        update = new RowUpdateBuilder(cfm, timestamp, "key0").add("s", 1).buildUpdate();
        update = new RowUpdateBuilder(update, timestamp, cfm.params.defaultTimeToLive).clustering(1)
                                                                                      .add("a", 1)
                                                                                      .buildUpdate();
        Assert.assertEquals(2, update.operationCount());
    }

    @Test
    public void testOperationCountWithCompactTable()
    {
        createTable("CREATE TABLE %s (key text PRIMARY KEY, a int) WITH COMPACT STORAGE");
        CFMetaData cfm = currentTableMetadata();

        PartitionUpdate update = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "key0").add("a", 1)
                                                                                                 .buildUpdate();
        Assert.assertEquals(1, update.operationCount());

        update = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), "key0").buildUpdate();
        Assert.assertEquals(0, update.operationCount());
    }
}
