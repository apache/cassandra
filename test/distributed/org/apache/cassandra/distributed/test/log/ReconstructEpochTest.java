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

package org.apache.cassandra.distributed.test.log;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Retry;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LogState;

public class ReconstructEpochTest extends TestBaseImpl
{
    @Test
    public void logReaderTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (id int primary key)"));
            for (int i = 0; i < 30; i++)
            {
                if (i > 0 && i % 5 == 0)
                    cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().triggerSnapshot());
                cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl WITH comment = '" + i + "'"));
            }

            cluster.get(1).runOnInstance(() -> {
                for (int[] cfg : new int[][]{ new int[]{ 6, 9 },
                                              new int[]{ 2, 20 },
                                              new int[]{ 5, 5 },
                                              new int[]{ 15, 20 } })
                {
                    int start = cfg[0];
                    int end = cfg[1];
                    LogState logState = DistributedMetadataLogKeyspace.getLogState(Epoch.create(start), Epoch.create(end), true);
                    Assert.assertEquals(start, logState.baseState.epoch.getEpoch());
                    Iterator<Entry> iter = logState.entries.iterator();
                    for (int i = start + 1; i <= end; i++)
                        Assert.assertEquals(i, iter.next().epoch.getEpoch());
                }
            });


            cluster.get(2).runOnInstance(() -> {
                for (int[] cfg : new int[][]{ new int[]{ 6, 9 },
                                              new int[]{ 2, 20 },
                                              new int[]{ 5, 5 },
                                              new int[]{ 15, 20 } })
                {
                    int start = cfg[0];
                    int end = cfg[1];
                    LogState logState = ClusterMetadataService.instance()
                                                              .processor()
                                                              .getLogState(Epoch.create(start),
                                                                           Epoch.create(end),
                                                                           true,
                                                                           Retry.Deadline.retryIndefinitely(DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.NANOSECONDS),
                                                                                                            TCMMetrics.instance.commitRetries));

                    Assert.assertEquals(start, logState.baseState.epoch.getEpoch());
                    Iterator<Entry> iter = logState.entries.iterator();
                    for (int i = start + 1; i <= end; i++)
                        Assert.assertEquals(i, iter.next().epoch.getEpoch());
                }
            });
        }
    }
}
