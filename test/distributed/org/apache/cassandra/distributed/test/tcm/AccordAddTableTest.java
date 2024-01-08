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

package org.apache.cassandra.distributed.test.tcm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

import org.junit.Test;

import accord.primitives.Ranges;
import accord.primitives.Txn;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

public class AccordAddTableTest extends TestBaseImpl
{
    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = builder().withNodes(6)
                                        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                        .start())
        {
            List<Future<?>> results = new ArrayList<>(cluster.size());
            for (IInvokableInstance inst : cluster)
            {
                Future<?> result = inst.asyncRunsOnInstance(() -> {
                    for (int i = 0; i < 100; i++)
                    {
                        AccordService.instance().maybeConvertTablesToAccord(fakeTxn(i));
                        if (!ClusterMetadata.current().accordTables.contains(fromNum(i)))
                            throw new AssertionError("Table not found in TCM!");
                    }
                }).call();
                results.add(result);
            }
            FBUtilities.waitOnFutures(results);
        }
    }

    private static Txn fakeTxn(int i)
    {
        TableId id = fromNum(i);

        Ranges of = Ranges.of(new TokenRange(AccordRoutingKey.SentinelKey.min(id), AccordRoutingKey.SentinelKey.max(id)));
        return new Txn.InMemory(of, null, null);
    }

    private static TableId fromNum(int i)
    {
        return TableId.fromUUID(new UUID(i, 0)); // not valid... but do we care?
    }
}
