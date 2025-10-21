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

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.sequences.AddToCMS;
import org.apache.cassandra.tcm.transformations.cms.RemoveFromCMS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ReconfigureCMSStreamingTest extends TestBaseImpl
{
    @Test
    public void testRF1() throws IOException
    {
        /*
        1. place the CMS on the "wrong" node (2)
        2. execute a bunch of schema changes
        3. run "cms reconfigure 1" which moves the cms back to the correct node (1)
        4. make sure node1 has all the epochs that node2 had before reconfiguration
         */
        try (Cluster cluster = init(Cluster.build()
                                           .withNodes(3)
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                           .start()))
        {
            cluster.get(2).runOnInstance(() -> AddToCMS.initiate());
            cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().commit(new RemoveFromCMS(InetAddressAndPort.getByNameUnchecked("127.0.0.1"), true)));

            for (int i = 0; i < 20; i++)
                cluster.schemaChange(withKeyspace("create table %s.tbl"+i+" (id int primary key)"));

            long[] epochsBefore = epochs(cluster.get(2).executeInternal("select * from system_cluster_metadata.distributed_metadata_log"));
            cluster.get(3).nodetoolResult("cms", "reconfigure", "1");
            long[] epochsAfter = epochs(cluster.get(1).executeInternal("select epoch from system_cluster_metadata.distributed_metadata_log"));
            assertTrue(epochsBefore.length > 20); // at least 20 schema changes above
            assertTrue(epochsAfter.length > epochsBefore.length); // we get a few more epochs from reconfiguration
            for (int i = 0; i < epochsBefore.length; i++)
                assertEquals(epochsBefore[i] + " != " + epochsAfter[i], epochsBefore[i], epochsAfter[i]);
        }
    }

    private long[] epochs(Object[][] res)
    {
        long[] epochs = new long[res.length];
        for (int i = 0; i < res.length; i++)
            epochs[i] = (long)res[i][0];
        Arrays.sort(epochs);
        return epochs;
    }
}
