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

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.impl.NetworkTopology;
import org.apache.cassandra.utils.Pair;

public class NetworkTopologyTest extends DistributedTestBase
{
    @Test
    public void namedDcTest() throws Throwable
    {
        try (Cluster cluster = Cluster.build()
                                      .withNodeIdTopology(Collections.singletonMap(1, NetworkTopology.dcAndRack("somewhere", "rack0")))
                                      .withRack("elsewhere", "firstrack", 1)
                                      .withRack("elsewhere", "secondrack", 2)
                                      .withDC("nearthere", 4)
                                      .start())
        {
            Assert.assertEquals(1, cluster.stream("somewhere").count());
            Assert.assertEquals(1, cluster.stream("elsewhere", "firstrack").count());
            Assert.assertEquals(2, cluster.stream("elsewhere", "secondrack").count());
            Assert.assertEquals(3, cluster.stream("elsewhere").count());
            Assert.assertEquals(4, cluster.stream("nearthere").count());

            Set<IInstance> expect = cluster.stream().collect(Collectors.toSet());
            Set<IInstance> result = Stream.concat(Stream.concat(cluster.stream("somewhere"),
                                                                cluster.stream("elsewhere")),
                                                  cluster.stream("nearthere")).collect(Collectors.toSet());
            Assert.assertEquals(expect, result);
        }
    }

    @Test
    public void automaticNamedDcTest() throws Throwable

    {
        try (Cluster cluster = Cluster.build()
                                      .withRacks(2, 1, 3)
                                      .start())
        {
            Assert.assertEquals(6, cluster.stream().count());
            Assert.assertEquals(3, cluster.stream("datacenter1").count());
            Assert.assertEquals(3, cluster.stream("datacenter2", "rack1").count());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void noCountsAfterNamingDCsTest()
    {
        Cluster.build()
               .withDC("nameddc", 1)
               .withDCs(1);
    }

    @Test(expected = IllegalStateException.class)
    public void mustProvideNodeCountBeforeWithDCsTest()
    {
        Cluster.build()
               .withDCs(1);
    }

    @Test(expected = IllegalStateException.class)
    public void noEmptyNodeIdTopologyTest()
    {
        Cluster.build().withNodeIdTopology(Collections.emptyMap());
    }

    @Test(expected = IllegalStateException.class)
    public void noHolesInNodeIdTopologyTest()
    {
        Cluster.build().withNodeIdTopology(Collections.singletonMap(2, NetworkTopology.dcAndRack("doomed", "rack")));
    }
}
