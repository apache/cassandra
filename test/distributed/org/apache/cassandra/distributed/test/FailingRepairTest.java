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
import java.io.Serializable;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.util.RepairUtil;
import org.apache.cassandra.service.ActiveRepairService.ParentRepairStatus;

/**
 * Tests check the different code paths expected to cause a repair to fail
 */
//@RunWith(Parameterized.class)
public class FailingRepairTest extends DistributedTestBase implements Serializable
{
    private static Cluster CLUSTER;

//    private final RepairOption option;
//
//    public FailingRepairTest(RepairOption option)
//    {
//        this.option = option;
//    }

//    @Parameterized.Parameters(name = "{0}")
//    public static Collection<Object[]> tests()
//    {
//        List<RepairOption> tests = new ArrayList<>();
//
//        tests.add(RepairUtil.forDataCenter("not-found"));
//
//        return tests.stream().map(tc -> new Object[] { tc }).collect(Collectors.toList());
//    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        // streaming requires networking ATM
        // streaming also requires gossip or isn't setup properly
        CLUSTER = init(Cluster.build(2)
                              .withConfig(c -> c.with(Feature.NETWORK)
                                                .with(Feature.GOSSIP))
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test(timeout = 1 * 60 * 1000)
    public void notThere()
    {
        // test showing what happens when a table doesn't exist
        RepairUtil.Result result = RepairUtil.runRepairAndAwait(CLUSTER.get(2), KEYSPACE, RepairUtil.forTables("doesnotexist"));
        Assert.assertEquals(result.fullStatus.toString(), ParentRepairStatus.FAILED, result.status);
    }

    @Test(timeout = 1 * 60 * 1000)
    public void emptyTables()
    {
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".withindex (key text, value text, PRIMARY KEY (key))");
        CLUSTER.schemaChange("CREATE INDEX value ON " + KEYSPACE + ".withindex (value)");
        // if CF has a . in it, it is assumed to be a 2i which rejects repairs
        RepairUtil.Result result = RepairUtil.runRepairAndAwait(CLUSTER.get(2), KEYSPACE, RepairUtil.forTables("withindex.value"));
        Assert.assertEquals(result.fullStatus.toString(), ParentRepairStatus.COMPLETED, result.status);
    }
}
