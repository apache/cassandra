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

import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.gms.Gossiper;

import static org.apache.cassandra.distributed.test.ReadDigestConsistencyTest.CREATE_TABLE;
import static org.apache.cassandra.distributed.test.ReadDigestConsistencyTest.insertData;
import static org.apache.cassandra.distributed.test.ReadDigestConsistencyTest.testDigestConsistency;

public class MixedModeReadTest extends UpgradeTestBase
{
    @Test
    public void mixedModeReadColumnSubsetDigestCheck() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1)
        .singleUpgrade(v30, v3X)
        .withConfig(config -> config.with(Feature.GOSSIP, Feature.NETWORK))
        .setup(cluster -> {
            cluster.schemaChange(CREATE_TABLE);
            insertData(cluster.coordinator(1));
            testDigestConsistency(cluster.coordinator(1));
            testDigestConsistency(cluster.coordinator(2));
        })
        .runAfterClusterUpgrade(cluster -> {
            // we need to let gossip settle or the test will fail
            int attempts = 1;
            //noinspection Convert2MethodRef
            while (!((IInvokableInstance) (cluster.get(1))).callOnInstance(() -> Gossiper.instance.isAnyNodeOn30()))
            {
                if (attempts++ > 30)
                    throw new RuntimeException("Gossiper.instance.isAnyNodeOn30() continually returns false despite expecting to be true");
                Thread.sleep(1000);
            }

            // should not cause a disgest mismatch in mixed mode
            testDigestConsistency(cluster.coordinator(1));
            testDigestConsistency(cluster.coordinator(2));
        })
        .run();
    }
}
