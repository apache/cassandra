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
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.RemoteProcessor;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class DiscoverCMSTest extends TestBaseImpl
{
    @Test
    public void discoverTest() throws IOException, TimeoutException
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(config -> config.with(NETWORK, GOSSIP))
                                             .withInstanceInitializer(BB::install)
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            // Simulate node2 having an incorrect view of the CMS membership. This triggers a discovery request and
            // potential retry which can be observed in the logs.
            cluster.get(2).runOnInstance(() -> BB.returnIncorrectList.set(true));
            cluster.coordinator(2).execute(withKeyspace("create table %s.tbl2 (id int primary key)"), ConsistencyLevel.ONE);
            cluster.get(2).logs().watchFor("/127.0.0.3:7012 is not a member of the CMS, querying it to discover current membership");
            cluster.get(2).logs().watchFor("Got CMS from /127.0.0.3:7012: DiscoveredNodes\\{nodes=\\[/127.0.0.1:7012\\], kind=CMS_ONLY\\}, retrying on: CandidateIterator\\{candidates=\\[/127.0.0.1:7012, /127.0.0.3:7012\\], checkLive=true}");
        }
    }

    public static class BB
    {
        public static AtomicBoolean returnIncorrectList = new AtomicBoolean(false);
        public static void install(ClassLoader cl, int i)
        {
            if (i != 2)
                return;
            new ByteBuddy().rebase(RemoteProcessor.class)
                           .method(named("candidates"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static List<InetAddressAndPort> candidates(boolean allowDiscovery)
        {
            InetAddressAndPort cms = InetAddressAndPort.getByNameUnchecked("127.0.0.1");
            if (returnIncorrectList.get())
                cms = InetAddressAndPort.getByNameUnchecked("127.0.0.3");
            return Arrays.asList(cms);
        }
    }
}
