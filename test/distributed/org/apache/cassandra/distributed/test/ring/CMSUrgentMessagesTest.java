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

package org.apache.cassandra.distributed.test.ring;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.sequences.AddToCMS;

public class CMSUrgentMessagesTest extends FuzzTestBase
{
    @Test
    public void allPaxosMessagesAreUrgentTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            List<Throwable> thrown = new CopyOnWriteArrayList<>();
            cluster.filters()
                   .allVerbs()
                   .messagesMatching((from, to, msg) -> {
                       Verb verb = Verb.fromId(msg.verb());
                       if (!verb.toString().contains("PAXOS2"))
                           return false;

                       try
                       {
                           boolean hasFlag = cluster.get(1).callOnInstance(() -> Instance.deserializeHeader(msg).hasFlag(MessageFlag.URGENT));
                           assert hasFlag : String.format("%s does not have URGENT flag set: %s", verb, msg);
                       }
                       catch (Throwable t)
                       {
                           thrown.add(t);
                       }
                       return false;
                   })
                   .drop();

            for (int idx : new int[]{ 2, 3 })
                cluster.get(idx).runOnInstance(() -> AddToCMS.initiate());

            if (!thrown.isEmpty())
            {
                Throwable t = new AssertionError("Caught exceptions");
                for (Throwable throwable : thrown)
                    t.addSuppressed(throwable);
                throw t;
            }
        }
    }

}
