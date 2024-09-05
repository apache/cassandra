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

package org.apache.cassandra.service.paxos;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.tcm.Epoch;

import static org.apache.cassandra.distributed.impl.Instance.deserializeMessage;
import static org.junit.Assert.assertTrue;

public class MessageHelper
{
    public static IMessageFilters.Matcher electorateMismatchChecker(final Cluster cluster)
    {
        return (from, to, msg) -> {
            cluster.get(to).runOnInstance(() -> {
                Message<?> message = deserializeMessage(msg);
                if (message.payload instanceof PaxosPrepare.Response)
                {
                    PaxosPrepare.Permitted permitted = ((PaxosPrepare.Response)message.payload).permitted();
                    assertTrue(permitted.gossipInfo.isEmpty());
                    assertTrue(permitted.electorateEpoch.is(Epoch.EMPTY));
                }
            });
            return false;
        };
    }
}
