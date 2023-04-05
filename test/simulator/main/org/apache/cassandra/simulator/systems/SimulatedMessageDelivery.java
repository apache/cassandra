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

package org.apache.cassandra.simulator.systems;

import java.net.InetSocketAddress;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageSink;

public class SimulatedMessageDelivery implements IMessageSink
{
    private final ICluster<? extends IInvokableInstance> cluster;
    public SimulatedMessageDelivery(ICluster<? extends IInvokableInstance> cluster)
    {
        this.cluster = cluster;
        cluster.setMessageSink(this);
    }

    @Override
    public void accept(InetSocketAddress to, IMessage message)
    {
        ((InterceptibleThread)Thread.currentThread()).interceptMessage(cluster.get(message.from()), cluster.get(to), message);
    }
}
