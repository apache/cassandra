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

package org.apache.cassandra.gms;

import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.BooleanSerializer;

public class GossipShutdown
{
    public static final Serializer serializer = new Serializer();
    public final EndpointState state;
    public final boolean isForceShutdown;
    public GossipShutdown(EndpointState state, boolean isForceShutdown)
    {
        // clone the endpoint state so this state in message won't be changed
        this.state = new EndpointState(state);
        this.isForceShutdown = isForceShutdown;
    }

    public static final class Serializer implements IVersionedSerializer<GossipShutdown>
    {
        @Override
        public GossipShutdown deserialize(DataInputPlus in, int version) throws IOException
        {
            EndpointState endpointState = EndpointState.serializer.deserialize(in, version);
            boolean isForceShutdown = BooleanSerializer.serializer.deserialize(in, version);
            return new GossipShutdown(endpointState, isForceShutdown);
        }

        @Override
        public void serialize(GossipShutdown t, DataOutputPlus out, int version) throws IOException
        {
            EndpointState.serializer.serialize(t.state, out, version);
            BooleanSerializer.serializer.serialize(t.isForceShutdown, out, version);
        }

        @Override
        public long serializedSize(GossipShutdown t, int version)
        {
            return EndpointState.serializer.serializedSize(t.state, version) + BooleanSerializer.serializer.serializedSize(t.isForceShutdown, version);
        }
    }
}
