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

package org.apache.cassandra.tcm.transformations.cms;

import java.io.IOException;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public abstract class BaseMembershipTransformation implements Transformation
{
    protected final InetAddressAndPort endpoint;
    protected final Replica replica;

    protected BaseMembershipTransformation(InetAddressAndPort endpoint)
    {
        this.endpoint = endpoint;
        this.replica = EntireRange.replica(endpoint);
    }

    public static abstract class SerializerBase<T extends BaseMembershipTransformation> implements AsymmetricMetadataSerializer<Transformation, T>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            T transformation = (T) t;
            InetAddressAndPort.MetadataSerializer.serializer.serialize(transformation.endpoint, out, version);
        }

        public T deserialize(DataInputPlus in, Version version) throws IOException
        {
            InetAddressAndPort addr = InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version);
            return createTransformation(addr);
        }

        public long serializedSize(Transformation t, Version version)
        {
            T transformation = (T) t;
            return InetAddressAndPort.MetadataSerializer.serializer.serializedSize(transformation.endpoint, version);
        }

        public abstract T createTransformation(InetAddressAndPort addr);
    }
}
