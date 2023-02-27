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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class PreInitialize implements Transformation
{
    public static Serializer serializer = new Serializer();

    public final InetAddressAndPort addr;

    private PreInitialize(InetAddressAndPort addr)
    {
        this.addr = addr;
    }

    public static PreInitialize forTesting()
    {
        return new PreInitialize(null);
    }

    public static PreInitialize blank()
    {
        return new PreInitialize(null);
    }

    public static PreInitialize withFirstCMS(InetAddressAndPort addr)
    {
        return new PreInitialize(addr);
    }


    public Kind kind()
    {
        return Kind.PRE_INITIALIZE_CMS;
    }

    public Result execute(ClusterMetadata metadata)
    {
        assert metadata.epoch.isBefore(Epoch.FIRST);

        ClusterMetadata.Transformer transformer = metadata.transformer();
        if (addr != null)
            transformer = transformer.withCMSMember(addr);

        ClusterMetadata.Transformer.Transformed transformed = transformer.build();
        metadata = transformed.metadata;
        assert metadata.epoch.is(Epoch.FIRST) : metadata.epoch;

        return success(transformer);
    }

    @Override
    public String toString()
    {
        return "BootstrapCms{" +
               "addr=" + addr +
               '}';
    }

    static class Serializer implements AsymmetricMetadataSerializer<Transformation, PreInitialize>
    {

        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t.kind() == Kind.PRE_INITIALIZE_CMS;
            PreInitialize bcms = (PreInitialize)t;
            out.writeBoolean(bcms.addr != null);
            if (bcms.addr != null)
                InetAddressAndPort.MetadataSerializer.serializer.serialize(((PreInitialize)t).addr, out, version);
        }

        public PreInitialize deserialize(DataInputPlus in, Version version) throws IOException
        {
            boolean hasAddr = in.readBoolean();
            if (!hasAddr)
                return PreInitialize.blank();

            InetAddressAndPort addr = InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version);
            return new PreInitialize(addr);
        }

        public long serializedSize(Transformation t, Version version)
        {
            PreInitialize bcms = (PreInitialize)t;
            long size = TypeSizes.sizeof(bcms.addr != null);

            return size + (bcms.addr != null ? InetAddressAndPort.MetadataSerializer.serializer.serializedSize(((PreInitialize)t).addr, version) : 0);
        }
    }
}