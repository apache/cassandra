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

package org.apache.cassandra.tcm.discovery;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.membership.NodeId;

public class SurveyResponse
{
    public static final Serializer serializer = new Serializer();
    public final int metadataId;
    public final NodeId nodeId;
    public final InetAddressAndPort broadcastAddress;

    public SurveyResponse(int metadataId, NodeId nodeId, InetAddressAndPort broadcastAddress)
    {
        this.metadataId = metadataId;
        this.nodeId = nodeId;
        this.broadcastAddress = broadcastAddress;
    }

    @Override
    public String toString()
    {
        return "SurveyResponse{" +
               "metadataId=" + metadataId +
               ", nodeId=" + nodeId +
               ", broadcast_address=" + broadcastAddress +
               '}';
    }

    public static class Serializer implements IVersionedSerializer<SurveyResponse>
    {
        @Override
        public void serialize(SurveyResponse t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(t.metadataId);
            out.writeUnsignedVInt32(t.nodeId.id());
            InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(t.broadcastAddress, out, version);
        }

        @Override
        public SurveyResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            int metadataId = in.readUnsignedVInt32();
            int nodeId = in.readUnsignedVInt32();
            InetAddressAndPort broadcastAddress = InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version);
            return new SurveyResponse(metadataId, new NodeId(nodeId), broadcastAddress);
        }

        @Override
        public long serializedSize(SurveyResponse t, int version)
        {
            return TypeSizes.sizeofUnsignedVInt(t.metadataId) +
                   TypeSizes.sizeofUnsignedVInt(t.nodeId.id()) +
                   InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(t.broadcastAddress, version);
        }
    }
}
