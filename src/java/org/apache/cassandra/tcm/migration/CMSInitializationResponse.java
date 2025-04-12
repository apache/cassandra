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

package org.apache.cassandra.tcm.migration;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class CMSInitializationResponse
{
    public static final IVersionedSerializer<CMSInitializationResponse> serializer = new Serializer();

    public final CMSInitializationRequest.Initiator initiator;
    public final boolean metadataMatches;

    public CMSInitializationResponse(CMSInitializationRequest.Initiator initiator, boolean metadataMatches)
    {
        this.initiator = initiator;
        this.metadataMatches = metadataMatches;
    }

    @Override
    public String toString()
    {
        return "CMSInitializationResponse{" +
               "initiator=" + initiator +
               ", metadataMatches=" + metadataMatches +
               '}';
    }

    private static class Serializer implements IVersionedSerializer<CMSInitializationResponse>
    {
        @Override
        public void serialize(CMSInitializationResponse t, DataOutputPlus out, int version) throws IOException
        {
            CMSInitializationRequest.Initiator.serializer.serialize(t.initiator, out, version);
            out.writeBoolean(t.metadataMatches);
        }

        @Override
        public CMSInitializationResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            CMSInitializationRequest.Initiator coordinator = CMSInitializationRequest.Initiator.serializer.deserialize(in, version);
            boolean metadataMatches = in.readBoolean();
            return new CMSInitializationResponse(coordinator, metadataMatches);
        }

        @Override
        public long serializedSize(CMSInitializationResponse t, int version)
        {
            return CMSInitializationRequest.Initiator.serializer.serializedSize(t.initiator, version) +
                   TypeSizes.sizeof(t.metadataMatches);
        }
    }
}