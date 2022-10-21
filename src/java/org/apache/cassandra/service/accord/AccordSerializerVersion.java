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

package org.apache.cassandra.service.accord;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.MessageVersionProvider;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

public enum AccordSerializerVersion implements MessageVersionProvider
{
    // If MessagingService version bumps, this mapping does not need to be updated; only updates needed are those that
    // include accord serializer changes.
    V1(1, MessagingService.VERSION_40);

    public static final AccordSerializerVersion CURRENT = V1;
    public static final Serializer serializer = new Serializer();

    public final int version;
    public final int msgVersion;

    AccordSerializerVersion(int version, int msgVersion)
    {
        this.version = version;
        this.msgVersion = msgVersion;
    }

    public static AccordSerializerVersion fromVersion(int version)
    {
        switch (version)
        {
            case 1:
                return V1;
            default:
                throw new IllegalArgumentException();
        }
    }

    public static AccordSerializerVersion fromMessageVersion(int version)
    {
        AccordSerializerVersion[] versions = values();
        for (int i = versions.length - 1; i >= 0; i--)
        {
            AccordSerializerVersion v = versions[i];
            // If network version bumped (12 to 13), the accord serializers may not have been changed; use the largest
            // version smaller than or equal to this version
            if (v.msgVersion <= version)
                return v;
        }
        throw new IllegalArgumentException("Attempted to use message version " + version + " which is smaller than " + versions[0] + " can handle (" + versions[0].msgVersion + ")");
    }

    @Override
    public int messageVersion()
    {
        return msgVersion;
    }

    public static class Serializer implements IVersionedSerializer<AccordSerializerVersion>
    {
        @Override
        public void serialize(AccordSerializerVersion t, DataOutputPlus out, int version) throws IOException
        {
            serialize(t, out);
        }

        public void serialize(AccordSerializerVersion t, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(t.version);
        }

        @Override
        public AccordSerializerVersion deserialize(DataInputPlus in, int version) throws IOException
        {
            return deserialize(in);
        }

        public AccordSerializerVersion deserialize(DataInputPlus in) throws IOException
        {
            return fromVersion(in.readUnsignedVInt32());
        }

        @Override
        public long serializedSize(AccordSerializerVersion t, int version)
        {
            return serializedSize(t);
        }

        public long serializedSize(AccordSerializerVersion t)
        {
            return TypeSizes.sizeofUnsignedVInt(t.version);
        }
    }
}
