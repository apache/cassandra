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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.ReconciledLogSnapshot;
import org.apache.cassandra.replication.Version;
import org.apache.cassandra.replication.VersionedSerializer;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;

/**
 * Header for mutation log stream messages containing both manifest and session metadata.
 */
public class LogStreamHeader
{
    public final LogStreamManifest manifest;
    public final ReconciledLogSnapshot reconciled;
    public final InetAddressAndPort sender;
    public final TimeUUID planId;
    public final int sessionIndex;
    public final boolean sendByFollower;

    public LogStreamHeader(LogStreamManifest manifest,
                          ReconciledLogSnapshot reconciled,
                          InetAddressAndPort sender,
                          TimeUUID planId,
                          int sessionIndex,
                          boolean sendByFollower)
    {
        this.manifest = manifest;
        this.reconciled = reconciled;
        this.sender = sender;
        this.planId = planId;
        this.sessionIndex = sessionIndex;
        this.sendByFollower = sendByFollower;
    }

    public static final VersionedSerializer<LogStreamHeader> serializer = new VersionedSerializer<>()
    {
        @Override
        public void serialize(LogStreamHeader header, DataOutputPlus out, Version version) throws IOException
        {
            LogStreamManifest.serializer.serialize(header.manifest, out, version);
            ReconciledLogSnapshot.serializer.serialize(header.reconciled, out, version);
            inetAddressAndPortSerializer.serialize(header.sender, out, version.messagingVersion());
            header.planId.serialize(out);
            out.writeInt(header.sessionIndex);
            out.writeBoolean(header.sendByFollower);
        }

        @Override
        public LogStreamHeader deserialize(DataInputPlus in, Version version) throws IOException
        {
            LogStreamManifest manifest = LogStreamManifest.serializer.deserialize(in, version);
            ReconciledLogSnapshot reconciled = ReconciledLogSnapshot.serializer.deserialize(in, version);
            InetAddressAndPort sender = inetAddressAndPortSerializer.deserialize(in, version.messagingVersion());
            TimeUUID planId = TimeUUID.deserialize(in);
            int sessionIndex = in.readInt();
            boolean sendByFollower = in.readBoolean();
            return new LogStreamHeader(manifest, reconciled, sender, planId, sessionIndex, sendByFollower);
        }

        @Override
        public long serializedSize(LogStreamHeader header, Version version)
        {
            return LogStreamManifest.serializer.serializedSize(header.manifest, version)
                   + ReconciledLogSnapshot.serializer.serializedSize(header.reconciled, version)
                   + inetAddressAndPortSerializer.serializedSize(header.sender, version.messagingVersion())
                   + TimeUUID.sizeInBytes()
                   + TypeSizes.sizeof(header.sessionIndex)
                   + TypeSizes.sizeof(header.sendByFollower);
        }
    };

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        LogStreamHeader that = (LogStreamHeader) o;
        return sessionIndex == that.sessionIndex &&
               sendByFollower == that.sendByFollower &&
               Objects.equals(manifest, that.manifest) &&
               Objects.equals(sender, that.sender) &&
               Objects.equals(planId, that.planId) &&
               Objects.equals(reconciled, that.reconciled);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(manifest, sender, planId, sessionIndex, sendByFollower, reconciled);
    }

    @Override
    public String toString()
    {
        return String.format("LogStreamHeader{manifest=%s, sender=%s, planId=%s, sessionIndex=%d, sendByFollower=%s, reconciledOffsets=%s}",
                             manifest, sender, planId, sessionIndex, sendByFollower, reconciled);
    }
}