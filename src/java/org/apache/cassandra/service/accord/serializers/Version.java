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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.MessageVersionProvider;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

public enum Version implements MessageVersionProvider
{
    // If MessagingService version bumps, this mapping does not need to be updated; only updates needed are those that
    // include accord serializer changes.
    V1(1, MessagingService.Version.VERSION_51);

    public static final Version LATEST = Version.V1;
    /**
     * Version that should be used for disk serialization where downgrade may be possible.
     *
     * As of this writing only 1 version exists, so this is the same as LATEST... Once v2 comes into the picture we need this version to be the oldest version needed for downgrade...  If you upgrade from 5.1 to 5.2 (assuming this adds a v2) you need a version that works with 5.1 here.
     */
    public static final Version DOWNGRADE_SAFE_VERSION = Version.V1;

    /**
     * Version that should be used for messaging serialization where mixed versions may be possible.
     *
     * As of this writing only 1 version exists, so this is the same as LATEST... Once v2 comes into the picture we need this version to be the oldest version needed for downgrade...  If you upgrade from 5.1 to 5.2 (assuming this adds a v2) you need a version that works with 5.1 here.
     */
    public static final Version CLUSTER_SAFE_VERSION = Version.V1;

    /**
     * Version number used in the serialization protocol.  This is not the same as the messaging version, and is localized to this class.
     */
    public final int version;
    /**
     * For the accord versioned serializers they sometimes need to access existing messaging serializers, in these cases an agreed messaging version is required and can not be plumbed directly from the messaging layer.
     *
     * @see #messageVersion()
     */
    private final MessagingService.Version messagingVersion;

    Version(int version, MessagingService.Version messagingVersion)
    {
        this.version = version;
        this.messagingVersion = messagingVersion;
    }

    public static Version fromVersion(int version)
    {
        switch (version)
        {
            case 1: return V1;
            default:
                throw new IllegalArgumentException("Unknown version: " + version);
        }
    }

    public static Version findBestMatchForMessagingVersion(int messagingVersion)
    {
        Version[] versions = values();
        for (int i = versions.length - 1; i >= 0; i--)
        {
            Version v = versions[i];
            // If network version bumped (12 to 13), the accord serializers may not have been changed; use the largest
            // version smaller than or equal to this version
            if (v.messageVersion() <= messagingVersion)
                return v;
        }
        throw new IllegalArgumentException("Attempted to use message version " + messagingVersion + " which is smaller than " + versions[0] + " can handle (" + versions[0].messageVersion() + ")");
    }

    @Override
    public int messageVersion()
    {
        return messagingVersion.value;
    }

    public List<Version> greaterThanOrEqual()
    {
        Version[] all = Version.values();
        if (ordinal() == all.length - 1)
            return Collections.singletonList(this);
        List<Version> values = new ArrayList<>(all.length - ordinal());
        for (int i = ordinal(); i < all.length; i++)
            values.add(all[i]);
        return values;
    }

    public enum Serializer implements UnversionedSerializer<Version>
    {
        instance;

        @Override
        public void serialize(Version t, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(t.version);
        }

        @Override
        public Version deserialize(DataInputPlus in) throws IOException
        {
            return Version.fromVersion(in.readUnsignedVInt32());
        }

        @Override
        public long serializedSize(Version t)
        {
            return TypeSizes.sizeofUnsignedVInt(t.version);
        }
    }
}
