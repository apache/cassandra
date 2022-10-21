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

package org.apache.cassandra.io;

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Serializer that stores the version within the buffer.  Normal usage of {@link IVersionedSerializer} is to rely on
 * {@link org.apache.cassandra.net.MessagingService#current_version} and messaging version numbers, but that implies
 * that a messaging version bump is required if a change is made to this field; for some cases the serializer isn't
 * dealing with messages and instead are blobs stored in a table, for these cases it may be better to rely on a field
 * specific versioning that gets stored along the data.
 */
public class LocalVersionedSerializer<I>
{
    private final MessageVersionProvider currentVersion;
    private final IVersionedSerializer<MessageVersionProvider> versionSerializer;
    private final IVersionedSerializer<I> serializer;

    public <V extends MessageVersionProvider> LocalVersionedSerializer(V currentVersion,
                                                                       IVersionedSerializer<V> versionSerializer,
                                                                       IVersionedSerializer<I> serializer)
    {
        // V is local to the constructor to validate at construction time things are fine, but don't want in the type
        // sig of the class as it just gets verbose...
        this.currentVersion = Objects.requireNonNull(currentVersion);
        this.versionSerializer = (IVersionedSerializer<MessageVersionProvider>) Objects.requireNonNull(versionSerializer);
        this.serializer = Objects.requireNonNull(serializer);
    }

    public IVersionedSerializer<I> serializer()
    {
        return serializer;
    }

    /**
     * Serialize the specified type into the specified DataOutputStream instance.
     *
     * @param t   type that needs to be serialized
     * @param out DataOutput into which serialization needs to happen.
     * @throws IOException if serialization fails
     */
    public void serialize(I t, DataOutputPlus out) throws IOException
    {
        versionSerializer.serialize(currentVersion, out, currentVersion.messageVersion());
        serializer.serialize(t, out, currentVersion.messageVersion());
    }

    /**
     * Deserialize into the specified DataInputStream instance.
     *
     * @param in DataInput from which deserialization needs to happen.
     * @return the type that was deserialized
     * @throws IOException if deserialization fails
     */
    public I deserialize(DataInputPlus in) throws IOException
    {
        MessageVersionProvider version = versionSerializer.deserialize(in, currentVersion.messageVersion());
        return serializer.deserialize(in, version.messageVersion());
    }

    /**
     * Calculate serialized size of object without actually serializing.
     *
     * @param t object to calculate serialized size
     * @return serialized size of object t
     */
    public long serializedSize(I t)
    {
        long size = versionSerializer.serializedSize(currentVersion, currentVersion.messageVersion());
        size += serializer.serializedSize(t, currentVersion.messageVersion());
        return size;
    }
}
