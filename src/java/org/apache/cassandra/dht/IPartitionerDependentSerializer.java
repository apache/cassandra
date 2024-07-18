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
package org.apache.cassandra.dht;

import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;

/**
 * Versioned serializer where the serialization depends on partitioner.
 *
 * On serialization the partitioner is given by the entity being serialized. To deserialize the partitioner used must
 * be known to the calling method.
 */
public interface IPartitionerDependentSerializer<T> extends IVersionedSerializer<T>
{
    /**
     * Deserialize into the specified DataInputStream instance.
     * @param in DataInput from which deserialization needs to happen.
     * @param p Partitioner that will be used to construct tokens. Needs to match the partitioner that was used to
     *     serialize the token.
     * @param version protocol version
     * @return the type that was deserialized
     * @throws IOException if deserialization fails
     */
    public T deserialize(DataInputPlus in, IPartitioner p, int version) throws IOException;

    default T deserialize(DataInputPlus in, int version) throws IOException
    {
        return deserialize(in, DatabaseDescriptor.getPartitioner(), version);
    }
}
