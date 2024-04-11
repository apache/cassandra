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

package org.apache.cassandra.tcm.serialization;

import java.io.IOException;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public interface PartitionerAwareMetadataSerializer<T>
{
    /**
     * Serialize the specified type into the specified DataOutputStream instance.
     *
     * @param t type that needs to be serialized
     * @param out DataOutput into which serialization needs to happen.
     * @param partitioner The partitioner to use
     * @param version protocol version
     * @throws IOException if serialization fails
     */
    void serialize(T t, DataOutputPlus out, IPartitioner partitioner, Version version) throws IOException;

    /**
     * Deserialize into the specified DataInputStream instance.
     * @param in DataInput from which deserialization needs to happen.
     * @param partitioner The partitioner to use
     * @param version protocol version
     * @return the type that was deserialized
     * @throws IOException if deserialization fails
     */
    T deserialize(DataInputPlus in, IPartitioner partitioner, Version version) throws IOException;


    /**
     * Calculate serialized size of object without actually serializing.
     * @param t object to calculate serialized size
     * @param partitioner The partitioner to use
     * @param version protocol version
     * @return serialized size of object t
     */
    long serializedSize(T t, IPartitioner partitioner, Version version);
}
