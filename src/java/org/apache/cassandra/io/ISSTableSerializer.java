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

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.DataOutputPlus;

public interface ISSTableSerializer<T>
{
    /**
     * Serialize the specified type into the specified DataOutputStream
     * instance in the format suited for SSTables.
     *
     * @param t type that needs to be serialized
     * @param out DataOutput into which serialization needs to happen.
     * @throws java.io.IOException
     */
    public void serializeForSSTable(T t, DataOutputPlus out) throws IOException;

    /**
     * Deserialize into the specified DataInputStream instance in the format
     * suited for SSTables.
     * @param in DataInput from which deserialization needs to happen.
     * @param version the version for the sstable we're reading from
     * @throws IOException
     * @return the type that was deserialized
     */
    public T deserializeFromSSTable(DataInput in, Descriptor.Version version) throws IOException;
}
