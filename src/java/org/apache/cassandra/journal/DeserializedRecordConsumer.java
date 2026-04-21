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

package org.apache.cassandra.journal;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.DataInputBuffer;

public abstract class DeserializedRecordConsumer<K, V> implements RecordConsumer<K>
{
    final ValueSerializer<K, V> valueSerializer;

    public DeserializedRecordConsumer(ValueSerializer<K, V> valueSerializer)
    {
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void accept(long segment, int position, K key, ByteBuffer buffer, int userVersion)
    {
        try (DataInputBuffer in = new DataInputBuffer(buffer, false))
        {
            V value = valueSerializer.deserialize(key, in, userVersion);
            accept(segment, position, key, value);
        }
        catch (IOException e)
        {
            // can only throw if serializer is buggy
            throw new RuntimeException(e);
        }
    }

    protected abstract void accept(long segment, int position, K key, V value);
}
