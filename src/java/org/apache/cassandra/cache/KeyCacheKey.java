/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cache;

import java.nio.ByteBuffer;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.Pair;

public class KeyCacheKey extends Pair<Descriptor, ByteBuffer> implements CacheKey
{
    public KeyCacheKey(Descriptor desc, ByteBuffer key)
    {
        super(desc, key);
    }

    public ByteBuffer serializeForStorage()
    {
        ByteBuffer bytes = ByteBuffer.allocate(serializedSize());

        bytes.put(right.slice());
        bytes.rewind();

        return bytes;
    }

    public Pair<String, String> getPathInfo()
    {
        return new Pair<String, String>(left.ksname, left.cfname);
    }

    public int serializedSize()
    {
        return right.remaining();
    }

    public String toString()
    {
        return String.format("KeyCacheKey(descriptor:%s, key:%s)", left, right);
    }
}
