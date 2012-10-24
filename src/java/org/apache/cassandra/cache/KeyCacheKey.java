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
package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class KeyCacheKey implements CacheKey
{
    public final Descriptor desc;

    // keeping an array instead of a ByteBuffer lowers the overhead of the key cache working set,
    // without extra copies on lookup since client-provided key ByteBuffers will be array-backed already
    public final byte[] key;

    public KeyCacheKey(Descriptor desc, ByteBuffer key)
    {
        this.desc = desc;
        this.key = ByteBufferUtil.getArray(key);
        assert this.key != null;
    }

    public Pair<String, String> getPathInfo()
    {
        return Pair.create(desc.ksname, desc.cfname);
    }

    public String toString()
    {
        return String.format("KeyCacheKey(%s, %s)", desc, ByteBufferUtil.bytesToHex(ByteBuffer.wrap(key)));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyCacheKey that = (KeyCacheKey) o;

        if (desc != null ? !desc.equals(that.desc) : that.desc != null) return false;
        return Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode()
    {
        int result = desc != null ? desc.hashCode() : 0;
        result = 31 * result + (key != null ? Arrays.hashCode(key) : 0);
        return result;
    }
}
