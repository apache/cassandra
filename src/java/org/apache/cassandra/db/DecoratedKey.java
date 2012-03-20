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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Represents a decorated key, handy for certain operations
 * where just working with strings gets slow.
 *
 * We do a lot of sorting of DecoratedKeys, so for speed, we assume that tokens correspond one-to-one with keys.
 * This is not quite correct in the case of RandomPartitioner (which uses MD5 to hash keys to tokens);
 * if this matters, you can subclass RP to use a stronger hash, or use a non-lossy tokenization scheme (as in the
 * OrderPreservingPartitioner classes).
 */
public class DecoratedKey extends RowPosition
{
    private static final IPartitioner partitioner = StorageService.getPartitioner();

    public static final Comparator<DecoratedKey> comparator = new Comparator<DecoratedKey>()
    {
        public int compare(DecoratedKey o1, DecoratedKey o2)
        {
            return o1.compareTo(o2);
        }
    };

    public final Token token;
    public final ByteBuffer key;

    public DecoratedKey(Token token, ByteBuffer key)
    {
        assert token != null && key != null;
        this.token = token;
        this.key = key;
    }

    @Override
    public int hashCode()
    {
        return key.hashCode(); // hash of key is enough
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null || this.getClass() != obj.getClass())
            return false;

        DecoratedKey other = (DecoratedKey)obj;

        return ByteBufferUtil.compareUnsigned(key, other.key) == 0; // we compare faster than BB.equals for array backed BB
    }

    public int compareTo(RowPosition pos)
    {
        if (this == pos)
            return 0;

        // delegate to Token.KeyBound if needed
        if (!(pos instanceof DecoratedKey))
            return -pos.compareTo(this);

        DecoratedKey otherKey = (DecoratedKey) pos;
        int cmp = token.compareTo(otherKey.getToken());
        return cmp == 0 ? ByteBufferUtil.compareUnsigned(key, otherKey.key) : cmp;
    }

    public boolean isMinimum(IPartitioner partitioner)
    {
        // A DecoratedKey can never be the minimum position on the ring
        return false;
    }

    public RowPosition.Kind kind()
    {
        return RowPosition.Kind.ROW_KEY;
    }

    @Override
    public String toString()
    {
        String keystring = key == null ? "null" : ByteBufferUtil.bytesToHex(key);
        return "DecoratedKey(" + token + ", " + keystring + ")";
    }

    public Token getToken()
    {
        return token;
    }
}
