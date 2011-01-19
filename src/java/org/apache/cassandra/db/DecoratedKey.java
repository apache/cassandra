/**
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
public class DecoratedKey<T extends Token> implements Comparable<DecoratedKey>
{
    private static IPartitioner partitioner = StorageService.getPartitioner();

    public static final Comparator<DecoratedKey> comparator = new Comparator<DecoratedKey>()
    {
        public int compare(DecoratedKey o1, DecoratedKey o2)
        {
            return o1.compareTo(o2);
        }
    };

    public final T token;
    public final ByteBuffer key;

    public DecoratedKey(T token, ByteBuffer key)
    {
        super();
        assert token != null;
        this.token = token;
        this.key = key;
    }

    @Override
    public int hashCode()
    {
        return token.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        DecoratedKey other = (DecoratedKey) obj;
        return token.equals(other.token);
    }

    public int compareTo(DecoratedKey other)
    {
        return token.compareTo(other.token);
    }

    public boolean isEmpty()
    {
        return token.equals(partitioner.getMinimumToken());
    }

    @Override
    public String toString()
    {
        String keystring = key == null ? "null" : ByteBufferUtil.bytesToHex(key);
        return "DecoratedKey(" + token + ", " + keystring + ")";
    }
}
