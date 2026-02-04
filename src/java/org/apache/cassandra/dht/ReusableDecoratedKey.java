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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class ReusableDecoratedKey extends BufferDecoratedKey
{
    protected int keyLength = 0;
    protected byte[] keyBytes;

    public ReusableDecoratedKey(Token token, int initialSize)
    {
        super(token, ByteBuffer.wrap(new byte[initialSize]).limit(0));
        keyBytes = key.array();
    }

    abstract void recalculateToken();

    public void copyKey(ByteBuffer newKey)
    {
        int length = newKey.remaining();
        maybeResizeKey(length);
        ByteBufferUtil.copyBytes(newKey, newKey.position(), key, 0, length);
        keyLength = length;
        key.limit(length);
        recalculateToken();
    }

    /** WARNING: retains ref to external buffer */
    public void shadowKey(ByteBuffer newKey, byte[] newKeyBytes, int newKeyLength)
    {
        key = newKey;
        keyBytes = newKeyBytes;
        keyLength = newKeyLength;
        recalculateToken();
    }

    /** WARNING: retains ref to external buffer */
    public void shadowKey(int newKeyLength)
    {
        keyLength = newKeyLength;
        recalculateToken();
    }

    @Override
    public int getKeyLength()
    {
        return keyLength;
    }

    public byte[] keyBytes()
    {
        return keyBytes;
    }

    public void reset()
    {
        keyLength = 0;
        key.limit(0);
        recalculateToken();
    }

    private void maybeResizeKey(int length)
    {
        int capacity = keyBytes.length;
        if (capacity > length)
            return;
        keyBytes = new byte[Math.max(length, capacity * 2)];
        key = ByteBuffer.wrap(keyBytes);
    }
}
