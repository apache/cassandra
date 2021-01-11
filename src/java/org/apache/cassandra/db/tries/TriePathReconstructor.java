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

package org.apache.cassandra.db.tries;

import java.util.Arrays;

import org.agrona.DirectBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class TriePathReconstructor implements Trie.ResettingTransitionsReceiver
{
    protected byte[] keyBytes = new byte[32];
    protected int keyPos = 0;

    public void addPathByte(int nextByte)
    {
        if (keyPos >= keyBytes.length)
            keyBytes = Arrays.copyOf(keyBytes, keyPos * 2);
        keyBytes[keyPos++] = (byte) nextByte;
    }

    public void addPathBytes(DirectBuffer buffer, int pos, int count)
    {
        int newPos = keyPos + count;
        if (newPos > keyBytes.length)
            keyBytes = Arrays.copyOf(keyBytes, Math.max(newPos + 16, keyBytes.length * 2));
        buffer.getBytes(pos, keyBytes, keyPos, count);
        keyPos = newPos;
    }

    public void resetPathLength(int newLength)
    {
        keyPos = newLength;
    }

    static ByteComparable toByteComparable(byte[] bytes, int byteLength)
    {
        return ByteComparable.fixedLength(Arrays.copyOf(bytes, byteLength));
    }
}
