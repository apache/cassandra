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

package org.apache.cassandra.index.sai.disk.v2.sortedterms;


import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader;
import org.apache.cassandra.io.tries.ReverseValueIterator;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.SizedInts;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

@NotThreadSafe
class ReversedTrieRangeIterator extends ReverseValueIterator<ReversedTrieRangeIterator>
{
    private long currentNode = -1;

    protected ReversedTrieRangeIterator(Rebufferer source, long root, ByteComparable end, boolean admitPrefix)
    {
        super(source, root, ByteComparable.EMPTY, end, admitPrefix);
    }

    /**
     * @return next biggest id that is smaller than given "end" ByteComparable
     */
    public long nextId()
    {
        if (currentNode == -1)
        {
            currentNode = nextPayloadedNode();
            if (currentNode == -1)
                return TrieTermsDictionaryReader.NOT_FOUND;
        }

        go(currentNode);
        long id = getCurrentPayload();

        currentNode = -1;
        return id;
    }

    private long getCurrentPayload()
    {
        return getPayload(buf, payloadPosition(), payloadFlags());
    }

    private long getPayload(ByteBuffer contents, int payloadPos, int bytes)
    {
        if (bytes == 0)
        {
            return TrieTermsDictionaryReader.NOT_FOUND;
        }
        return SizedInts.read(contents, payloadPos, bytes);
    }
}