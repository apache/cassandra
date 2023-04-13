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

package org.apache.cassandra.index.sai.disk.v1.trie;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.cassandra.io.tries.ValueIterator;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.SizedInts;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader.NOT_FOUND;

public class TrieTermsIterator extends ValueIterator<TrieTermsIterator> implements Iterator<Pair<ByteComparable, Long>>
{
    Pair<ByteComparable, Long> next = null;

    public TrieTermsIterator(Rebufferer rebufferer, long root)
    {
        super(rebufferer, root, true);
    }

    @Override
    public boolean hasNext()
    {
        if (next != null)
            return true;

        if (peekNode() == NOT_FOUND)
            return false;

        next = Pair.create(nextCollectedValue(), getCurrentPayload());

        nextPayloadedNode();

        return true;
    }

    @Override
    public Pair<ByteComparable, Long> next()
    {
        if (!hasNext())
            throw new NoSuchElementException();

        Pair<ByteComparable, Long> result = next;
        next = null;
        return result;
    }

    private long getCurrentPayload()
    {
        return getPayload(buf, payloadPosition(), payloadFlags());
    }

    private long getPayload(ByteBuffer contents, int payloadPos, int bytes)
    {
        if (bytes == 0)
        {
            return NOT_FOUND;
        }
        return SizedInts.read(contents, payloadPos, bytes);
    }
}
