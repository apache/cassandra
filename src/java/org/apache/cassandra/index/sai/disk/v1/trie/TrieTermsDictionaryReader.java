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

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.tries.SerializationNode;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.tries.TrieSerializer;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.SizedInts;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Page-aware random access reader for a trie terms dictionary written by {@link TrieTermsDictionaryWriter}.
 */
@NotThreadSafe
public class TrieTermsDictionaryReader extends Walker<TrieTermsDictionaryReader>
{
    public static final long NOT_FOUND = -1;

    public TrieTermsDictionaryReader(Rebufferer rebufferer, long root)
    {
        super(rebufferer, root);
    }

    public static final TrieSerializer<Long, DataOutputPlus> trieSerializer = new TrieSerializer<>()
    {
        @Override
        public int sizeofNode(SerializationNode<Long> node, long nodePosition)
        {
            return TrieNode.typeFor(node, nodePosition).sizeofNode(node) + sizeof(node.payload());
        }

        @Override
        public void write(DataOutputPlus dest, SerializationNode<Long> node, long nodePosition) throws IOException
        {
            TrieNode type = TrieNode.typeFor(node, nodePosition);
            Long payload = node.payload();
            int payloadBits = sizeof(payload);
            type.serialize(dest, node, payloadBits, nodePosition);

            if (payload != null)
                SizedInts.write(dest, payload, payloadBits);
        }

        private int sizeof(Long payload)
        {
            return payload == null ? 0 : SizedInts.nonZeroSize(payload);
        }
    };

    public long exactMatch(ByteComparable key)
    {
        // Since we are looking for an exact match we are always expecting the follow
        // to return END_OF_STREAM if the key was found.
        return follow(key) == ByteSource.END_OF_STREAM ? getCurrentPayload() : NOT_FOUND;
    }

    private long getCurrentPayload()
    {
        return getPayloadAt(buf, payloadPosition(), payloadFlags());
    }

    private long getPayloadAt(ByteBuffer contents, int payloadPos, int bytes)
    {
        if (bytes == 0)
        {
            return NOT_FOUND;
        }
        return SizedInts.read(contents, payloadPos, bytes);
    }
}
