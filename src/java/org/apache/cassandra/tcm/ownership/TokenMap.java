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

package org.apache.cassandra.tcm.ownership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.compatibility.AsTokenMap;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.BiMultiValMap;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SortedBiMultiValMap;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public class TokenMap implements MetadataValue<TokenMap>, AsTokenMap
{
    public static final Serializer serializer = new Serializer();

    private static final Logger logger = LoggerFactory.getLogger(TokenMap.class);

    private final SortedBiMultiValMap<Token, NodeId> map;
    private final List<Token> tokens;
    private final List<Range<Token>> ranges;
    // TODO: move partitioner to the users (SimpleStrategy and Uniform Range Placement?)
    private final IPartitioner partitioner;
    private final Epoch lastModified;

    public TokenMap(IPartitioner partitioner)
    {
        this(Epoch.EMPTY, partitioner, SortedBiMultiValMap.create());
    }

    private TokenMap(Epoch lastModified, IPartitioner partitioner, SortedBiMultiValMap<Token, NodeId> map)
    {
        this.lastModified = lastModified;
        this.partitioner = partitioner;
        this.map = map;
        this.tokens = tokens();
        this.ranges = toRanges(tokens, partitioner);
    }

    @Override
    public TokenMap withLastModified(Epoch epoch)
    {
        return new TokenMap(epoch, partitioner, map);
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    public TokenMap assignTokens(NodeId id, Collection<Token> tokens)
    {
        SortedBiMultiValMap<Token, NodeId> finalisedCopy = SortedBiMultiValMap.create(map);
        tokens.forEach(t -> finalisedCopy.putIfAbsent(t, id));
        return new TokenMap(lastModified, partitioner, finalisedCopy);
    }

    /**
     * Find a token that is immediately after this one in the ring.
     */
    public Token nextInRing(Token token, boolean exactMatchOnly)
    {
        int idx = Collections.binarySearch(tokens, token);
        if (idx < 0)
        {
            if (exactMatchOnly)
                throw new IllegalArgumentException(String.format("Can not find token %s in the ring %s", token, token));
            int realIdx = -1 - idx;
            if (realIdx == tokens.size())
                realIdx = 0;

            return tokens.get(realIdx);
        }

        if (idx == tokens.size() - 1)
            return tokens.get(0);
        else
            return tokens.get(idx + 1);
    }

    public TokenMap unassignTokens(NodeId id)
    {
        SortedBiMultiValMap<Token, NodeId> finalisedCopy = SortedBiMultiValMap.create(map);
        finalisedCopy.removeValue(id);
        return new TokenMap(lastModified, partitioner, finalisedCopy);
    }

    public TokenMap unassignTokens(NodeId id, Collection<Token> tokens)
    {
        SortedBiMultiValMap<Token, NodeId> finalisedCopy = SortedBiMultiValMap.create(map);
        for (Token token : tokens)
        {
            NodeId nodeId = finalisedCopy.remove(token);
            assert nodeId.equals(id);
        }

        return new TokenMap(lastModified, partitioner, finalisedCopy);
    }

    public BiMultiValMap<Token, NodeId> asMap()
    {
        return SortedBiMultiValMap.create(map);
    }

    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    public IPartitioner partitioner()
    {
        return partitioner;
    }

    public ImmutableList<Token> tokens()
    {
        return ImmutableList.copyOf(map.keySet());
    }

    public ImmutableList<Token> tokens(NodeId nodeId)
    {
        Collection<Token> tokens = map.inverse().get(nodeId);
        if (tokens == null)
            return null;
        return ImmutableList.copyOf(tokens);
    }

    public List<Range<Token>> toRanges()
    {
        return ranges;
    }

    public static List<Range<Token>> toRanges(List<Token> tokens, IPartitioner partitioner)
    {
        if (tokens.isEmpty())
            return Collections.emptyList();

        List<Range<Token>> ranges = new ArrayList<>(tokens.size() + 1);
        maybeAdd(ranges, new Range<>(partitioner.getMinimumToken(), tokens.get(0)));
        for (int i = 1; i < tokens.size(); i++)
            maybeAdd(ranges, new Range<>(tokens.get(i - 1), tokens.get(i)));
        maybeAdd(ranges, new Range<>(tokens.get(tokens.size() - 1), partitioner.getMinimumToken()));
        if (ranges.isEmpty())
            ranges.add(new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken()));
        return ranges;
    }

    private static void maybeAdd(List<Range<Token>> ranges, Range<Token> r)
    {
        if (r.left.compareTo(r.right) != 0)
            ranges.add(r);
    }

    public Token nextToken(List<Token> tokens, Token token)
    {
       return tokens.get(nextTokenIndex(tokens, token));
    }

    //Duplicated from TokenMetadata::firstTokenIndex
    public static int nextTokenIndex(final List<Token> ring, Token start)
    {
        assert ring.size() > 0;
        int i = Collections.binarySearch(ring, start);
        if (i < 0)
        {
            i = (i + 1) * (-1);
            if (i >= ring.size())
                i = 0;
        }
        return i;
    }

    public NodeId owner(Token token)
    {
        return map.get(token);
    }

    public String toString()
    {
        return "TokenMap{" +
               toDebugString()
               + '}';
    }

    public void logDebugString()
    {
        logger.info(toDebugString());
    }

    public String toDebugString()
    {
        StringBuilder b = new StringBuilder();
        for (Map.Entry<Token, NodeId> entry : map.entrySet())
            b.append('[').append(entry.getKey()).append("] => ").append(entry.getValue().uuid).append(";\n");
        return b.toString();
    }

    public static class Serializer implements MetadataSerializer<TokenMap>
    {
        public void serialize(TokenMap t, DataOutputPlus out, Version version) throws IOException
        {
            Epoch.serializer.serialize(t.lastModified, out, version);
            out.writeUTF(t.partitioner.getClass().getCanonicalName());
            out.writeInt(t.map.size());
            for (Map.Entry<Token, NodeId> entry : t.map.entrySet())
            {
                Token.metadataSerializer.serialize(entry.getKey(), out, version);
                NodeId.serializer.serialize(entry.getValue(), out, version);
            }
        }

        public TokenMap deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            IPartitioner partitioner = FBUtilities.newPartitioner(in.readUTF());
            int size = in.readInt();
            SortedBiMultiValMap<Token, NodeId> tokens = SortedBiMultiValMap.create();
            for (int i = 0; i < size; i++)
                tokens.put(Token.metadataSerializer.deserialize(in, version),
                           NodeId.serializer.deserialize(in, version));
            return new TokenMap(lastModified, partitioner, tokens);
        }

        public long serializedSize(TokenMap t, Version version)
        {
            long size = Epoch.serializer.serializedSize(t.lastModified, version);
            size += sizeof(t.partitioner.getClass().getCanonicalName());
            size += sizeof(t.map.size());
            for (Map.Entry<Token, NodeId> entry : t.map.entrySet())
            {
                size += Token.metadataSerializer.serializedSize(entry.getKey(), version);
                size += NodeId.serializer.serializedSize(entry.getValue(), version);
            }
            return size;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof TokenMap)) return false;
        TokenMap tokenMap = (TokenMap) o;
        return Objects.equals(lastModified, tokenMap.lastModified) &&
               isEquivalent(tokenMap);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lastModified, map, partitioner);
    }

    /**
     * returns true if this token map is functionally equivalent to the given one
     *
     * does not check equality of lastModified
     */
    public boolean isEquivalent(TokenMap tokenMap)
    {
        return Objects.equals(map, tokenMap.map) &&
               Objects.equals(partitioner, tokenMap.partitioner);
    }
}
