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
import java.util.List;
import java.util.StringJoiner;
import java.util.function.BiFunction;

import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.KeyBound;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.IFilter.FilterKey;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.cassandra.utils.memory.HeapCloner;

/**
 * Represents a decorated key, handy for certain operations
 * where just working with strings gets slow.
 *
 * We do a lot of sorting of DecoratedKeys, so for speed, we assume that tokens correspond one-to-one with keys.
 * This is not quite correct in the case of RandomPartitioner (which uses MD5 to hash keys to tokens);
 * if this matters, you can subclass RP to use a stronger hash, or use a non-lossy tokenization scheme (as in the
 * OrderPreservingPartitioner classes).
 */
public abstract class DecoratedKey implements PartitionPosition, FilterKey
{
    public static final Comparator<DecoratedKey> comparator = DecoratedKey::compareTo;

    private final Token token;

    public DecoratedKey(Token token)
    {
        assert token != null;
        this.token = token;
    }

    @Override
    public int hashCode()
    {
        return getKey().hashCode(); // hash of key is enough
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null || !(obj instanceof DecoratedKey))
            return false;

        DecoratedKey other = (DecoratedKey)obj;
        return ByteBufferUtil.compareUnsigned(getKey(), other.getKey()) == 0; // we compare faster than BB.equals for array backed BB
    }

    public int compareTo(PartitionPosition pos)
    {
        if (this == pos)
            return 0;

        // delegate to Token.KeyBound if needed
        if (!(pos instanceof DecoratedKey))
            return -pos.compareTo(this);

        DecoratedKey otherKey = (DecoratedKey) pos;
        int cmp = getToken().compareTo(otherKey.getToken());
        return cmp == 0 ? ByteBufferUtil.compareUnsigned(getKey(), otherKey.getKey()) : cmp;
    }

    public static int compareTo(IPartitioner partitioner, ByteBuffer key, PartitionPosition position)
    {
        // delegate to Token.KeyBound if needed
        if (!(position instanceof DecoratedKey))
            return -position.compareTo(partitioner.decorateKey(key));

        DecoratedKey otherKey = (DecoratedKey) position;
        int cmp = partitioner.getToken(key).compareTo(otherKey.getToken());
        return cmp == 0 ? ByteBufferUtil.compareUnsigned(key, otherKey.getKey()) : cmp;
    }

    @Override
    public ByteSource asComparableBytes(Version version)
    {
        // Note: In the legacy version one encoding could be a prefix of another as the escaping is only weakly
        // prefix-free (see ByteSourceTest.testDecoratedKeyPrefixes()).
        // The OSS50 version avoids this by adding a terminator.
        return ByteSource.withTerminatorMaybeLegacy(version,
                                                    ByteSource.END_OF_STREAM,
                                                    token.asComparableBytes(version),
                                                    keyComparableBytes(version));
    }

    @Override
    public ByteComparable asComparableBound(boolean before)
    {
        return version ->
        {
            assert (version != Version.LEGACY) : "Decorated key bounds are not supported by the legacy encoding.";

            return ByteSource.withTerminator(
                    before ? ByteSource.LT_NEXT_COMPONENT : ByteSource.GT_NEXT_COMPONENT,
                    token.asComparableBytes(version),
                    keyComparableBytes(version));
        };
    }

    protected ByteSource keyComparableBytes(Version version)
    {
        return ByteSource.of(getKey(), version);
    }

    public IPartitioner getPartitioner()
    {
        return getToken().getPartitioner();
    }

    public KeyBound minValue()
    {
        return getPartitioner().getMinimumToken().minKeyBound();
    }

    public boolean isMinimum()
    {
        // A DecoratedKey can never be the minimum position on the ring
        return false;
    }

    public PartitionPosition.Kind kind()
    {
        return PartitionPosition.Kind.ROW_KEY;
    }

    @Override
    public String toString()
    {
        String keystring = getKey() == null ? "null" : ByteBufferUtil.bytesToHex(getKey());
        return "DecoratedKey(" + getToken() + ", " + keystring + ")";
    }

    /**
     * Returns a CQL representation of this key.
     *
     * @param metadata the metadata of the table that this key belogs to
     * @return a CQL representation of this key
     */
    public String toCQLString(TableMetadata metadata)
    {
        List<ColumnMetadata> columns = metadata.partitionKeyColumns();

        if (columns.size() == 1)
            return toCQLString(columns.get(0), getKey());

        ByteBuffer[] values = ((CompositeType) metadata.partitionKeyType).split(getKey());
        StringJoiner joiner = new StringJoiner(" AND ");

        for (int i = 0; i < columns.size(); i++)
            joiner.add(toCQLString(columns.get(i), values[i]));

        return joiner.toString();
    }

    private static String toCQLString(ColumnMetadata metadata, ByteBuffer key)
    {
        return String.format("%s = %s", metadata.name.toCQLString(), metadata.type.toCQLString(key));
    }

    public Token getToken()
    {
        return token;
    }

    public abstract ByteBuffer getKey();
    public abstract int getKeyLength();

    /**
     * If this key occupies only part of a larger buffer, allocate a new buffer that is only as large as necessary.
     * Otherwise, it returns this key.
     */
    public DecoratedKey retainable()
    {
        return ByteBufferUtil.canMinimize(getKey())
               ? new BufferDecoratedKey(getToken(), HeapCloner.instance.clone(getKey()))
               : this;
    }

    public void filterHash(long[] dest)
    {
        ByteBuffer key = getKey();
        MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0, dest);
    }

    /**
     * A template factory method for creating decorated keys from their byte-comparable representation.
     */
    static <T extends DecoratedKey> T fromByteComparable(ByteComparable byteComparable,
                                                         Version version,
                                                         IPartitioner partitioner,
                                                         BiFunction<Token, byte[], T> decoratedKeyFactory)
    {
        ByteSource.Peekable peekable = ByteSource.peekable(byteComparable.asComparableBytes(version));
        // Decode the token from the first component of the multi-component sequence representing the whole decorated key.
        Token token = partitioner.getTokenFactory().fromComparableBytes(ByteSourceInverse.nextComponentSource(peekable), version);
        // Decode the key bytes from the second component.
        byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable));
        // Consume the terminator byte.
        int terminator = peekable.next();
        assert terminator == ByteSource.TERMINATOR : "Decorated key encoding must end in terminator.";
        // Instantiate a decorated key from the decoded token and key bytes, using the provided factory method.
        return decoratedKeyFactory.apply(token, keyBytes);
    }

    public static byte[] keyFromByteSource(ByteSource.Peekable peekableByteSource,
                                           Version version,
                                           IPartitioner partitioner)
    {
        assert version != Version.LEGACY;   // reverse translation is not supported for LEGACY version.
        // Decode the token from the first component of the multi-component sequence representing the whole decorated key.
        // We won't use it, but the decoding also positions the byte source after it.
        partitioner.getTokenFactory().fromComparableBytes(ByteSourceInverse.nextComponentSource(peekableByteSource), version);
        // Decode the key bytes from the second component.
        byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekableByteSource));
        int terminator = peekableByteSource.next();
        assert terminator == ByteSource.TERMINATOR : "Decorated key encoding must end in terminator.";
        return keyBytes;
    }
}