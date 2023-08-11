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

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public abstract class Token implements RingPosition<Token>, Serializable
{
    private static final long serialVersionUID = 1L;

    public static final TokenSerializer serializer = new TokenSerializer();

    public static abstract class TokenFactory
    {
        public abstract ByteBuffer toByteArray(Token token);
        public abstract Token fromByteArray(ByteBuffer bytes);

        /**
         * Produce a byte-comparable representation of the token.
         * See {@link Token#asComparableBytes}
         */
        public ByteSource asComparableBytes(Token token, ByteComparable.Version version)
        {
            return token.asComparableBytes(version);
        }

        /**
         * Translates the given byte-comparable representation to a token instance. If the given bytes don't correspond
         * to the encoding of an instance of the expected token type, an {@link IllegalArgumentException} may be thrown.
         *
         * @param comparableBytes A byte-comparable representation (presumably of a token of some expected token type).
         * @return A new {@link Token} instance, corresponding to the given byte-ordered representation. If we were
         * to call {@link #asComparableBytes(ByteComparable.Version)} on the returned object, we should get a
         * {@link ByteSource} equal to the input one as a result.
         * @throws IllegalArgumentException if the bytes do not encode a valid token.
         */
        public abstract Token fromComparableBytes(ByteSource.Peekable comparableBytes, ByteComparable.Version version);

        public abstract String toString(Token token); // serialize as string, not necessarily human-readable
        public abstract Token fromString(String string); // deserialize

        public abstract void validate(String token) throws ConfigurationException;

        public void serialize(Token token, DataOutputPlus out) throws IOException
        {
            out.write(toByteArray(token));
        }

        public void serialize(Token token, ByteBuffer out) throws IOException
        {
            out.put(toByteArray(token));
        }

        public Token fromByteBuffer(ByteBuffer bytes, int position, int length)
        {
            bytes = bytes.duplicate();
            bytes.position(position)
                 .limit(position + length);
            return fromByteArray(bytes);
        }

        public int byteSize(Token token)
        {
            return toByteArray(token).remaining();
        }
    }

    public static class TokenSerializer implements IPartitionerDependentSerializer<Token>
    {
        public void serialize(Token token, DataOutputPlus out, int version) throws IOException
        {
            IPartitioner p = token.getPartitioner();
            out.writeInt(p.getTokenFactory().byteSize(token));
            p.getTokenFactory().serialize(token, out);
        }

        public Token deserialize(DataInput in, IPartitioner p, int version) throws IOException
        {
            int size = deserializeSize(in);
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            return p.getTokenFactory().fromByteArray(ByteBuffer.wrap(bytes));
        }

        public int deserializeSize(DataInput in) throws IOException
        {
            return in.readInt();
        }

        public long serializedSize(Token object, int version)
        {
            IPartitioner p = object.getPartitioner();
            int byteSize = p.getTokenFactory().byteSize(object);
            return TypeSizes.sizeof(byteSize) + byteSize;
        }
    }

    abstract public IPartitioner getPartitioner();
    abstract public long getHeapSize();
    abstract public Object getTokenValue();

    /**
     * This method exists so that callers can access the primitive {@code long} value for this {@link Token}, if
     * one exits. It is especially useful when the auto-boxing induced by a call to {@link #getTokenValue()} would
     * be unacceptable for reasons of performance.
     *
     * @return the primitive {@code long} value of this token, if one exists
     *
     * @throws UnsupportedOperationException if this {@link Token} is not backed by a primitive {@code long} value
     */
    public long getLongValue()
    {
        throw new UnsupportedOperationException();
    }


    /**
     * Produce a weakly prefix-free byte-comparable representation of the token, i.e. such a sequence of bytes that any
     * pair x, y of valid tokens of this type and any bytes b1, b2 between 0x10 and 0xEF,
     * (+ stands for concatenation)
     *   compare(x, y) == compareLexicographicallyUnsigned(asByteComparable(x)+b1, asByteComparable(y)+b2)
     * (i.e. the values compare like the original type, and an added 0x10-0xEF byte at the end does not change that) and:
     *   asByteComparable(x)+b1 is not a prefix of asByteComparable(y)      (weakly prefix free)
     * (i.e. a valid representation of a value may be a prefix of another valid representation of a value only if the
     * following byte in the latter is smaller than 0x10 or larger than 0xEF). These properties are trivially true if
     * the encoding compares correctly and is prefix free, but also permits a little more freedom that enables somewhat
     * more efficient encoding of arbitrary-length byte-comparable blobs.
     */
    abstract public ByteSource asComparableBytes(ByteComparable.Version version);

    /**
     * Returns a measure for the token space covered between this token and next.
     * Used by the token allocation algorithm (see CASSANDRA-7032).
     */
    abstract public double size(Token next);
    /**
     * Returns the next possible token in the token space, one that compares
     * greater than this and such that there is no other token that sits
     * between this token and it in the token order.
     *
     * This is not possible for all token types, esp. for comparison-based
     * tokens such as the LocalPartioner used for classic secondary indexes.
     *
     * Used to avoid clashes between nodes in separate datacentres trying to
     * use the same token via the token allocation algorithm, as well as in
     * constructing token ranges for sstables.
     */
    abstract public Token nextValidToken();

    public Token getToken()
    {
        return this;
    }

    public Token minValue()
    {
        return getPartitioner().getMinimumToken();
    }

    public boolean isMinimum()
    {
        return this.equals(minValue());
    }

    /*
     * A token corresponds to the range of all the keys having this token.
     * A token is thus not comparable directly to a key. But to be able to select
     * keys given tokens, we introduce two "fake" keys for each token T:
     *   - lowerBoundKey: a "fake" key representing the lower bound T represents.
     *                    In other words, lowerBoundKey is the smallest key that
     *                    have token T.
     *   - upperBoundKey: a "fake" key representing the upper bound T represents.
     *                    In other words, upperBoundKey is the largest key that
     *                    have token T.
     *
     * Note that those are "fake" keys and should only be used for comparison
     * of other keys, for selection of keys when only a token is known.
     */
    public KeyBound minKeyBound()
    {
        return new KeyBound(this, true);
    }

    public KeyBound maxKeyBound()
    {
        /*
         * For each token, we needs both minKeyBound and maxKeyBound
         * because a token corresponds to a range of keys. But the minimun
         * token corresponds to no key, so it is valid and actually much
         * simpler to associate the same value for minKeyBound and
         * maxKeyBound for the minimun token.
         */
        if (isMinimum())
            return minKeyBound();
        return new KeyBound(this, false);
    }

    public static class KeyBound implements PartitionPosition
    {
        private final Token token;
        public final boolean isMinimumBound;

        private KeyBound(Token t, boolean isMinimumBound)
        {
            this.token = t;
            this.isMinimumBound = isMinimumBound;
        }

        public Token getToken()
        {
            return token;
        }

        public int compareTo(PartitionPosition pos)
        {
            if (this == pos)
                return 0;

            int cmp = getToken().compareTo(pos.getToken());
            if (cmp != 0)
                return cmp;

            if (isMinimumBound)
                return ((pos instanceof KeyBound) && ((KeyBound)pos).isMinimumBound) ? 0 : -1;
            else
                return ((pos instanceof KeyBound) && !((KeyBound)pos).isMinimumBound) ? 0 : 1;
        }

        @Override
        public ByteSource asComparableBytes(Version version)
        {
            int terminator = isMinimumBound ? ByteSource.LT_NEXT_COMPONENT : ByteSource.GT_NEXT_COMPONENT;
            return ByteSource.withTerminator(terminator, token.asComparableBytes(version));
        }

        @Override
        public ByteComparable asComparableBound(boolean before)
        {
            // This class is already a bound thus nothing needs to be changed from its representation
            return this;
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
            return getToken().isMinimum();
        }

        public PartitionPosition.Kind kind()
        {
            return isMinimumBound ? PartitionPosition.Kind.MIN_BOUND : PartitionPosition.Kind.MAX_BOUND;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || this.getClass() != obj.getClass())
                return false;

            KeyBound other = (KeyBound)obj;
            return token.equals(other.token) && isMinimumBound == other.isMinimumBound;
        }

        @Override
        public int hashCode()
        {
            return getToken().hashCode() + (isMinimumBound ? 0 : 1);
        }

        @Override
        public String toString()
        {
            return String.format("%s(%s)", isMinimumBound ? "min" : "max", getToken().toString());
        }
    }
}
