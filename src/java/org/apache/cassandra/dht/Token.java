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
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class Token implements RingPosition<Token>, Serializable
{
    private static final long serialVersionUID = 1L;

    public static final TokenSerializer serializer = new TokenSerializer();

    public static abstract class TokenFactory
    {
        public abstract ByteBuffer toByteArray(Token token);
        public abstract Token fromByteArray(ByteBuffer bytes);
        public abstract String toString(Token token); // serialize as string, not necessarily human-readable
        public abstract Token fromString(String string); // deserialize

        public abstract void validate(String token) throws ConfigurationException;
    }

    public static class TokenSerializer implements IPartitionerDependentSerializer<Token>
    {
        public void serialize(Token token, DataOutputPlus out, int version) throws IOException
        {
            IPartitioner p = token.getPartitioner();
            ByteBuffer b = p.getTokenFactory().toByteArray(token);
            ByteBufferUtil.writeWithLength(b, out);
        }

        public Token deserialize(DataInput in, IPartitioner p, int version) throws IOException
        {
            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            return p.getTokenFactory().fromByteArray(ByteBuffer.wrap(bytes));
        }

        public long serializedSize(Token object, int version)
        {
            IPartitioner p = object.getPartitioner();
            ByteBuffer b = p.getTokenFactory().toByteArray(object);
            return TypeSizes.sizeof(b.remaining()) + b.remaining();
        }
    }

    abstract public IPartitioner getPartitioner();
    abstract public long getHeapSize();
    abstract public Object getTokenValue();

    /**
     * Returns a measure for the token space covered between this token and next.
     * Used by the token allocation algorithm (see CASSANDRA-7032).
     */
    abstract public double size(Token next);
    /**
     * Returns a token that is slightly greater than this. Used to avoid clashes
     * between nodes in separate datacentres trying to use the same token via
     * the token allocation algorithm.
     */
    abstract public Token increaseSlightly();

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
     * A token is thus no comparable directly to a key. But to be able to select
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

    @SuppressWarnings("unchecked")
    public <R extends RingPosition<R>> R upperBound(Class<R> klass)
    {
        if (klass.equals(getClass()))
            return (R)this;
        else
            return (R)maxKeyBound();
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
