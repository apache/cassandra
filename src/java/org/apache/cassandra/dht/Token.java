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

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.StorageService;
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

    public static class TokenSerializer implements ISerializer<Token>
    {
        public void serialize(Token token, DataOutputPlus out) throws IOException
        {
            IPartitioner p = StorageService.getPartitioner();
            ByteBuffer b = p.getTokenFactory().toByteArray(token);
            ByteBufferUtil.writeWithLength(b, out);
        }

        public Token deserialize(DataInput in) throws IOException
        {
            IPartitioner p = StorageService.getPartitioner();
            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            return p.getTokenFactory().fromByteArray(ByteBuffer.wrap(bytes));
        }

        public long serializedSize(Token object, TypeSizes typeSizes)
        {
            IPartitioner p = StorageService.getPartitioner();
            ByteBuffer b = p.getTokenFactory().toByteArray(object);
            return TypeSizes.NATIVE.sizeof(b.remaining()) + b.remaining();
        }
    }

    abstract public Object getTokenValue();

    public Token getToken()
    {
        return this;
    }

    public boolean isMinimum(IPartitioner partitioner)
    {
        return this.equals(partitioner.getMinimumToken());
    }

    public boolean isMinimum()
    {
        return isMinimum(StorageService.getPartitioner());
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
    public KeyBound minKeyBound(IPartitioner partitioner)
    {
        return new KeyBound(this, true);
    }

    public KeyBound minKeyBound()
    {
        return minKeyBound(null);
    }

    public KeyBound maxKeyBound(IPartitioner partitioner)
    {
        /*
         * For each token, we needs both minKeyBound and maxKeyBound
         * because a token corresponds to a range of keys. But the minimun
         * token corresponds to no key, so it is valid and actually much
         * simpler to associate the same value for minKeyBound and
         * maxKeyBound for the minimun token.
         */
        if (isMinimum(partitioner))
            return minKeyBound();
        return new KeyBound(this, false);
    }

    public KeyBound maxKeyBound()
    {
        return maxKeyBound(StorageService.getPartitioner());
    }

    @SuppressWarnings("unchecked")
    public <R extends RingPosition<R>> R upperBound(Class<R> klass)
    {
        if (klass.equals(getClass()))
            return (R)this;
        else
            return (R)maxKeyBound();
    }

    public static class KeyBound implements RowPosition
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

        public int compareTo(RowPosition pos)
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

        public boolean isMinimum(IPartitioner partitioner)
        {
            return getToken().isMinimum(partitioner);
        }

        public boolean isMinimum()
        {
            return isMinimum(StorageService.getPartitioner());
        }

        public RowPosition.Kind kind()
        {
            return isMinimumBound ? RowPosition.Kind.MIN_BOUND : RowPosition.Kind.MAX_BOUND;
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
