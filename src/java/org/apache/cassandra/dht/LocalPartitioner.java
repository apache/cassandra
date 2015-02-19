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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.CachedHashDecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

public class LocalPartitioner implements IPartitioner
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new LocalPartitioner(null).new LocalToken(null));

    final AbstractType<?> comparator;   // package-private to avoid access workarounds in embedded LocalToken.

    public LocalPartitioner(AbstractType<?> comparator)
    {
        this.comparator = comparator;
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return new CachedHashDecoratedKey(getToken(key), key);
    }

    public Token midpoint(Token left, Token right)
    {
        throw new UnsupportedOperationException();
    }

    public LocalToken getMinimumToken()
    {
        return new LocalToken(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    public LocalToken getToken(ByteBuffer key)
    {
        return new LocalToken(key);
    }

    public LocalToken getRandomToken()
    {
        throw new UnsupportedOperationException();
    }

    public Token.TokenFactory getTokenFactory()
    {
        throw new UnsupportedOperationException();
    }

    public boolean preservesOrder()
    {
        return true;
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        return Collections.singletonMap((Token)getMinimumToken(), new Float(1.0));
    }

    public AbstractType<?> getTokenValidator()
    {
        return comparator;
    }

    public class LocalToken extends ComparableObjectToken<ByteBuffer>
    {
        static final long serialVersionUID = 8437543776403014875L;

        public LocalToken(ByteBuffer token)
        {
            super(token);
        }

        @Override
        public String toString()
        {
            return comparator.getString(token);
        }

        @Override
        public int compareTo(Token o)
        {
            assert getPartitioner() == o.getPartitioner();
            return comparator.compare(token, ((LocalToken) o).token);
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            return prime + token.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (!(obj instanceof LocalToken))
                return false;
            LocalToken other = (LocalToken) obj;
            return token.equals(other.token);
        }

        @Override
        public IPartitioner getPartitioner()
        {
            return LocalPartitioner.this;
        }

        @Override
        public long getHeapSize()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(token);
        }
    }
}
