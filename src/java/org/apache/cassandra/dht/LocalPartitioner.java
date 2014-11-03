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

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

public class LocalPartitioner extends AbstractPartitioner
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new LocalToken(null, null));

    private final AbstractType<?> comparator;

    public LocalPartitioner(AbstractType<?> comparator)
    {
        this.comparator = comparator;
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return new BufferDecoratedKey(getToken(key), key);
    }

    public Token midpoint(Token left, Token right)
    {
        throw new UnsupportedOperationException();
    }

    public LocalToken getMinimumToken()
    {
        return new LocalToken(comparator, ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    public LocalToken getToken(ByteBuffer key)
    {
        return new LocalToken(comparator, key);
    }

    public long getHeapSizeOf(Token token)
    {
        return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(((LocalToken) token).token);
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
}
