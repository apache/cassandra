/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.dht;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;

public class LocalPartitioner implements IPartitioner<LocalToken>
{
    private final AbstractType comparator;

    public LocalPartitioner(AbstractType comparator)
    {
        this.comparator = comparator;
    }

    public DecoratedKey<LocalToken> convertFromDiskFormat(byte[] key)
    {
        return decorateKey(key);
    }

    public DecoratedKey<LocalToken> decorateKey(byte[] key)
    {
        return new DecoratedKey<LocalToken>(getToken(key), key);
    }

    public LocalToken midpoint(LocalToken left, LocalToken right)
    {
        throw new UnsupportedOperationException();
    }

    public LocalToken getMinimumToken()
    {
        return new LocalToken(comparator, ArrayUtils.EMPTY_BYTE_ARRAY);
    }

    public LocalToken getToken(byte[] key)
    {
        return new LocalToken(comparator, key);
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
}
