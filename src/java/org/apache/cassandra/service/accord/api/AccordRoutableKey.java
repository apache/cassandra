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

package org.apache.cassandra.service.accord.api;

import java.util.Objects;

import accord.primitives.RoutableKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.SentinelKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;

public abstract class AccordRoutableKey implements RoutableKey
{
    final String keyspace; // TODO (desired): use an id (TrM)

    protected AccordRoutableKey(String keyspace)
    {
        this.keyspace = keyspace;
    }

    public final String keyspace() { return keyspace; }
    public abstract Token token();

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspace, token().tokenHash());
    }

    @Override
    public final int compareTo(RoutableKey that)
    {
        return compareTo((AccordRoutableKey) that);
    }

    public final int compareTo(AccordRoutableKey that)
    {
        int cmp = this.keyspace().compareTo(that.keyspace());
        if (cmp != 0)
            return cmp;

        if (this.getClass() == SentinelKey.class || that.getClass() == SentinelKey.class)
        {
            int leftInt = this.getClass() == SentinelKey.class ? ((SentinelKey) this).asInt() : 0;
            int rightInt = that.getClass() == SentinelKey.class ? ((SentinelKey) that).asInt() : 0;
            return Integer.compare(leftInt, rightInt);
        }

        cmp = this.token().compareTo(that.token());
        if (cmp != 0)
            return cmp;

        if (this.getClass() == TokenKey.class)
            return that.getClass() == TokenKey.class ? 0 : 1;
        return that.getClass() == TokenKey.class ? -1 : ((PartitionKey)this).tableId.compareTo(((PartitionKey)that).tableId);
    }

    @Override
    public final boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordRoutableKey that = (AccordRoutableKey) o;
        return compareTo(that) == 0;
    }
}
