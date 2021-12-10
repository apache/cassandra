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

import accord.api.Key;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;

public interface AccordKey extends Key<AccordKey>
{
    TableId tableId();
    PartitionPosition partitionKey();

    static int compare(AccordKey left, AccordKey right)
    {
        int cmp = left.tableId().compareTo(right.tableId());
        return cmp == 0 ? left.partitionKey().compareTo(right.partitionKey()) : cmp;
    }

    default int compareTo(AccordKey that)
    {
        return compare(this, that);
    }

    static abstract class AbstractKey<T extends PartitionPosition> implements AccordKey
    {
        private final TableId tableId;
        private final T key;

        public AbstractKey(TableId tableId, T key)
        {
            this.tableId = tableId;
            this.key = key;
        }

        @Override
        public TableId tableId()
        {
            return tableId;
        }

        @Override
        public PartitionPosition partitionKey()
        {
            return key;
        }
    }

    public static class PartitionKey extends AbstractKey<DecoratedKey>
    {
        public PartitionKey(TableId tableId, DecoratedKey key)
        {
            super(tableId, key);
        }
    }

    public static class TokenKey extends AbstractKey<Token.KeyBound>
    {
        public TokenKey(TableId tableId, Token.KeyBound key)
        {
            super(tableId, key);
        }
    }
}
