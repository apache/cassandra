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

package org.apache.cassandra.index.sai.disk.v1;

import java.util.Objects;
import java.util.function.Supplier;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * A partition-aware {@link PrimaryKey.Factory}. This creates {@link PrimaryKey} instances that are
 * sortable by {@link DecoratedKey} only.
 */
public class PartitionAwarePrimaryKeyFactory implements PrimaryKey.Factory
{
    @Override
    public PrimaryKey createTokenOnly(Token token)
    {
        assert token != null;
        return new PartitionAwarePrimaryKey(token, null, null);
    }

    @Override
    public PrimaryKey createDeferred(Token token, Supplier<PrimaryKey> primaryKeySupplier)
    {
        assert token != null;
        return new PartitionAwarePrimaryKey(token, null, primaryKeySupplier);
    }

    @Override
    public PrimaryKey create(DecoratedKey partitionKey, Clustering clustering)
    {
        assert partitionKey != null;
        return new PartitionAwarePrimaryKey(partitionKey.getToken(), partitionKey, null);
    }

    private class PartitionAwarePrimaryKey implements PrimaryKey
    {
        private final Token token;
        private DecoratedKey partitionKey;
        private Supplier<PrimaryKey> primaryKeySupplier;

        private PartitionAwarePrimaryKey(Token token, DecoratedKey partitionKey, Supplier<PrimaryKey> primaryKeySupplier)
        {
            this.token = token;
            this.partitionKey = partitionKey;
            this.primaryKeySupplier = primaryKeySupplier;
        }

        @Override
        public PrimaryKey loadDeferred()
        {
            if (primaryKeySupplier != null && partitionKey == null)
            {
                this.partitionKey = primaryKeySupplier.get().partitionKey();
                primaryKeySupplier = null;
            }
            return this;
        }

        @Override
        public Token token()
        {
            return this.token;
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return partitionKey;
        }

        @Override
        public Clustering clustering()
        {
            return Clustering.EMPTY;
        }

        @Override
        public ByteSource asComparableBytes(ByteComparable.Version version)
        {
            // Note: Unlike row-aware primary keys the asComparable method in for
            // partition aware keys is only used on the write side so we do not need
            // to enforce deferred loading here.
            ByteSource tokenComparable = token.asComparableBytes(version);
            ByteSource keyComparable = partitionKey == null ? null
                                                            :ByteSource.of(partitionKey.getKey(), version);
            return ByteSource.withTerminator(version == ByteComparable.Version.LEGACY
                                             ? ByteSource.END_OF_STREAM
                                             : ByteSource.TERMINATOR,
                                             tokenComparable,
                                             keyComparable,
                                             null);
        }

        @Override
        public int compareTo(PrimaryKey o)
        {
            if (partitionKey == null || o.partitionKey() == null)
                return token().compareTo(o.token());
            return partitionKey.compareTo(o.partitionKey());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(token);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof PrimaryKey)
                return compareTo((PrimaryKey)obj) == 0;
            return false;
        }

        @Override
        public String toString()
        {
            return String.format("TokenAwarePrimaryKey: { token: %s, partition: %s } ", token, partitionKey == null ? null : partitionKey);
        }
    }
}
