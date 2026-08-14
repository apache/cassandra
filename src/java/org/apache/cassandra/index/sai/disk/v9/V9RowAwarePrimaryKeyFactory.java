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

package org.apache.cassandra.index.sai.disk.v9;

import java.util.function.Supplier;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.v2.V2RowAwarePrimaryKeyFactory;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Factory for creating row aware primary keys, which does not use the token as prefix in byte comparison.
 * Thus, the byte comparables are not prefixed with tokens and allowing better compression.
 */
public class V9RowAwarePrimaryKeyFactory extends V2RowAwarePrimaryKeyFactory
{
    public V9RowAwarePrimaryKeyFactory(ClusteringComparator clusteringComparator)
    {
        super(clusteringComparator);
    }

    @Override
    public PrimaryKey createDeferred(Token token, Supplier<PrimaryKey> primaryKeySupplier)
    {
        return new RowAwarePrimaryKey(token, null, null, primaryKeySupplier);
    }

    @Override
    public PrimaryKey create(DecoratedKey partitionKey, Clustering<?> clustering)
    {
        return new RowAwarePrimaryKey(partitionKey.getToken(), partitionKey, clustering, null);
    }

    private class RowAwarePrimaryKey extends V2RowAwarePrimaryKeyFactory.RowAwarePrimaryKey
    {
        private RowAwarePrimaryKey(Token token, DecoratedKey partitionKey, Clustering<?> clustering, Supplier<PrimaryKey> primaryKeySupplier)
        {
            super(token, partitionKey, clustering, primaryKeySupplier);
        }

        @Override
        protected ByteSource asComparableBytes(int terminator, ByteComparable.Version version, boolean isPrefix)
        {
            return ByteSource.withTerminator(terminator, buildComparableSources(version, isPrefix, false));
        }
    }
}
