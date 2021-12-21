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

package org.apache.cassandra.index.sai.disk.v2;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * A row-aware {@link PrimaryKey.Factory}. This creates {@link PrimaryKey} instances that are
 * sortable by {@link DecoratedKey} and {@link Clustering}.
 */
public class RowAwarePrimaryKeyFactory implements PrimaryKey.Factory
{
    private final ClusteringComparator clusteringComparator;

    public RowAwarePrimaryKeyFactory(ClusteringComparator clusteringComparator)
    {
        this.clusteringComparator = clusteringComparator;
    }

    @Override
    public PrimaryKey createTokenOnly(Token token)
    {
        return new RowAwarePrimaryKey(token, null, null, null);
    }

    @Override
    public PrimaryKey createDeferred(Token token, Supplier<PrimaryKey> primaryKeySupplier)
    {
        return new RowAwarePrimaryKey(token, null, null, primaryKeySupplier);
    }

    @Override
    public PrimaryKey create(DecoratedKey partitionKey, Clustering clustering)
    {
        return new RowAwarePrimaryKey(partitionKey.getToken(), partitionKey, clustering, null);
    }

    private class RowAwarePrimaryKey implements PrimaryKey
    {
        private Token token;
        private DecoratedKey partitionKey;
        private Clustering clustering;
        private Supplier<PrimaryKey> primaryKeySupplier;

        private RowAwarePrimaryKey(Token token, DecoratedKey partitionKey, Clustering clustering, Supplier<PrimaryKey> primaryKeySupplier)
        {
            this.token = token;
            this.partitionKey = partitionKey;
            this.clustering = clustering;
            this.primaryKeySupplier = primaryKeySupplier;
        }

        @Override
        public Token token()
        {
            return token;
        }

        @Override
        public DecoratedKey partitionKey()
        {
            loadDeferred();
            return partitionKey;
        }

        @Override
        public Clustering clustering()
        {
            loadDeferred();
            return clustering;
        }

        @Override
        public PrimaryKey loadDeferred()
        {
            if (primaryKeySupplier != null && partitionKey == null)
            {
                PrimaryKey deferredPrimaryKey = primaryKeySupplier.get();
                this.partitionKey = deferredPrimaryKey.partitionKey();
                this.clustering = deferredPrimaryKey.clustering();
                primaryKeySupplier = null;
            }
            return this;
        }

        @Override
        public ByteSource asComparableBytes(ByteComparable.Version version)
        {
            // We need to make sure that the key is loaded before returning a
            // byte comparable representation. If we don't we won't get a correct
            // comparison because we potentially won't be using the partition key
            // and clustering for the lookup
            loadDeferred();

            ByteSource tokenComparable = token.asComparableBytes(version);
            ByteSource keyComparable = partitionKey == null ? null
                                                            : ByteSource.of(partitionKey.getKey(), version);
            // It is important that the ClusteringComparator.asBytesComparable method is used
            // to maintain the correct clustering sort order
            ByteSource clusteringComparable = clusteringComparator.size() == 0 ||
                                              clustering == null ||
                                              clustering.isEmpty() ? null
                                                                   : clusteringComparator.asByteComparable(clustering)
                                                                                         .asComparableBytes(version);
            return ByteSource.withTerminator(version == ByteComparable.Version.LEGACY
                                             ? ByteSource.END_OF_STREAM
                                             : ByteSource.TERMINATOR,
                                             tokenComparable,
                                             keyComparable,
                                             clusteringComparable);
        }

        @Override
        public int compareTo(PrimaryKey o)
        {
            int cmp = token().compareTo(o.token());

            // If the tokens don't match then we don't need to compare any more of the key.
            // Otherwise if this key has no deferred loader and it's partition key is null
            // or the other partition key is null then one or both of the keys
            // are token only so we can only compare tokens
            if ((cmp != 0) || (primaryKeySupplier == null && partitionKey == null) || o.partitionKey() == null)
                return cmp;

            // Next compare the partition keys. If they are not equal or
            // this is a single row partition key or there are no
            // clusterings then we can return the result of this without
            // needing to compare the clusterings
            cmp = partitionKey().compareTo(o.partitionKey());
            if (cmp != 0 || hasEmptyClustering() || o.hasEmptyClustering())
                return cmp;
            return clusteringComparator.compare(clustering(), o.clustering());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(token, partitionKey, clustering, clusteringComparator);
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
            return String.format("RowAwarePrimaryKey: { token: %s, partition: %s, clustering: %s:%s} ",
                                 token,
                                 partitionKey,
                                 clustering == null ? null : clustering.kind(),
                                 clustering == null ? null :String.join(",", Arrays.stream(clustering.getBufferArray())
                                                                                   .map(ByteBufferUtil::bytesToHex)
                                                                                   .collect(Collectors.toList())));
        }
    }
}
