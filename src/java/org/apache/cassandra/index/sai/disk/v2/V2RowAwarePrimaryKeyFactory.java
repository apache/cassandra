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

import io.github.jbellis.jvector.util.RamUsageEstimator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * A row-aware {@link PrimaryKey.Factory}. This creates {@link PrimaryKey} instances that are
 * sortable by {@link DecoratedKey} and {@link Clustering}.
 */
public class V2RowAwarePrimaryKeyFactory implements PrimaryKey.Factory
{
    protected final ClusteringComparator clusteringComparator;
    public final boolean hasClustering;


    public V2RowAwarePrimaryKeyFactory(ClusteringComparator clusteringComparator)
    {
        this.clusteringComparator = clusteringComparator;
        this.hasClustering = clusteringComparator.size() > 0;
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

    public PrimaryKey createWithSource(PrimaryKeyMap primaryKeyMap, long sstableRowId, PrimaryKey sourceSstableMinKey, PrimaryKey sourceSstableMaxKey)
    {
        return new PrimaryKeyWithSource(primaryKeyMap, sstableRowId, sourceSstableMinKey, sourceSstableMaxKey);
    }

    protected class RowAwarePrimaryKey implements PrimaryKey
    {
        private final Token token;
        protected DecoratedKey partitionKey;
        protected Clustering<?> clustering;
        private Supplier<PrimaryKey> primaryKeySupplier;

        protected RowAwarePrimaryKey(Token token, DecoratedKey partitionKey, Clustering<?> clustering, Supplier<PrimaryKey> primaryKeySupplier)
        {
            this.token = token;
            this.partitionKey = partitionKey;
            this.clustering = clustering;
            this.primaryKeySupplier = primaryKeySupplier;
        }

        @Override
        public RowAwarePrimaryKey forStaticRow()
        {
            return new RowAwarePrimaryKey(token, partitionKey, Clustering.STATIC_CLUSTERING, primaryKeySupplier);
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
        public Clustering<?> clustering()
        {
            loadDeferred();
            return clustering;
        }

        @Override
        public PrimaryKey loadDeferred()
        {
            if (primaryKeySupplier != null)
            {
                assert partitionKey == null : "While applying existing primaryKeySupplier to load deferred primaryKey the partition key was unexpectedly already set";
                PrimaryKey deferredPrimaryKey = primaryKeySupplier.get();
                this.partitionKey = deferredPrimaryKey.partitionKey();
                assert this.token.equals(this.partitionKey.getToken()) : "Deferred primary key must contain the same token";

                // It is possible we have already set clustering to `STATIC_CLUSTERING` because this object
                // is the result of a call to `forStaticRow` on some other primary key that was not yet loaded.
                // In that case we must not overwrite it. Overwriting it would turn the key for the static row back into
                // the key to a regular row.
                if (this.clustering == null)
                    this.clustering = deferredPrimaryKey.clustering();

                primaryKeySupplier = null;
            }
            return this;
        }

        @Override
        public ByteSource asComparableBytes(ByteComparable.Version version)
        {
            return asComparableBytes(version == ByteComparable.Version.LEGACY ? ByteSource.END_OF_STREAM : ByteSource.TERMINATOR, version, false);
        }

        @Override
        public ByteSource asComparableBytesMinPrefix(ByteComparable.Version version)
        {
            return asComparableBytes(ByteSource.LT_NEXT_COMPONENT, version, true);
        }

        @Override
        public ByteSource asComparableBytesMaxPrefix(ByteComparable.Version version)
        {
            return asComparableBytes(ByteSource.GT_NEXT_COMPONENT, version, true);
        }

        protected ByteSource asComparableBytes(int terminator, ByteComparable.Version version, boolean isPrefix)
        {
            return ByteSource.withTerminator(terminator, buildComparableSources(version, isPrefix, true));
        }

        protected ByteSource[] buildComparableSources(ByteComparable.Version version, boolean isPrefix, boolean includeToken)
        {
            // We need to make sure that the key is loaded before returning a
            // byte comparable representation. If we don't, we won't get a correct
            // comparison because we potentially won't be using the partition key
            // and clustering for the lookup
            loadDeferred();

            int size = (includeToken ? 1 : 0) + 1 + ((hasClustering() || !isPrefix) ? 1 : 0);
            ByteSource[] comparableSources = new ByteSource[size];

            int index = 0;

            if (includeToken)
                comparableSources[index++] = token.asComparableBytes(version);

            comparableSources[index++] = ByteSource.of(partitionKey.getKey(), version);

            if (hasClustering())
                // It is important that the ClusteringComparator.asBytesComparable method is used
                // to maintain the correct clustering sort order
                comparableSources[index++] = clusteringComparator.asByteComparable(clustering).asComparableBytes(version);
            else if (!isPrefix)
                // prefix doesn't include null components
                comparableSources[index++] = null;

            assert index == comparableSources.length;
            return comparableSources;
        }

        @Override
        public int compareTo(PrimaryKey o)
        {
            // Always start comparison with token, since comparing directly on partition key requires loading it
            int cmp = token().compareTo(o.token());
            if (cmp != 0 || o.isTokenOnly())
                return cmp;

            // Compare the partition keys. If they are not equal or
            // this is a single row partition key or there are no
            // clusterings, then return the result of this without
            // needing to compare the clusterings.
            cmp = partitionKey().compareTo(o.partitionKey());
            if (cmp != 0 || !hasClustering() || !o.hasClustering())
                return cmp;
            return clusteringComparator.compare(clustering(), o.clustering());
        }

        @Override
        public int hashCode()
        {
            if (hasClustering)
                return Objects.hash(token, clustering());
            else
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
            return String.format("RowAwarePrimaryKey: { token: %s, partition: %s, clustering: %s:%s} ",
                                 token,
                                 partitionKey,
                                 clustering == null ? null : clustering.kind(),
                                 clustering == null ? null : Arrays.stream(clustering.getBufferArray())
                                                                   .map(ByteBufferUtil::bytesToHex)
                                                                   .collect(Collectors.joining(",")));
        }

        @Override
        public long ramBytesUsed()
        {
            // Object header + 4 references (token, partitionKey, clustering, primaryKeySupplier) + implicit outer reference
            long size = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER +
                       5L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

            if (token != null)
                size += token.getHeapSize();
            if (partitionKey != null)
                size += RamUsageEstimator.NUM_BYTES_OBJECT_HEADER +
                       2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF + // token and key references
                       2L * Long.BYTES;
            // We don't count clustering size here as it's managed elsewhere
            return size;
        }
    }
}
