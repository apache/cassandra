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
package org.apache.cassandra.index.sai.utils;

import java.util.function.Supplier;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.v1.PartitionAwarePrimaryKeyFactory;
import org.apache.cassandra.index.sai.disk.v2.RowAwarePrimaryKeyFactory;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Representation of the primary key for a row consisting of the {@link DecoratedKey} and
 * {@link Clustering} associated with a {@link org.apache.cassandra.db.rows.Row}.
 *
 * For legacy V1 support only the {@link DecoratedKey} will ever be supported for a row.
 *
 * For the V2 on-disk format the {@link DecoratedKey} and {@link Clustering} are supported.
 *
 */
public interface PrimaryKey extends Comparable<PrimaryKey>
{
    /**
     * A factory for creating {@link PrimaryKey} instances
     */
    interface Factory
    {
        /**
         * Creates a {@link PrimaryKey} that is represented by a {@link Token}.
         *
         * {@link Token} only primary keys are used for defining the partition range
         * of a query.
         *
         * @param token the {@link Token}
         * @return a {@link PrimaryKey} represented by a token only
         */
        PrimaryKey createTokenOnly(Token token);

        /**
         * Creates a {@link PrimaryKey} that is represented by a {@link DecoratedKey}.
         *
         * {@link DecoratedKey} only primary keys are used to define the minumum and
         * maximum coverage of an index.
         *
         * @param partitionKey the {@link DecoratedKey}
         * @return a {@link PrimaryKey} represented by a partition key only
         */
        default PrimaryKey createPartitionKeyOnly(DecoratedKey partitionKey)
        {
            return create(partitionKey, Clustering.EMPTY);
        }

        /**
         * Creates a {@link PrimaryKey} with deferred loading. Deferred loading means
         * that the key will only be fully loaded when the full representation of the
         * key is needed for comparison. Before the key is loaded it will be represented
         * by a token only, so it will only need loading if the token is matched in a
         * comparison or the byte comparable representation of the key is required.
         *
         * @param token the {@link Token}
         * @param primaryKeySupplier the supplier of the full key
         * @return a {@link PrimaryKey} the token and a primary key supplier
         */
        PrimaryKey createDeferred(Token token, Supplier<PrimaryKey> primaryKeySupplier);

        /**
         * Creates a {@link PrimaryKey} that is fully represented by partition key
         * and clustering.
         *
         * @param partitionKey the {@link DecoratedKey}
         * @param clustering the {@link Clustering}
         * @return a {@link PrimaryKey} contain the partition key and clustering
         */
        PrimaryKey create(DecoratedKey partitionKey, Clustering clustering);
    }

    /**
     * Returns a {@link Factory} for creating {@link PrimaryKey} instances. The factory
     * returned is based on the capabilities of the {@link IndexFeatureSet}.
     *
     * @param clusteringComparator the {@link ClusteringComparator} used by the
     *                             {@link RowAwarePrimaryKeyFactory} for clustering comparisons
     * @param indexFeatureSet the {@link IndexFeatureSet} used to decide the type of
     *                        factory to use
     * @return a {@link Factory} for {@link PrimaryKey} creation
     */
    static Factory factory(ClusteringComparator clusteringComparator, IndexFeatureSet indexFeatureSet)
    {
        return indexFeatureSet.isRowAware() ? new RowAwarePrimaryKeyFactory(clusteringComparator)
                                            : new PartitionAwarePrimaryKeyFactory();
    }

    /**
     * Returns the {@link Token} associated with this primary key.
     *
     * @return the {@link Token}
     */
    Token token();

    /**
     * Returns the {@link DecoratedKey} associated with this primary key.
     *
     * @return the {@link DecoratedKey}
     */
    DecoratedKey partitionKey();

    /**
     * Returns the {@link Clustering} associated with this primary key
     *
     * @return the {@link Clustering}
     */
    Clustering clustering();

    /**
     * Return whether the primary key has an empty clustering or not.
     * By default the clustering is empty if the internal clustering
     * is null or is empty.
     *
     * @return {@code true} if the clustering is empty, otherwise {@code false}
     */
    default boolean hasEmptyClustering()
    {
        return clustering() == null || clustering().isEmpty();
    }

    /**
     * Load the primary key from the {@link Supplier<PrimaryKey>} (if one
     * is available) and fully populate the primary key.
     *
     * @return the fully populated {@link PrimaryKey}
     */
    PrimaryKey loadDeferred();

    /**
     * Returns the {@link PrimaryKey} as a {@link ByteSource} byte comparable representation.
     *
     * It is important that these representations are only ever used with byte comparables using
     * the same elements. This means that {@code asComparableBytes} responses can only be used
     * together from the same {@link PrimaryKey} implementation.
     *
     * @param version the {@link ByteComparable.Version} to use for the implementation
     * @return the {@code ByteSource} byte comparable.
     */
    ByteSource asComparableBytes(ByteComparable.Version version);
}
