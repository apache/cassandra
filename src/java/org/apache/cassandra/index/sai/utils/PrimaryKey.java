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

import java.util.Arrays;
import java.util.Objects;

import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Representation of the primary key for a row consisting of the {@link DecoratedKey} and
 * {@link Clustering} associated with a {@link org.apache.cassandra.db.rows.Row}.
 */
public interface PrimaryKey extends Comparable<PrimaryKey>, ByteComparable
{
    class Factory
    {
        private final ClusteringComparator clusteringComparator;

        public Factory(ClusteringComparator clusteringComparator)
        {
            this.clusteringComparator = clusteringComparator;
        }

        /**
         * Creates a {@link PrimaryKey} that is represented by a {@link Token}.
         * <p>
         * {@link Token} only primary keys are used for defining the partition range
         * of a query.
         */
        public PrimaryKey createTokenOnly(Token token)
        {
            assert token != null : "Cannot create a primary key with a null token";

            return new TokenOnlyPrimaryKey(token);
        }

        public PrimaryKey createPartitionKeyOnly(DecoratedKey partitionKey)
        {
            assert partitionKey != null : "Cannot create a primary key with a null partition key";

            return new ImmutablePrimaryKey(partitionKey, null);
        }

        /**
         * Creates a {@link PrimaryKey} that is fully represented by partition key
         * and clustering.
         */
        public PrimaryKey create(DecoratedKey partitionKey, Clustering<?> clustering)
        {
            assert partitionKey != null : "Cannot create a primary key with a null partition key";
            assert clustering != null : "Cannot create a primary key with a null clustering";

            return new ImmutablePrimaryKey(partitionKey, clustering);
        }

        public PrimaryKey createDeferred(Token token, Supplier<PrimaryKey> primaryKeySupplier)
        {
            assert token != null : "Cannot create a deferred primary key with a null token";
            assert primaryKeySupplier != null : "Cannot create a deferred primary key with a null key supplier";

            return new MutablePrimaryKey(token, primaryKeySupplier);
        }

        abstract class AbstractPrimaryKey implements PrimaryKey
        {
            @Override
            @SuppressWarnings("ConstantConditions")
            public ByteSource asComparableBytes(ByteComparable.Version version)
            {
                ByteSource keyComparable = ByteSource.of(partitionKey().getKey(), version);
                if (clusteringComparator.size() == 0)
                    return keyComparable;
                // It is important that the ClusteringComparator.asBytesComparable method is used
                // to maintain the correct clustering sort order
                ByteSource clusteringComparable = clustering() == null ||
                                                  clustering().isEmpty() ? null
                                                                         : clusteringComparator.asByteComparable(clustering())
                                                                                               .asComparableBytes(version);
                return ByteSource.withTerminator(version == ByteComparable.Version.LEGACY ? ByteSource.END_OF_STREAM
                                                                                          : ByteSource.TERMINATOR,
                                                 keyComparable,
                                                 clusteringComparable);
            }

            @Override
            @SuppressWarnings("ConstantConditions")
            public int compareTo(PrimaryKey o)
            {
                int cmp = token().compareTo(o.token());

                // If the tokens don't match then we don't need to compare any more of the key.
                // Otherwise, if either of the keys are token only we can only compare tokens
                if ((cmp != 0) || isTokenOnly() || o.isTokenOnly())
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
                return Objects.hash(token(), partitionKey(), clustering(), clusteringComparator);
            }

            @Override
            public boolean equals(Object obj)
            {
                if (obj instanceof PrimaryKey)
                    return compareTo((PrimaryKey) obj) == 0;
                return false;
            }

            @Override
            @SuppressWarnings("ConstantConditions")
            public String toString()
            {
                return isTokenOnly() ? String.format("PrimaryKey: { token: %s }", token())
                                     : String.format("PrimaryKey: { token: %s, partition: %s, clustering: %s:%s } ",
                                                     token(),
                                                     partitionKey(),
                                                     clustering() == null ? null : clustering().kind(),
                                                     clustering() == null ? null : Arrays.stream(clustering().getBufferArray())
                                                                                         .map(ByteBufferUtil::bytesToHex)
                                                                                         .collect(Collectors.joining(", ")));
            }
        }

        class TokenOnlyPrimaryKey extends AbstractPrimaryKey
        {
            private final Token token;

            TokenOnlyPrimaryKey(Token token)
            {
                this.token = token;
            }

            @Override
            public boolean isTokenOnly()
            {
                return true;
            }

            @Override
            public Token token()
            {
                return token;
            }

            @Override
            public DecoratedKey partitionKey()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Clustering<?> clustering()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ByteSource asComparableBytes(Version version)
            {
                throw new UnsupportedOperationException();
            }
        }

        class ImmutablePrimaryKey extends AbstractPrimaryKey
        {
            private final Token token;
            private final DecoratedKey partitionKey;
            private final Clustering<?> clustering;

            ImmutablePrimaryKey(DecoratedKey partitionKey, Clustering<?> clustering)
            {
                this.token = partitionKey.getToken();
                this.partitionKey = partitionKey;
                this.clustering = clustering;
            }

            @Override
            public Token token()
            {
                return token;
            }

            @Override
            public DecoratedKey partitionKey()
            {
                return partitionKey;
            }

            @Override
            public Clustering<?> clustering()
            {
                return clustering;
            }
        }

        class MutablePrimaryKey extends AbstractPrimaryKey
        {
            private final Token token;
            private final Supplier<PrimaryKey> primaryKeySupplier;

            private boolean notLoaded = true;
            private DecoratedKey partitionKey;
            private Clustering<?> clustering;

            MutablePrimaryKey(Token token, Supplier<PrimaryKey> primaryKeySupplier)
            {
                this.token = token;
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
            public Clustering<?> clustering()
            {
                loadDeferred();
                return clustering;
            }

            private void loadDeferred()
            {
                if (notLoaded)
                {
                    PrimaryKey deferredPrimaryKey = primaryKeySupplier.get();
                    this.partitionKey = deferredPrimaryKey.partitionKey();
                    this.clustering = deferredPrimaryKey.clustering();
                    notLoaded = false;
                }
            }
        }
    }

    default boolean isTokenOnly()
    {
        return false;
    }

    Token token();

    @Nullable
    DecoratedKey partitionKey();

    @Nullable
    Clustering<?> clustering();

    /**
     * Return whether the primary key has an empty clustering or not.
     * By default, the clustering is empty if the internal clustering
     * is null or is empty.
     *
     * @return {@code true} if the clustering is empty, otherwise {@code false}
     */
    @SuppressWarnings("ConstantConditions")
    default boolean hasEmptyClustering()
    {
        return clustering() == null || clustering().isEmpty();
    }

    /**
     * Returns the {@link PrimaryKey} as a {@link ByteSource} byte comparable representation.
     * <p>
     * It is important that these representations are only ever used with byte comparables using
     * the same elements. This means that {@code asComparableBytes} responses can only be used
     * together from the same {@link PrimaryKey} implementation.
     *
     * @param version the {@link ByteComparable.Version} to use for the implementation
     * @return the {@code ByteSource} byte comparable.
     */
    ByteSource asComparableBytes(ByteComparable.Version version);
}
