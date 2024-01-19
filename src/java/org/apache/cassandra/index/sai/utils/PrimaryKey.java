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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * Representation of the primary key for a row consisting of the {@link DecoratedKey} and
 * {@link Clustering} associated with a {@link org.apache.cassandra.db.rows.Row}.
 * The {@link Factory.TokenOnlyPrimaryKey} is used by the {@link org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher} to
 * position the search within the query range.
 */
public interface PrimaryKey extends Comparable<PrimaryKey>, ByteComparable
{
    /**
     * See the javadoc for {@link #kind()} for how this enum is used.
      */
    enum Kind
    {
        TOKEN(false),
        SKINNY(false),
        WIDE(true),
        STATIC(true);

        public final boolean hasClustering;

        Kind(boolean hasClustering)
        {
            this.hasClustering = hasClustering;
        }

        public boolean isIntersectable(Kind other)
        {
            if (this == TOKEN)
                return other == TOKEN;
            else if (this == SKINNY)
                return other == SKINNY;
            else if (this == WIDE || this == STATIC)
                return other == WIDE || other == STATIC;
            
            throw new AssertionError("Unknown Kind: " + other);
        }
    }

    class Factory
    {
        private final IPartitioner partitioner;
        private final ClusteringComparator clusteringComparator;

        public Factory(IPartitioner partitioner, ClusteringComparator clusteringComparator)
        {
            this.partitioner = partitioner;
            this.clusteringComparator = clusteringComparator;
        }

        /**
         * Creates a {@link PrimaryKey} that is represented by a {@link Token}.
         * <p>
         * {@link Token} only primary keys are used for defining the partition range
         * of a query.
         */
        public PrimaryKey create(Token token)
        {
            assert token != null : "Cannot create a primary key with a null token";

            return new TokenOnlyPrimaryKey(token);
        }

        /**
         * Create a {@link PrimaryKey} for tables without clustering columns
         */
        public PrimaryKey create(DecoratedKey partitionKey)
        {
            assert clusteringComparator.size() == 0 : "Cannot create a skinny primary key for a table with clustering columns";
            assert partitionKey != null : "Cannot create a primary key with a null partition key";

            return new SkinnyPrimaryKey(partitionKey);
        }

        /**
         * Creates a {@link PrimaryKey} that is fully represented by partition key
         * and clustering.
         */
        public PrimaryKey create(DecoratedKey partitionKey, Clustering<?> clustering)
        {
            assert clusteringComparator.size() > 0 : "Cannot create a wide primary key for a table without clustering columns";
            assert partitionKey != null : "Cannot create a primary key with a null partition key";
            assert clustering != null : "Cannot create a primary key with a null clustering";

            return clustering == Clustering.STATIC_CLUSTERING ? new StaticPrimaryKey(partitionKey) : new WidePrimaryKey(partitionKey, clustering);
        }

        /**
         * Create a {@link PrimaryKey} from a {@link ByteSource}. This should only be used with {@link ByteSource} instances
         * created by calls to {@link PrimaryKey#asComparableBytes(Version)}.
         */
        public PrimaryKey fromComparableBytes(ByteSource byteSource)
        {
            if (clusteringComparator.size() > 0)
            {
                ByteSource.Peekable peekable = ByteSource.peekable(byteSource);
                DecoratedKey partitionKey = partitionKeyFromComparableBytes(ByteSourceInverse.nextComponentSource(peekable));
                Clustering<?> clustering = clusteringFromByteComparable(ByteSourceInverse.nextComponentSource(peekable));
                return create(partitionKey, clustering);
            }
            else
            {
                return create(partitionKeyFromComparableBytes(byteSource));
            }
        }

        /**
         * Create a {@link DecoratedKey} from a {@link ByteSource}. This is a separate method because of it's use by
         * the {@link org.apache.cassandra.index.sai.disk.PrimaryKeyMap} implementations to create partition keys.
         */
        public DecoratedKey partitionKeyFromComparableBytes(ByteSource byteSource)
        {
            ByteBuffer decoratedKey = ByteBuffer.wrap(ByteSourceInverse.getUnescapedBytes(ByteSource.peekable(byteSource)));
            return new BufferDecoratedKey(partitioner.getToken(decoratedKey), decoratedKey);
        }

        /**
         * Create a {@link Clustering} from a {@link ByteSource}. This is a separate method because of its use by
         * the {@link org.apache.cassandra.index.sai.disk.v1.WidePrimaryKeyMap} to create its clustering keys.
         */
        public Clustering<?> clusteringFromByteComparable(ByteSource byteSource)
        {
            Clustering<?> clustering = clusteringComparator.clusteringFromByteComparable(ByteBufferAccessor.instance, v -> byteSource);

            // Clustering is null for static rows
            return (clustering == null) ? Clustering.STATIC_CLUSTERING : clustering;
        }

        class TokenOnlyPrimaryKey implements PrimaryKey
        {
            protected final Token token;

            TokenOnlyPrimaryKey(Token token)
            {
                this.token = token;
            }

            @Override
            public Kind kind()
            {
                return Kind.TOKEN;
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

            @Override
            public int compareTo(PrimaryKey o)
            {
                return token().compareTo(o.token());
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(token(), clusteringComparator);
            }

            @Override
            public boolean equals(Object obj)
            {
                if (obj instanceof PrimaryKey)
                    return compareTo((PrimaryKey) obj) == 0;
                return false;
            }

            @Override
            public String toString()
            {
                return String.format("PrimaryKey: { token: %s }", token());
            }
        }

        class SkinnyPrimaryKey extends TokenOnlyPrimaryKey
        {
            protected final DecoratedKey partitionKey;

            SkinnyPrimaryKey(DecoratedKey partitionKey)
            {
                super(partitionKey.getToken());
                this.partitionKey = partitionKey;
            }

            @Override
            public Kind kind()
            {
                return Kind.SKINNY;
            }

            @Override
            public DecoratedKey partitionKey()
            {
                return partitionKey;
            }

            @Override
            public ByteSource asComparableBytes(Version version)
            {
                return ByteSource.of(partitionKey().getKey(), version);
            }

            @Override
            public int compareTo(PrimaryKey o)
            {
                int cmp = super.compareTo(o);

                // If the tokens don't match then we don't need to compare any more of the key.
                // Otherwise, if the other key is token only we can only compare tokens
                // This is used by the ResultRetriever to skip to the current key range start position
                // during result retrieval.
                if ((cmp != 0) || o.kind() == Kind.TOKEN)
                    return cmp;

                // Finally compare the partition keys
                return partitionKey().compareTo(o.partitionKey());
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(token(), partitionKey(), Clustering.EMPTY, clusteringComparator);
            }

            @Override
            public String toString()
            {
                return String.format("PrimaryKey: { token: %s, partition: %s }", token(), partitionKey());
            }
        }

        class StaticPrimaryKey extends SkinnyPrimaryKey
        {
            StaticPrimaryKey(DecoratedKey partitionKey)
            {
                super(partitionKey);
            }

            @Override
            public Kind kind()
            {
                return Kind.STATIC;
            }

            @Override
            public Clustering<?> clustering()
            {
                return Clustering.STATIC_CLUSTERING;
            }

            @Override
            public ByteSource asComparableBytes(ByteComparable.Version version)
            {
                ByteSource keyComparable = ByteSource.of(partitionKey().getKey(), version);
                // Static clustering cannot be serialized or made to a byte comparable, so we use null as the component.
                return ByteSource.withTerminator(version == ByteComparable.Version.LEGACY ? ByteSource.END_OF_STREAM
                                                                                          : ByteSource.TERMINATOR,
                                                 keyComparable,
                                                 null);
            }

            @Override
            public int compareTo(PrimaryKey o)
            {
                int cmp = super.compareTo(o);
                if (cmp != 0 || o.kind() == Kind.TOKEN || o.kind() == Kind.SKINNY)
                    return cmp;
                // At this point the other key is in the same partition as this static key so is equal to it. This
                // has to be the case because otherwise, intersections between static column indexes and ordinary
                // indexes will fail.
                return 0;
            }

            @Override
            public int compareToStrict(PrimaryKey o)
            {
                int cmp = compareTo(o);
                // Always order this STATIC key before a WIDE key in the same partition, as this corresponds to the
                // order of the corresponding row IDs in an on-disk postings list.
                return o.kind() == Kind.WIDE && cmp == 0 ? -1 : cmp;
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(token(), partitionKey(), Clustering.STATIC_CLUSTERING, clusteringComparator);
            }

            @Override
            public String toString()
            {
                return String.format("PrimaryKey: { token: %s, partition: %s, clustering: STATIC } ", token(), partitionKey());
            }

            @Override
            public PrimaryKey toStatic()
            {
                return this;
            }
        }

        class WidePrimaryKey extends SkinnyPrimaryKey
        {
            private final Clustering<?> clustering;

            WidePrimaryKey(DecoratedKey partitionKey, Clustering<?> clustering)
            {
                super(partitionKey);
                this.clustering = clustering;
            }

            @Override
            public Kind kind()
            {
                return Kind.WIDE;
            }

            @Override
            public Clustering<?> clustering()
            {
                return clustering;
            }

            @Override
            public ByteSource asComparableBytes(ByteComparable.Version version)
            {
                ByteSource keyComparable = ByteSource.of(partitionKey().getKey(), version);
                // It is important that the ClusteringComparator.asBytesComparable method is used
                // to maintain the correct clustering sort order.
                ByteSource clusteringComparable = clusteringComparator.asByteComparable(clustering()).asComparableBytes(version);
                return ByteSource.withTerminator(version == ByteComparable.Version.LEGACY ? ByteSource.END_OF_STREAM
                                                                                          : ByteSource.TERMINATOR,
                                                 keyComparable,
                                                 clusteringComparable);
            }

            @Override
            public int compareTo(PrimaryKey o)
            {
                int cmp = super.compareTo(o);
                if (cmp != 0 || o.kind() == Kind.TOKEN || o.kind() == Kind.SKINNY)
                    return cmp;
                // At this point this key is in the same partition as the other key so if the other key is a static
                // key then it must be equal to it. See comment in the compareTo for static keys above.
                if (o.kind() == Kind.STATIC)
                    return 0;
                return clusteringComparator.compare(clustering(), o.clustering());
            }

            @Override
            public int compareToStrict(PrimaryKey o)
            {
                int cmp = compareTo(o);
                // Always order this WIDE key before a STATIC key in the same partition, as this corresponds to the
                // order of the corresponding row IDs in an on-disk postings list.
                return o.kind() == Kind.STATIC && cmp == 0 ? 1 : cmp;
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(token(), partitionKey(), clustering(), clusteringComparator);
            }

            @Override
            public String toString()
            {
                return String.format("PrimaryKey: { token: %s, partition: %s, clustering: %s:%s } ",
                                     token(),
                                     partitionKey(),
                                     clustering().kind(),
                                     Arrays.stream(clustering().getBufferArray())
                                           .map(ByteBufferUtil::bytesToHex)
                                           .collect(Collectors.joining(", ")));
            }

            @Override
            public PrimaryKey toStatic()
            {
                return new StaticPrimaryKey(partitionKey);
            }
        }
    }

    /**
     * Returns the {@link Kind} of the {@link PrimaryKey}. The {@link Kind} is used locally in the {@link #compareTo(Object)}
     * methods to determine how far the comparision needs to go between keys.
     * <p>
     * The {@link Kind} values have a categorization of {@code isClustering}. This indicates whether the key belongs to
     * a table with clustering tables or not.
     */
    Kind kind();

    /**
     * Returns the {@link Token} component of the {@link PrimaryKey}
     */
    Token token();

    /**
     * Returns the {@link DecoratedKey} representing the partition key of the {@link PrimaryKey}.
     * <p>
     * Note: This cannot be null but some {@link PrimaryKey} implementations can throw {@link UnsupportedOperationException}
     * if they do not support partition keys.
     */
    DecoratedKey partitionKey();

    /**
     * Returns the {@link Clustering} representing the clustering component of the {@link PrimaryKey}.
     * <p>
     * Note: This cannot be null but some {@link PrimaryKey} implementations can throw {@link UnsupportedOperationException}
     * if they do not support clustering columns.
     */
    Clustering<?> clustering();

    /**
     * Returns the {@link PrimaryKey} as a {@link ByteSource} byte comparable representation.
     * <p>
     * It is important that these representations are only ever used with byte comparables using
     * the same elements. This means that {@code asComparableBytes} responses can only be used
     * together from the same {@link PrimaryKey} implementation.
     *
     * @param version the {@link ByteComparable.Version} to use for the implementation
     * @return the {@code ByteSource} byte comparable.
     * @throws UnsupportedOperationException for {@link PrimaryKey} implementations that are not byte-comparable
     */
    ByteSource asComparableBytes(ByteComparable.Version version);

    default PrimaryKey toStatic()
    {
        throw new UnsupportedOperationException("Only STATIC and WIDE keys can be converted to STATIC");
    }

    default int compareToStrict(PrimaryKey o)
    {
        return compareTo(o);
    }
}
