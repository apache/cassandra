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
import java.util.stream.Collectors;

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
public class PrimaryKey implements Comparable<PrimaryKey>
{
    private final Token token;
    private final DecoratedKey partitionKey;
    private final Clustering<?> clustering;
    private final ClusteringComparator clusteringComparator;

    PrimaryKey(Token token)
    {
        this(token, null, null, null);
    }

    PrimaryKey(Token token,
               DecoratedKey partitionKey,
               Clustering<?> clustering,
               ClusteringComparator clusteringComparator)
    {
        this.token = token;
        this.partitionKey = partitionKey;
        this.clustering = clustering;
        this.clusteringComparator = clusteringComparator;
    }

    /**
     * Returns a {@link PrimaryKeyFactory} for creating {@link PrimaryKey} instances.
     *
     * @param clusteringComparator the {@link ClusteringComparator} used by the
     *                             {@link PrimaryKeyFactory} for clustering comparisons
     * @return a {@link PrimaryKeyFactory} for {@link PrimaryKey} creation
     */
    public static PrimaryKeyFactory factory(ClusteringComparator clusteringComparator)
    {
        return new PrimaryKeyFactory(clusteringComparator);
    }

    /**
     * @return the {@link Token} associated with this primary key.
     */
    public Token token()
    {
        return token;
    }

    /**
     * @return the {@link DecoratedKey} associated with this primary key.
     */
    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    /**
     * @return the {@link Clustering} associated with this primary key.
     */
    public Clustering<?> clustering()
    {
        return clustering;
    }

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
    public ByteSource asComparableBytes(ByteComparable.Version version)
    {
        ByteSource tokenComparable = token.asComparableBytes(version);
        if (partitionKey == null)
            return ByteSource.withTerminator(version == ByteComparable.Version.LEGACY ? ByteSource.END_OF_STREAM
                                                                                      : ByteSource.TERMINATOR,
                                             tokenComparable,
                                             null,
                                             null);
        ByteSource keyComparable = ByteSource.of(partitionKey.getKey(), version);
        // It is important that the ClusteringComparator.asBytesComparable method is used
        // to maintain the correct clustering sort order
        ByteSource clusteringComparable = clusteringComparator.size() == 0 ||
                                          clustering == null ||
                                          clustering.isEmpty() ? null
                                                               : clusteringComparator.asByteComparable(clustering)
                                                                                     .asComparableBytes(version);
        return ByteSource.withTerminator(version == ByteComparable.Version.LEGACY ? ByteSource.END_OF_STREAM
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
        // Otherwise, if it's partition key is null or the other partition key is null
        // then one or both of the keys are token only so we can only compare tokens
        if ((cmp != 0) || (partitionKey == null) || o.partitionKey() == null)
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

    /**
     * Return whether the primary key has an empty clustering or not.
     * By default, the clustering is empty if the internal clustering
     * is null or is empty.
     *
     * @return {@code true} if the clustering is empty, otherwise {@code false}
     */
    public boolean hasEmptyClustering()
    {
        return clustering() == null || clustering().isEmpty();
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
        return String.format("PrimaryKey: { token: %s, partition: %s, clustering: %s:%s} ",
                             token,
                             partitionKey,
                             clustering == null ? null : clustering.kind(),
                             clustering == null ? null : Arrays.stream(clustering.getBufferArray())
                                                               .map(ByteBufferUtil::bytesToHex)
                                                               .collect(Collectors.joining(",")));
    }
}
