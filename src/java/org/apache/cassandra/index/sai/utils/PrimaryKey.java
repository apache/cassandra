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
import java.util.stream.Collectors;

import com.google.common.base.Objects;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The primary key of a row, composed by the partition key and the clustering key.
 */
public abstract class PrimaryKey
{
    private final DecoratedKey partitionKey;

    /**
     * Returns a new primary key composed by the specified partition and clustering keys.
     *
     * @param partitionKey a partition key
     * @param clustering a clustering key
     * @return a new primary key composed by {@code partitionKey} and {@code ClusteringKey}
     */
    public static PrimaryKey of(DecoratedKey partitionKey, Clustering clustering)
    {
        if (clustering == Clustering.EMPTY)
            return new Skinny(partitionKey);
        else if (clustering == Clustering.STATIC_CLUSTERING)
            return new Static(partitionKey);
        else
            return new Wide(partitionKey, clustering);
    }

    private PrimaryKey(DecoratedKey partitionKey)
    {
        this.partitionKey = partitionKey;
    }

    /**
     * Returns the {@link Token} of the partition key.
     *
     * @return the partitioning token of the partition key
     */
    public Token token()
    {
        return partitionKey.getToken();
    }

    /**
     * Returns the partition key.
     *
     * @return the partition key
     */
    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    /**
     * Returns the clustering key.
     *
     * @return the clustering key
     */
    public abstract Clustering clustering();

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrimaryKey that = (PrimaryKey) o;
        return Objects.equal(partitionKey, that.partitionKey) && Objects.equal(clustering(), that.clustering());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(partitionKey, clustering());
    }

    public String toString(TableMetadata metadata)
    {
        return String.format("PrimaryKey: { partition : %s, clustering: %s}",
                             metadata.partitionKeyType.getString(partitionKey.getKey()),
                             clustering().toString(metadata));
    }

    @Override
    public String toString()
    {
        return String.format("PrimaryKey: { partition : %s, clustering: %s} "+getClass().getSimpleName(),
                             ByteBufferUtil.bytesToHex(partitionKey.getKey()),
                             String.join(",", Arrays.stream(clustering().getBufferArray())
                                                    .map(ByteBufferUtil::bytesToHex)
                                                    .collect(Collectors.toList())));
    }

    /**
     * {@link PrimaryKey} implementation for rows in tables without a defined clustering key.
     */
    static class Skinny extends PrimaryKey
    {
        Skinny(DecoratedKey partitionKey)
        {
            super(partitionKey);
        }

        @Override
        public Clustering clustering()
        {
            return Clustering.EMPTY;
        }
    }

    /**
     * {@link PrimaryKey} implementation for static rows in tables with a defined clustering key.
     */
    static class Static extends PrimaryKey
    {
        Static(DecoratedKey partitionKey)
        {
            super(partitionKey);
        }

        @Override
        public Clustering clustering()
        {
            return Clustering.STATIC_CLUSTERING;
        }
    }

    /**
     * {@link PrimaryKey} implementation for regular rows in tables with a defined clustering key.
     */
    static class Wide extends PrimaryKey
    {
        private final Clustering clustering;

        Wide(DecoratedKey partitionKey, Clustering clustering)
        {
            super(partitionKey);
            this.clustering = clustering;
        }

        @Override
        public Clustering clustering()
        {
            return clustering;
        }
    }
}
