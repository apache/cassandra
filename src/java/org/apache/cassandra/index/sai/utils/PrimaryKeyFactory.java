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

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;

/**
 * A factory for creating instances that are sortable by {@link DecoratedKey} and {@link Clustering}.
 */
public class PrimaryKeyFactory
{
    private final ClusteringComparator clusteringComparator;

    public PrimaryKeyFactory(ClusteringComparator clusteringComparator)
    {
        this.clusteringComparator = clusteringComparator;
    }

    /**
     * Creates a {@link PrimaryKey} that is represented by a {@link Token}.
     *
     * {@link Token} only primary keys are used for defining the partition range
     * of a query.
     */
    public PrimaryKey createTokenOnly(Token token)
    {
        return new PrimaryKey(token);
    }

    /**
     * Creates a {@link PrimaryKey} that is fully represented by partition key
     * and clustering.
     */
    public PrimaryKey create(DecoratedKey partitionKey, Clustering<?> clustering)
    {
        return new PrimaryKey(partitionKey.getToken(), partitionKey, clustering, clusteringComparator);
    }
}
