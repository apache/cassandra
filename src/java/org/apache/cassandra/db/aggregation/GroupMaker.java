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
package org.apache.cassandra.db.aggregation;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A <code>GroupMaker</code> can be used to determine if some sorted rows belongs to the same group or not.
 */
public abstract class GroupMaker
{
    /**
     * <code>GroupMaker</code> that groups all the rows together.
     */
    public static final GroupMaker GROUP_EVERYTHING = new GroupMaker()
    {
        public boolean isNewGroup(DecoratedKey partitionKey, Clustering clustering)
        {
            return false;
        }

        public boolean returnAtLeastOneRow()
        {
            return true;
        }
    };

    public static GroupMaker newInstance(ClusteringComparator comparator, int clusteringPrefixSize, GroupingState state)
    {
        return new PkPrefixGroupMaker(comparator, clusteringPrefixSize, state);
    }

    public static GroupMaker newInstance(ClusteringComparator comparator, int clusteringPrefixSize)
    {
        return new PkPrefixGroupMaker(comparator, clusteringPrefixSize);
    }

    /**
     * Checks if a given row belongs to the same group that the previous row or not.
     *
     * @param partitionKey the partition key.
     * @param clustering the row clustering key
     * @return <code>true</code> if the row belongs to the same group that the previous one, <code>false</code>
     * otherwise.
     */
    public abstract boolean isNewGroup(DecoratedKey partitionKey, Clustering clustering);

    /**
     * Specify if at least one row must be returned. If the selection is performing some aggregations on all the rows,
     * one row should be returned even if no records were processed.
     *
     * @return <code>true</code> if at least one row must be returned, <code>false</code> otherwise.
     */
    public boolean returnAtLeastOneRow()
    {
        return false;
    }

    private static final class PkPrefixGroupMaker extends GroupMaker
    {
        /**
         * The size of the clustering prefix used to make the groups
         */
        private final int clusteringPrefixSize;

        /**
         * The comparator used to compare the clustering prefixes.
         */
        private final ClusteringComparator comparator;

        /**
         * The last partition key seen
         */
        private ByteBuffer lastPartitionKey;

        /**
         * The last clustering seen
         */
        private Clustering lastClustering;

        public PkPrefixGroupMaker(ClusteringComparator comparator, int clusteringPrefixSize, GroupingState state)
        {
            this(comparator, clusteringPrefixSize);
            this.lastPartitionKey = state.partitionKey();
            this.lastClustering = state.clustering;
        }

        public PkPrefixGroupMaker(ClusteringComparator comparator, int clusteringPrefixSize)
        {
            this.comparator = comparator;
            this.clusteringPrefixSize = clusteringPrefixSize;
        }

        @Override
        public boolean isNewGroup(DecoratedKey partitionKey, Clustering clustering)
        {
            boolean isNew = false;

            // We are entering a new group if:
            // - the partition key is a new one
            // - the last clustering was not null and does not have the same prefix as the new clustering one
            if (!partitionKey.getKey().equals(lastPartitionKey))
            {
                lastPartitionKey = partitionKey.getKey();
                isNew = true;
                if (Clustering.STATIC_CLUSTERING == clustering)
                {
                    lastClustering = null;
                    return true;
                }
            }
            else if (lastClustering != null && comparator.compare(lastClustering, clustering, clusteringPrefixSize) != 0)
            {
                isNew = true;
            }

            lastClustering = clustering;
            return isNew;
        }
    }
}