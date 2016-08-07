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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * {@code GroupMaker} state.
 *
 * <p>The state contains the primary key of the last row that has been processed by the previous
 * {@code GroupMaker} used. It can be passed to a {@code GroupMaker} to allow to resuming the grouping.
 * </p>
 * <p>
 * {@code GroupingState} is only used for internal paging. When a new page is requested by a client the initial state
 * will always be empty.</p>
 * <p>
 * If the state has a partition key but no clustering, it means that the previous group ended at the end of the
 * previous partition. If the clustering is not null it means that we are in the middle of a group.
 * </p>
 */
public final class GroupingState
{
    public static final GroupingState.Serializer serializer = new Serializer();

    public static final GroupingState EMPTY_STATE = new GroupingState(null, null);

    /**
     * The last row partition key.
     */
    private final ByteBuffer partitionKey;

    /**
     * The last row clustering
     */
    final Clustering clustering;

    public GroupingState(ByteBuffer partitionKey, Clustering clustering)
    {
        this.partitionKey = partitionKey;
        this.clustering = clustering;
    }

    /**
     * Returns the last row partition key or <code>null</code> if no rows has been processed yet.
     * @return the last row partition key or <code>null</code> if no rows has been processed yet
     */
    public ByteBuffer partitionKey()
    {
        return partitionKey;
    }

    /**
     * Returns the last row clustering or <code>null</code> if either no rows has been processed yet or the last
     * row was a static row.
     * @return he last row clustering or <code>null</code> if either no rows has been processed yet or the last
     * row was a static row
     */
    public Clustering clustering()
    {
        return clustering;
    }

    /**
     * Checks if the state contains a Clustering for the last row that has been processed.
     * @return <code>true</code> if the state contains a Clustering for the last row that has been processed,
     * <code>false</code> otherwise.
     */
    public boolean hasClustering()
    {
        return clustering != null;
    }

    public static class Serializer
    {
        public void serialize(GroupingState state, DataOutputPlus out, int version, ClusteringComparator comparator) throws IOException
        {
            boolean hasPartitionKey = state.partitionKey != null;
            out.writeBoolean(hasPartitionKey);
            if (hasPartitionKey)
            {
                ByteBufferUtil.writeWithVIntLength(state.partitionKey, out);
                boolean hasClustering = state.hasClustering();
                out.writeBoolean(hasClustering);
                if (hasClustering)
                    Clustering.serializer.serialize(state.clustering, out, version, comparator.subtypes());
            }
        }

        public GroupingState deserialize(DataInputPlus in, int version, ClusteringComparator comparator) throws IOException
        {
            if (!in.readBoolean())
                return GroupingState.EMPTY_STATE;

            ByteBuffer partitionKey = ByteBufferUtil.readWithVIntLength(in);
            Clustering clustering = null;
            if (in.readBoolean())
                clustering = Clustering.serializer.deserialize(in, version, comparator.subtypes());

            return new GroupingState(partitionKey, clustering);
        }

        public long serializedSize(GroupingState state, int version, ClusteringComparator comparator)
        {
            boolean hasPartitionKey = state.partitionKey != null;
            long size = TypeSizes.sizeof(hasPartitionKey);
            if (hasPartitionKey)
            {
                size += ByteBufferUtil.serializedSizeWithVIntLength(state.partitionKey);
                boolean hasClustering = state.hasClustering();
                size += TypeSizes.sizeof(hasClustering);
                if (hasClustering)
                {
                    size += Clustering.serializer.serializedSize(state.clustering, version, comparator.subtypes());
                }
            }
            return size;
        }
    }
}
