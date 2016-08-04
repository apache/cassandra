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

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Defines how rows should be grouped for creating aggregates.
 */
public abstract class AggregationSpecification
{
    public static final Serializer serializer = new Serializer();

    /**
     * <code>AggregationSpecification</code> that group all the row together.
     */
    public static final AggregationSpecification AGGREGATE_EVERYTHING = new AggregationSpecification(Kind.AGGREGATE_EVERYTHING)
    {
        @Override
        public GroupMaker newGroupMaker(GroupingState state)
        {
            return GroupMaker.GROUP_EVERYTHING;
        }
    };

    /**
     * The <code>AggregationSpecification</code> kind.
     */
    private final Kind kind;

    /**
     * The <code>AggregationSpecification</code> kinds.
     */
    public static enum Kind
    {
        AGGREGATE_EVERYTHING, AGGREGATE_BY_PK_PREFIX
    }

    /**
     * Returns the <code>AggregationSpecification</code> kind.
     * @return the <code>AggregationSpecification</code> kind
     */
    public Kind kind()
    {
        return kind;
    }

    private AggregationSpecification(Kind kind)
    {
        this.kind = kind;
    }

    /**
     * Creates a new <code>GroupMaker</code> instance.
     *
     * @return a new <code>GroupMaker</code> instance
     */
    public final GroupMaker newGroupMaker()
    {
        return newGroupMaker(GroupingState.EMPTY_STATE);
    }

    /**
     * Creates a new <code>GroupMaker</code> instance.
     *
     * @param state <code>GroupMaker</code> state
     * @return a new <code>GroupMaker</code> instance
     */
    public abstract GroupMaker newGroupMaker(GroupingState state);

    /**
     * Creates a new <code>AggregationSpecification</code> instance that will build aggregates based on primary key
     * columns.
     *
     * @param comparator the comparator used to compare the clustering prefixes
     * @param clusteringPrefixSize the number of clustering columns used to create the aggregates
     * @return a new <code>AggregationSpecification</code> instance that will build aggregates based on primary key
     * columns
     */
    public static AggregationSpecification aggregatePkPrefix(ClusteringComparator comparator, int clusteringPrefixSize)
    {
        return new AggregateByPkPrefix(comparator, clusteringPrefixSize);
    }

    /**
     * <code>AggregationSpecification</code> that build aggregates based on primary key columns
     */
    private static final class AggregateByPkPrefix extends AggregationSpecification
    {
        /**
         * The number of clustering component to compare.
         */
        private final int clusteringPrefixSize;

        /**
         * The comparator used to compare the clustering prefixes.
         */
        private final ClusteringComparator comparator;

        public AggregateByPkPrefix(ClusteringComparator comparator, int clusteringPrefixSize)
        {
            super(Kind.AGGREGATE_BY_PK_PREFIX);
            this.comparator = comparator;
            this.clusteringPrefixSize = clusteringPrefixSize;
        }

        @Override
        public GroupMaker newGroupMaker(GroupingState state)
        {
            return GroupMaker.newInstance(comparator, clusteringPrefixSize, state);
        }
    }

    public static class Serializer
    {
        public void serialize(AggregationSpecification aggregationSpec, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(aggregationSpec.kind().ordinal());
            switch (aggregationSpec.kind())
            {
                case AGGREGATE_EVERYTHING:
                    break;
                case AGGREGATE_BY_PK_PREFIX:
                    out.writeUnsignedVInt(((AggregateByPkPrefix) aggregationSpec).clusteringPrefixSize);
                    break;
                default:
                    throw new AssertionError();
            }
        }

        public AggregationSpecification deserialize(DataInputPlus in, int version, ClusteringComparator comparator) throws IOException
        {
            Kind kind = Kind.values()[in.readUnsignedByte()];
            switch (kind)
            {
                case AGGREGATE_EVERYTHING:
                    return AggregationSpecification.AGGREGATE_EVERYTHING;
                case AGGREGATE_BY_PK_PREFIX:
                    int clusteringPrefixSize = (int) in.readUnsignedVInt();
                    return AggregationSpecification.aggregatePkPrefix(comparator, clusteringPrefixSize);
                default:
                    throw new AssertionError();
            }
        }

        public long serializedSize(AggregationSpecification aggregationSpec, int version)
        {
            long size = TypeSizes.sizeof((byte) aggregationSpec.kind().ordinal());
            switch (aggregationSpec.kind())
            {
                case AGGREGATE_EVERYTHING:
                    break;
                case AGGREGATE_BY_PK_PREFIX:
                    size += TypeSizes.sizeofUnsignedVInt(((AggregateByPkPrefix) aggregationSpec).clusteringPrefixSize);
                    break;
                default:
                    throw new AssertionError();
            }
            return size;
        }
    }
}
