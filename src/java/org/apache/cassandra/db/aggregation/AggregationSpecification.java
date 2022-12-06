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
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

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
     * Factory for <code>AggregationSpecification</code> that group all the row together.
     */
    public static final AggregationSpecification.Factory AGGREGATE_EVERYTHING_FACTORY = options -> AGGREGATE_EVERYTHING;

    /**
     * The <code>AggregationSpecification</code> kind.
     */
    private final Kind kind;

    /**
     * The <code>AggregationSpecification</code> kinds.
     */
    public enum Kind
    {
        AGGREGATE_EVERYTHING, AGGREGATE_BY_PK_PREFIX, AGGREGATE_BY_PK_PREFIX_WITH_SELECTOR
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
     * Creates a new {@code Factory} instance to create {@code AggregationSpecification} that will build aggregates
     * based on primary key columns.
     *
     * @param comparator the comparator used to compare the clustering prefixes
     * @param clusteringPrefixSize the number of clustering columns used to create the aggregates
     * @return a  new {@code Factory} instance to create {@code AggregationSpecification} that will build aggregates
     * based on primary key columns.
     */
    public static AggregationSpecification.Factory aggregatePkPrefixFactory(ClusteringComparator comparator,
                                                                            int clusteringPrefixSize)
    {
        return options -> new  AggregateByPkPrefix(comparator, clusteringPrefixSize);
    }

    public static AggregationSpecification.Factory aggregatePkPrefixFactoryWithSelector(final ClusteringComparator comparator,
                                                                                        final int clusteringPrefixSize,
                                                                                        final Selector.Factory factory,
                                                                                        final List<ColumnMetadata> columns)
    {
        return new Factory()
        {
            @Override
            public void addFunctionsTo(List<Function> functions)
            {
                factory.addFunctionsTo(functions);
            }

            @Override
            public AggregationSpecification newInstance(QueryOptions options)
            {
                Selector selector = factory.newInstance(options);
                selector.validateForGroupBy();
                return new  AggregateByPkPrefixWithSelector(comparator,
                                                            clusteringPrefixSize,
                                                            selector,
                                                            columns);
            }
        };
    }
    
    /**
     * Factory for {@code AggregationSpecification}.
     *
     */
    public static interface Factory
    {
        /**
         * Creates a new {@code AggregationSpecification} instance after having binded the parameters.
         *
         * @param options the query options
         * @return a new {@code AggregationSpecification} instance.
         */
        public AggregationSpecification newInstance(QueryOptions options);

        public default void addFunctionsTo(List<Function> functions)
        {
        }
    }

    /**
     * <code>AggregationSpecification</code> that build aggregates based on primary key columns
     */
    private static class AggregateByPkPrefix extends AggregationSpecification
    {
        /**
         * The number of clustering component to compare.
         */
        protected final int clusteringPrefixSize;

        /**
         * The comparator used to compare the clustering prefixes.
         */
        protected final ClusteringComparator comparator;

        public AggregateByPkPrefix(ClusteringComparator comparator, int clusteringPrefixSize)
        {
            this(Kind.AGGREGATE_BY_PK_PREFIX, comparator, clusteringPrefixSize);
        }

        protected AggregateByPkPrefix(Kind kind, ClusteringComparator comparator, int clusteringPrefixSize)
        {
            super(kind);
            this.comparator = comparator;
            this.clusteringPrefixSize = clusteringPrefixSize;
        }

        @Override
        public GroupMaker newGroupMaker(GroupingState state)
        {
            return GroupMaker.newPkPrefixGroupMaker(comparator, clusteringPrefixSize, state);
        }
    }

    /**
     * <code>AggregationSpecification</code> that build aggregates based on primary key columns using a selector.
     */
    private static final class AggregateByPkPrefixWithSelector extends AggregateByPkPrefix
    {
        /**
         * The selector.
         */
        private final Selector selector;

        /**
         * The columns used by the selector.
         */
        private final List<ColumnMetadata> columns;

        public AggregateByPkPrefixWithSelector(ClusteringComparator comparator,
                                               int clusteringPrefixSize,
                                               Selector selector,
                                               List<ColumnMetadata> columns)
        {
            super(Kind.AGGREGATE_BY_PK_PREFIX_WITH_SELECTOR, comparator, clusteringPrefixSize);
            this.selector = selector;
            this.columns = columns;
        }

        @Override
        public GroupMaker newGroupMaker(GroupingState state)
        {
            return GroupMaker.newSelectorGroupMaker(comparator, clusteringPrefixSize, selector, columns, state);
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
                    out.writeUnsignedVInt32(((AggregateByPkPrefix) aggregationSpec).clusteringPrefixSize);
                    break;
                case AGGREGATE_BY_PK_PREFIX_WITH_SELECTOR:
                    AggregateByPkPrefixWithSelector spec = (AggregateByPkPrefixWithSelector) aggregationSpec;
                    out.writeUnsignedVInt32(spec.clusteringPrefixSize);
                    Selector.serializer.serialize(spec.selector, out, version);
                    // Ideally we should serialize the columns but that will break backward compatibility.
                    // So for the moment we can rebuild the list from the prefix size as we know that there will be
                    // only one column and that its indice will be: clusteringPrefixSize - 1.
                    break;
                default:
                    throw new AssertionError("Unknow aggregation kind: " + aggregationSpec.kind());
            }
        }

        public AggregationSpecification deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            Kind kind = Kind.values()[in.readUnsignedByte()];
            switch (kind)
            {
                case AGGREGATE_EVERYTHING:
                    return AggregationSpecification.AGGREGATE_EVERYTHING;
                case AGGREGATE_BY_PK_PREFIX:
                    return new AggregateByPkPrefix(metadata.comparator, in.readUnsignedVInt32());
                case AGGREGATE_BY_PK_PREFIX_WITH_SELECTOR:
                    int clusteringPrefixSize = in.readUnsignedVInt32();
                    Selector selector = Selector.serializer.deserialize(in, version, metadata);
                    ColumnMetadata functionArgument = metadata.clusteringColumns().get(clusteringPrefixSize - 1);
                    return new AggregateByPkPrefixWithSelector(metadata.comparator,
                                                               clusteringPrefixSize,
                                                               selector,
                                                               Collections.singletonList(functionArgument));
                default:
                    throw new AssertionError("Unknow aggregation kind: " + kind);
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
                case AGGREGATE_BY_PK_PREFIX_WITH_SELECTOR:
                    AggregateByPkPrefixWithSelector spec = (AggregateByPkPrefixWithSelector) aggregationSpec;
                    size += TypeSizes.sizeofUnsignedVInt(spec.clusteringPrefixSize);
                    size += Selector.serializer.serializedSize(spec.selector, version);
                    break;
                default:
                    throw new AssertionError("Unknow aggregation kind: " + aggregationSpec.kind());
            }
            return size;
        }
    }
}
