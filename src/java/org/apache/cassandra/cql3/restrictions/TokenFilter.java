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
package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;

import static org.apache.cassandra.cql3.statements.Bound.END;
import static org.apache.cassandra.cql3.statements.Bound.START;

/**
 * <code>Restriction</code> decorator used to merge non-token restriction and token restriction on partition keys.
 *
 * <p>If all partition key columns have non-token restrictions and do not need filtering, they take precedence
 * when calculating bounds, incusiveness etc (see CASSANDRA-12149).</p>
 */
abstract class TokenFilter implements PartitionKeyRestrictions
{
    /**
     * The decorated restriction
     */
    final PartitionKeyRestrictions restrictions;

    /**
     * The restriction on the token
     */
    final TokenRestriction tokenRestriction;

    /**
     * Partitioner to manage tokens, extracted from tokenRestriction metadata.
     */
    private final IPartitioner partitioner;

    static TokenFilter create(PartitionKeyRestrictions restrictions, TokenRestriction tokenRestriction)
    {
        boolean onToken = restrictions.needFiltering(tokenRestriction.metadata) || restrictions.size() < tokenRestriction.size();
        return onToken ? new TokenFilter.OnToken(restrictions, tokenRestriction)
                       : new TokenFilter.NotOnToken(restrictions, tokenRestriction);
    }

    private TokenFilter(PartitionKeyRestrictions restrictions, TokenRestriction tokenRestriction)
    {
        this.restrictions = restrictions;
        this.tokenRestriction = tokenRestriction;
        this.partitioner = tokenRestriction.metadata.partitioner;
    }

    private static final class OnToken extends TokenFilter
    {
        private OnToken(PartitionKeyRestrictions restrictions, TokenRestriction tokenRestriction)
        {
            super(restrictions, tokenRestriction);
        }

        @Override
        public boolean isOnToken()
        {
            return true;
        }

        @Override
        public boolean isInclusive(Bound bound)
        {
            return tokenRestriction.isInclusive(bound);
        }

        @Override
        public boolean hasBound(Bound bound)
        {
            return tokenRestriction.hasBound(bound);
        }

        @Override
        public List<ByteBuffer> bounds(Bound bound, QueryOptions options) throws InvalidRequestException
        {
            return tokenRestriction.bounds(bound, options);
        }
    }

    private static final class NotOnToken extends TokenFilter
    {
        private NotOnToken(PartitionKeyRestrictions restrictions, TokenRestriction tokenRestriction)
        {
            super(restrictions, tokenRestriction);
        }

        @Override
        public boolean isInclusive(Bound bound)
        {
            return restrictions.isInclusive(bound);
        }

        @Override
        public boolean hasBound(Bound bound)
        {
            return restrictions.hasBound(bound);
        }

        @Override
        public List<ByteBuffer> bounds(Bound bound, QueryOptions options) throws InvalidRequestException
        {
            return restrictions.bounds(bound, options);
        }

        public boolean hasIN()
        {
            return restrictions.hasIN();
        }

        public boolean hasContains()
        {
            return restrictions.hasContains();
        }

        public boolean hasOnlyEqualityRestrictions()
        {
            return restrictions.hasOnlyEqualityRestrictions();
        }
    }

    @Override
    public Set<Restriction> getRestrictions(ColumnMetadata columnDef)
    {
        Set<Restriction> set = new HashSet<>();
        set.addAll(restrictions.getRestrictions(columnDef));
        set.addAll(tokenRestriction.getRestrictions(columnDef));
        return set;
    }

    @Override
    public boolean isOnToken()
    {
        // if all partition key columns have non-token restrictions and do not need filtering,
        // we can simply use the token range to filter those restrictions and then ignore the token range
        return needFiltering(tokenRestriction.metadata) || restrictions.size() < tokenRestriction.size();
    }

    @Override
    public List<ByteBuffer> values(QueryOptions options, QueryState queryState) throws InvalidRequestException
    {
        return filter(restrictions.values(options, queryState), options, queryState);
    }

    @Override
    public PartitionKeyRestrictions mergeWith(Restriction restriction) throws InvalidRequestException
    {
        if (restriction.isOnToken())
            return TokenFilter.create(restrictions, (TokenRestriction) tokenRestriction.mergeWith(restriction));

        return TokenFilter.create(restrictions.mergeWith(restriction), tokenRestriction);
    }

    /**
     * Filter the values returned by the restriction.
     *
     * @param values the values returned by the decorated restriction
     * @param options the query options
     * @param queryState the query state
     * @return the values matching the token restriction
     * @throws InvalidRequestException if the request is invalid
     */
    private List<ByteBuffer> filter(List<ByteBuffer> values, QueryOptions options, QueryState queryState) throws InvalidRequestException
    {
        RangeSet<Token> rangeSet = tokenRestriction.hasSlice() ? toRangeSet(tokenRestriction, options)
                                                               : toRangeSet(tokenRestriction.values(options, queryState));

        return filterWithRangeSet(rangeSet, values);
    }

    /**
     * Filter out the values for which the tokens are not included within the specified range.
     *
     * @param tokens the tokens range
     * @param values the restricted values
     * @return the values for which the tokens are not included within the specified range.
     */
    private List<ByteBuffer> filterWithRangeSet(RangeSet<Token> tokens, List<ByteBuffer> values)
    {
        List<ByteBuffer> remaining = new ArrayList<>();

        for (ByteBuffer value : values)
        {
            Token token = partitioner.getToken(value);

            if (!tokens.contains(token))
                continue;

            remaining.add(value);
        }
        return remaining;
    }

    /**
     * Converts the specified list into a range set.
     *
     * @param buffers the token restriction values
     * @return the range set corresponding to the specified list
     */
    private RangeSet<Token> toRangeSet(List<ByteBuffer> buffers)
    {
        ImmutableRangeSet.Builder<Token> builder = ImmutableRangeSet.builder();

        for (ByteBuffer buffer : buffers)
            builder.add(Range.singleton(deserializeToken(buffer)));

        return builder.build();
    }

    /**
     * Converts the specified slice into a range set.
     *
     * @param slice the slice to convert
     * @param options the query option
     * @return the range set corresponding to the specified slice
     * @throws InvalidRequestException if the request is invalid
     */
    private RangeSet<Token> toRangeSet(TokenRestriction slice, QueryOptions options) throws InvalidRequestException
    {
        if (slice.hasBound(START))
        {
            Token start = deserializeToken(slice.bounds(START, options).get(0));

            BoundType startBoundType = toBoundType(slice.isInclusive(START));

            if (slice.hasBound(END))
            {
                BoundType endBoundType = toBoundType(slice.isInclusive(END));
                Token end = deserializeToken(slice.bounds(END, options).get(0));

                if (start.equals(end) && (BoundType.OPEN == startBoundType || BoundType.OPEN == endBoundType))
                    return ImmutableRangeSet.of();

                if (start.compareTo(end) <= 0)
                    return ImmutableRangeSet.of(Range.range(start,
                                                            startBoundType,
                                                            end,
                                                            endBoundType));

                return ImmutableRangeSet.<Token> builder()
                                        .add(Range.upTo(end, endBoundType))
                                        .add(Range.downTo(start, startBoundType))
                                        .build();
            }
            return ImmutableRangeSet.of(Range.downTo(start,
                                                     startBoundType));
        }
        Token end = deserializeToken(slice.bounds(END, options).get(0));
        return ImmutableRangeSet.of(Range.upTo(end, toBoundType(slice.isInclusive(END))));
    }

    /**
     * Deserializes the token corresponding to the specified buffer.
     *
     * @param buffer the buffer
     * @return the token corresponding to the specified buffer
     */
    private Token deserializeToken(ByteBuffer buffer)
    {
        return partitioner.getTokenFactory().fromByteArray(buffer);
    }

    private static BoundType toBoundType(boolean inclusive)
    {
        return inclusive ? BoundType.CLOSED : BoundType.OPEN;
    }

    @Override
    public ColumnMetadata getFirstColumn()
    {
        return restrictions.getFirstColumn();
    }

    @Override
    public ColumnMetadata getLastColumn()
    {
        return restrictions.getLastColumn();
    }

    @Override
    public List<ColumnMetadata> getColumnDefs()
    {
        return restrictions.getColumnDefs();
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        restrictions.addFunctionsTo(functions);
    }

    @Override
    public boolean hasSupportingIndex(IndexRegistry indexRegistry)
    {
        return restrictions.hasSupportingIndex(indexRegistry);
    }

    @Override
    public boolean needsFiltering(Index.Group indexGroup)
    {
        return restrictions.needsFiltering(indexGroup);
    }

    @Override
    public void addToRowFilter(RowFilter filter, IndexRegistry indexRegistry, QueryOptions options)
    {
        restrictions.addToRowFilter(filter, indexRegistry, options);
    }

    @Override
    public boolean isEmpty()
    {
        return restrictions.isEmpty();
    }

    @Override
    public int size()
    {
        return restrictions.size();
    }

    @Override
    public boolean needFiltering(TableMetadata table)
    {
        return restrictions.needFiltering(table);
    }

    @Override
    public boolean hasUnrestrictedPartitionKeyComponents(TableMetadata table)
    {
        return restrictions.hasUnrestrictedPartitionKeyComponents(table);
    }

    @Override
    public boolean hasSlice()
    {
        return restrictions.hasSlice();
    }
}
