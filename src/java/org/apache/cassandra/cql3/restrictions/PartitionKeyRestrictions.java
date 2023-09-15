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
import java.util.*;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.service.ClientState;

/**
 * A set of restrictions on the partition key.
 *
 */
final class PartitionKeyRestrictions extends RestrictionSetWrapper
{
    /**
     * The composite type.
     */
    private final ClusteringComparator comparator;

    /**
     * The token restrictions or {@code null} if there are no restritions on tokens.
     */
    private final SingleRestriction tokenRestrictions;


    @Override
    public boolean isOnToken()
    {
        // if all partition key columns have non-token restrictions and do not need filtering,
        // we can simply use the token range to filter those restrictions and then ignore the token range
        return tokenRestrictions != null && (restrictions.isEmpty() || needFiltering());
    }

    public PartitionKeyRestrictions(ClusteringComparator comparator)
    {
        super(RestrictionSet.empty());
        this.comparator = comparator;
        this.tokenRestrictions = null;
    }

    private PartitionKeyRestrictions(PartitionKeyRestrictions pkRestrictions,
                                     SingleRestriction restriction)
    {
        super(restriction.isOnToken() ? pkRestrictions.restrictions
                                      : pkRestrictions.restrictions.addRestriction(restriction));
        this.comparator = pkRestrictions.comparator;
        this.tokenRestrictions = restriction.isOnToken() ? pkRestrictions.tokenRestrictions == null ? restriction
                                                                                                    : pkRestrictions.tokenRestrictions.mergeWith(restriction)
                                                         : pkRestrictions.tokenRestrictions;
    }

    public PartitionKeyRestrictions mergeWith(Restriction restriction)
    {
        return new PartitionKeyRestrictions(this, (SingleRestriction) restriction);
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        if (tokenRestrictions != null)
            tokenRestrictions.addFunctionsTo(functions);
        super.addFunctionsTo(functions);
    }

    /**
     * Returns the partitions selected by this set of restrictions
     *
     * @param partitioner the partitioner
     * @param options the query options
     * @param state the client state
     * @return the partitions selected by this set of restrictions
     */
    public List<ByteBuffer> values(IPartitioner partitioner, QueryOptions options, ClientState state)
    {
        // if we need to perform filtering its means that this query is a partition range query and that
        // this method should not be called
        if (isEmpty() || needFiltering())
            throw new IllegalStateException("the query is a partition range query and this method should not be called");

        List<ByteBuffer> nonTokenRestrictionValues = nonTokenRestrictionValues(options, state);

        if (tokenRestrictions == null)
            return nonTokenRestrictionValues;

        return filter(partitioner, nonTokenRestrictionValues, options);
    }

    /**
     * Returns the range of partitions selected by this set of restrictions
     *
     * @param partitioner the partitioner
     * @param options the query options
     * @return the range of partitions selected by this set of restrictions
     */
    public AbstractBounds<PartitionPosition> bounds(IPartitioner partitioner, QueryOptions options)
    {
        if (tokenRestrictions != null && (restrictions.isEmpty() || needFiltering()))
        {
            RangeSet<Token> tokenRangeSet = toRangeSet(partitioner, tokenRestrictions, options);
            Set<Range<Token>> ranges = tokenRangeSet.asRanges();

            if (ranges.isEmpty())
                return null;

            assert ranges.size() == 1; // We should only have 1 range.
            Range<Token> range = ranges.iterator().next();
            Token startToken = range.hasLowerBound() ? range.lowerEndpoint() : partitioner.getMinimumToken();
            Token endToken = range.hasUpperBound() ? range.upperEndpoint() : partitioner.getMinimumToken();

            boolean includeStart = range.hasLowerBound() && range.lowerBoundType() == BoundType.CLOSED;
            boolean includeEnd = range.hasUpperBound() && range.upperBoundType() == BoundType.CLOSED;

            /*
             * If we ask SP.getRangeSlice() for (token(200), token(200)], it will happily return the whole ring.
             * However, wrapping range doesn't really make sense for CQL, and we want to return an empty result in that
             * case (CASSANDRA-5573). So special case to create a range that is guaranteed to be empty.
             *
             * In practice, we want to return an empty result set if either startToken > endToken, or both are equal but
             * one of the bound is excluded (since [a, a] can contain something, but not (a, a], [a, a) or (a, a)).
             * Note though that in the case where startToken or endToken is the minimum token, then this special case
             * rule should not apply.
             */
            int cmp = startToken.compareTo(endToken);
            if (!startToken.isMinimum() && !endToken.isMinimum()
                && (cmp > 0 || (cmp == 0 && (!includeStart || !includeEnd))))
                return null;

            PartitionPosition start = includeStart ? startToken.minKeyBound() : startToken.maxKeyBound();
            PartitionPosition end = includeEnd ? endToken.maxKeyBound() : endToken.minKeyBound();

            return new org.apache.cassandra.dht.Range<>(start, end);
        }

        // If we do not have a token restrictions, we should only end up there if there is no restrictions or filtering is required.
        if (restrictions.isEmpty())
            return new Bounds<>(partitioner.getMinimumToken().minKeyBound() , partitioner.getMinimumToken().minKeyBound());

        if (needFiltering())
            return new org.apache.cassandra.dht.Range<>(partitioner.getMinimumToken().minKeyBound(), partitioner.getMinimumToken().maxKeyBound());

        // the request is for an index query for a single partition
        ByteBuffer partitionKey = nonTokenRestrictionValues(options, null).get(0);
        PartitionPosition position = PartitionPosition.ForKey.get(partitionKey, partitioner);
        return new Bounds<>(position, position);
    }

    /**
     * Computes the partition key values selected by the non-token restrictions.
     *
     * @param options the query options
     * @param state the client state used to check guardrails
     * @return the partition key values selected by the non-token restrictions.
     */
    private List<ByteBuffer> nonTokenRestrictionValues(QueryOptions options, ClientState state)
    {
        MultiCBuilder builder = MultiCBuilder.create(comparator, hasIN());
        for (SingleRestriction r : restrictions)
        {
            builder.addAllElementsToAll(r.values(options));

            if (Guardrails.inSelectCartesianProduct.enabled(state))
                Guardrails.inSelectCartesianProduct.guard(builder.buildSize(), "partition key", false, state);

            if (builder.hasMissingElements())
                break;
        }
        return toByteBuffers(builder.build());
    }

    private List<ByteBuffer> toByteBuffers(SortedSet<? extends ClusteringPrefix<?>> clusterings)
    {
        List<ByteBuffer> l = new ArrayList<>(clusterings.size());
        for (ClusteringPrefix<?> clustering : clusterings)
        {
            // Can not use QueryProcessor.validateKey here to validate each column as that validates that empty are not allowed
            // but composite partition keys actually allow empty!
            clustering.validate();
            l.add(clustering.serializeAsPartitionKey());
        }
        return l;
    }

    @Override
    public int size()
    {
        // If the token restriction is not null we know that all the partition key columns are restricted.
        return tokenRestrictions == null ? restrictions.size() : comparator.size() ;
    }

    /**
     * Use the token restrictions to filter the values returned by the non-token restrictions.
     *
     * @param partitioner the partitioner
     * @param values the values returned by the non-token restrictions
     * @param options the query options
     * @return the values matching the token restriction
     */
    private List<ByteBuffer> filter(IPartitioner partitioner, List<ByteBuffer> values, QueryOptions options)
    {
        RangeSet<Token> rangeSet = tokenRestrictions.isSlice() ? toRangeSet(partitioner, tokenRestrictions, options)
                                                               : toRangeSet(partitioner, tokenRestrictions.values(options));

        return filterWithRangeSet(partitioner, rangeSet, values);
    }

    /**
     * Filter out the values for which the tokens are not included within the specified range.
     *
     * @param partitioner the partitioner
     * @param tokens the tokens range
     * @param values the restricted values
     * @return the values for which the tokens are not included within the specified range.
     */
    private List<ByteBuffer> filterWithRangeSet(IPartitioner partitioner, RangeSet<Token> tokens, List<ByteBuffer> values)
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
     * @param partitioner the partitioner
     * @param values the token restriction values
     * @return the range set corresponding to the specified list
     */
    private RangeSet<Token> toRangeSet(IPartitioner partitioner, List<ValueList> values)
    {
        TokenFactory tokenFactory = partitioner.getTokenFactory();

        ImmutableRangeSet.Builder<Token> builder = ImmutableRangeSet.builder();

        for (List<ByteBuffer> value : values)
            builder.add(Range.singleton(tokenFactory.fromByteArray(value.get(0))));

        return builder.build();
    }

    /**
     * Converts the specified slice into a range set.
     *
     * @param partitioner the partitioner
     * @param slice the slice to convert
     * @param options the query option
     * @return the range set corresponding to the specified slice
     * @throws InvalidRequestException if the request is invalid
     */
    private RangeSet<Token> toRangeSet(IPartitioner partitioner, SingleRestriction slice, QueryOptions options)
    {
        RangeSet<ValueList> rangeSet = TreeRangeSet.create();
        rangeSet.add(Range.all());
        rangeSet = slice.restrict(rangeSet, options);

        ImmutableRangeSet.Builder<Token> builder = ImmutableRangeSet.builder();

        TokenFactory tokenFactory = partitioner.getTokenFactory();

        for (Range<ValueList> range : rangeSet.asRanges())
        {
            Range<Token> tokenRange = toTokenRange(tokenFactory, range);

            builder.add(tokenRange);
        }

        return builder.build();
    }

    private static Range<Token> toTokenRange(TokenFactory tokenFactory, Range<ValueList> range)
    {
        if (!range.hasLowerBound())
        {
            return !range.hasUpperBound() ? Range.all()
                                          : Range.upTo(tokenFactory.fromByteArray(range.upperEndpoint().get(0)), range.upperBoundType());
        }

        if (!range.hasUpperBound())
            return Range.downTo(tokenFactory.fromByteArray(range.lowerEndpoint().get(0)), range.lowerBoundType());

        return Range.range(tokenFactory.fromByteArray(range.lowerEndpoint().get(0)),
                           range.lowerBoundType(),
                           tokenFactory.fromByteArray(range.upperEndpoint().get(0)),
                           range.upperBoundType());
    }

    @Override
    public ColumnMetadata firstColumn()
    {
        return  isOnToken() ? tokenRestrictions.firstColumn() : restrictions.firstColumn();
    }

    @Override
    public ColumnMetadata lastColumn()
    {
        return  isOnToken() ? tokenRestrictions.lastColumn() : restrictions.lastColumn();
    }

    @Override
    public List<ColumnMetadata> columns()
    {
        return tokenRestrictions != null ? tokenRestrictions.columns() : restrictions.columns();
    }

    /**
     * checks if specified restrictions require filtering
     *
     * @return {@code true} if filtering is required, {@code false} otherwise
     */
    public boolean needFiltering()
    {
        if (isEmpty())
            return false;

        // has unrestricted key components or some restrictions that require filtering
        return hasUnrestrictedPartitionKeyComponents() || restrictions.needsFilteringOrIndexing();
    }

    /**
     * Checks if the partition key has unrestricted components.
     *
     * @return <code>true</code> if the partition key has unrestricted components, <code>false</code> otherwise.
     */
    public boolean hasUnrestrictedPartitionKeyComponents()
    {
        return restrictions.size() < comparator.size();
    }
}
