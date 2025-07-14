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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;

/**
 * User-provided directives about what indexes should be used by a {@code SELECT} query.
 * See {@code IndexHints.md} for further details.
 */
public class IndexHints
{
    public static final String CONFLICTING_INDEXES_ERROR = "Indexes cannot be both included and excluded: ";
    public static final String WRONG_KEYSPACE_ERROR = "Index %s is not in the same keyspace as the queried table.";
    public static final String MISSING_INDEX_ERROR = "Table %s doesn't have an index named %s";
    public static final String NON_INCLUDABLE_INDEXES_ERROR = "It's not possible to use all the specified included indexes with this query.";
    public static final String TOO_MANY_INDEXES_ERROR = format("Cannot have more than %d included/excluded indexes, found ", Short.MAX_VALUE);

    public static final IndexHints NONE = new IndexHints(Collections.emptySet(), Collections.emptySet())
    {
        @Override
        public boolean includes(Index index)
        {
            return false;
        }

        @Override
        public boolean includes(String indexName)
        {
            return false;
        }

        @Override
        public boolean excludes(Index index)
        {
            return false;
        }

        @Override
        public boolean excludes(String indexName)
        {
            return false;
        }

        @Override
        public <T extends Index> Set<T> nonExcluded(Iterable<T> indexes)
        {
            return Sets.newHashSet(indexes);
        }

        @Override
        public void validate(@Nullable Index.QueryPlan queryPlan)
        {
            // nothing to validate
        }

        @Override
        public Comparator<Index.QueryPlan> comparator()
        {
            // no index hints, so all plans are equal in that respect
            return (x, y) -> 0;
        }
    };

    public static final Serializer serializer = new Serializer();

    /**
     * The indexes to use when executing a query.
     */
    public final Set<IndexMetadata> included;

    /**
     * The indexes not to use when executing the query.
     */
    public final Set<IndexMetadata> excluded;

    private IndexHints(Set<IndexMetadata> included, Set<IndexMetadata> excluded)
    {
        this.included = included;
        this.excluded = excluded;
    }

    /**
     * @return {@code true} if the index is included, {@code false} otherwise
     */
    public boolean includes(Index index)
    {
        return includes(index.getIndexMetadata().name);
    }

    /**
     * @return {@code true} if the index is included, {@code false} otherwise
     */
    public boolean includes(String indexName)
    {
        for (IndexMetadata i : included)
        {
            if (i.name.equals(indexName))
                return true;
        }
        return false;
    }

    /**
     * @return {@code true} if the index is excluded, {@code false} otherwise
     */
    public boolean excludes(Index index)
    {
        return excludes(index.getIndexMetadata().name);
    }

    /**
     * @return {@code true} if the index is excluded, {@code false} otherwise
     */
    public boolean excludes(String indexName)
    {
        for (IndexMetadata i : excluded)
        {
            if (i.name.equals(indexName))
                return true;
        }
        return false;
    }

    /**
     * @param indexes a set of indexes
     * @return the indexes that are not excluded by these hints
     */
    public <T extends Index> Set<T> nonExcluded(Iterable<T> indexes)
    {
        Set<T> result = new HashSet<>();
        for (T index : indexes)
        {
            if (!excludes(index))
                result.add(index);
        }
        return result;
    }

    /**
     * Returns the best of the specified indexes that satisfies the specified filter and is not excluded.
     * The order of preference to determine whether an index is better than another is:
     * <ol>
     *     <li>An index included by these hints is better than an index not included by these hints.</li>
     *     <li>An index more selective according to {@link Index#getEstimatedResultRows()} is better. This is done
     *     according to the {@link Index.QueryPlan#getEstimatedResultRows()} method. Please note that some index
     *     implementations may return fixed values to prioritize themselves.</li>
     * </ol>
     *
     * @param indexes a collection of indexes
     * @param filter a filter to apply to the indexes
     *
     * @return the best of the specified indexes that satisfies these index hints and the specified filter
     */
    public <T extends Index> Optional<T> getBestIndexFor(Collection<T> indexes, Predicate<T> filter)
    {
        // filter excluded and filtered indexes
        Collection<T> candidates = filter(indexes, index -> !excludes(index) && filter.test(index));

        // prefer included indexes
        candidates = prefer(candidates, this::includes);

        // return the candidate with the best selectivity
        return bestSelectivityIndex(candidates);
    }

    /**
     * Returns the indexes in the specified collection of indexes that satisfy the specified filter.
     *
     * @param indexes a collection of indexes
     * @param filter a filter to apply to the indexes
     * @return the indexes that satisfy the specified filter
     */
    private static <T extends Index> Collection<T> filter(Collection<T> indexes, Predicate<T> filter)
    {
        if (indexes.isEmpty())
            return indexes;

        Set<T> candidates = new HashSet<>(indexes.size());
        for (T index : indexes)
        {
            if (filter.test(index))
                candidates.add(index);
        }
        return candidates;
    }

    /**
     * Returns the indexes in the specified collection that satisfy the specified predicate, or the unmodified
     * collection if there are no indexes satisfying the predicate.
     *
     * @param indexes a collection of indexes
     * @param predicate a predicate that returns {@code true} for preferred indexes
     * @return the preferred indexes, or the unmodified collection if there are no preferred indexes
     */
    private static <T extends Index> Collection<T> prefer(Collection<T> indexes, Predicate<T> predicate)
    {
        if (indexes.isEmpty() || indexes.size() == 1)
            return indexes;

        Collection<T> preferred = filter(indexes, predicate);
        return preferred.isEmpty() ? indexes : preferred;
    }

    /**
     * Returns the index with the best selectivity from the specified collection of indexes.
     * </p>
     * The selectivity is determined by the {@link Index#getEstimatedResultRows()} method. Please note that SAI and SASI
     * will always return -1 for that method, to force their selection. They will later use their own internal planning
     * when queried. The index selectivity will still be used for legacy indexes, and potentially for 3rd party
     * implementations.
     *
     * @param indexes a collection of indexes
     * @return the index with the best selectivity, according to {@link Index#getEstimatedResultRows()}
     */
    private static <T extends Index> Optional<T> bestSelectivityIndex(Collection<T> indexes)
    {
        if (indexes.isEmpty())
            return Optional.empty();

        if (indexes.size() == 1)
            return Optional.of(Iterables.getOnlyElement(indexes));

        T bestIndex = null;
        long bestCardinality = Long.MAX_VALUE;
        for (T index : indexes)
        {
            long cardinality = index.getEstimatedResultRows();
            if (bestIndex == null || cardinality < bestCardinality)
            {
                bestIndex = index;
                bestCardinality = cardinality;
            }
        }
        return Optional.of(bestIndex);
    }

    /**
     * Creates a new instance of {@link IndexHints} with the specified included and excluded indexes.
     *
     * @param included the indexes to include when executing the query
     * @param excluded the indexes to exclude when executing the query
     * @return a new instance of {@link IndexHints}
     */
    public static IndexHints create(Set<IndexMetadata> included, Set<IndexMetadata> excluded)
    {
        if ((included == null || included.isEmpty()) && (excluded == null || excluded.isEmpty()))
            return NONE;

        if (included == null)
            included = Collections.emptySet();
        if (excluded == null)
            excluded = Collections.emptySet();

        return new IndexHints(included, excluded);
    }

    /**
     * Validates these index hints for the specified index query plan, to verify that all the included indexes can be
     * selected. This might happen if the query doesn't have expressions for each of the included indexes, or if it has
     * them but the index implementation hasn't been able to use them for whatever reason.
     *
     * @param queryPlan the index query plan, which should have been built accordingly to these hints
     */
    public void validate(@Nullable Index.QueryPlan queryPlan)
    {
        if (queryPlan == null)
        {
            if (included.isEmpty())
                return;
            else
                throw new InvalidRequestException(NON_INCLUDABLE_INDEXES_ERROR);
        }

        for (IndexMetadata indexMetadata : included)
        {
            boolean found = false;
            for (Index i : queryPlan.getIndexes())
            {
                if (i.getIndexMetadata().equals(indexMetadata))
                {
                    found = true;
                    break;
                }
            }
            if (!found)
                throw new InvalidRequestException(NON_INCLUDABLE_INDEXES_ERROR);
        }

        // excluded indexes should never be included because the query plans are built from a filtered list
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexHints that = (IndexHints) o;
        return Objects.equals(included, that.included) &&
               Objects.equals(excluded, that.excluded);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(included, excluded);
    }

    /**
     * Returns the index hints represented by the specified sets of CQL names for the specified queried table.
     * </p>
     * There shouldn't be more included or excluded indexes than can fit in a short, otherwise an
     * {@link InvalidRequestException} will be thrown.
     * </p>
     * All the mentioned indexes should exist in the index registry of the queried table,
     * or an {@link InvalidRequestException} will be thrown.
     *
     * @param included the names of the indexes to include when executing the query
     * @param excluded the names of the indexes to exclude when executing the query
     * @param table the queried table
     * @param indexRegistry the index registry of the queried table
     * @return the index hints represented by the specified sets of CQL names
     * @throws InvalidRequestException if any of the specified indexes do not exist in the specified index registry
     */
    public static IndexHints fromCQLNames(Set<QualifiedName> included,
                                          Set<QualifiedName> excluded,
                                          TableMetadata table,
                                          IndexRegistry indexRegistry)
    {
        if (included != null && included.size() > Short.MAX_VALUE)
            throw new InvalidRequestException(TOO_MANY_INDEXES_ERROR + included.size());

        if (excluded != null && excluded.size() > Short.MAX_VALUE)
            throw new InvalidRequestException(TOO_MANY_INDEXES_ERROR + excluded.size());

        IndexHints hints = IndexHints.create(fetchIndexes(included, table, indexRegistry),
                                             fetchIndexes(excluded, table, indexRegistry));

        if (hints == IndexHints.NONE)
            return hints;

        // Ensure that no index is both included and excluded
        Set<IndexMetadata> conflictingIndexes = Sets.intersection(hints.included, hints.excluded);
        if (!conflictingIndexes.isEmpty())
        {
            throw new InvalidRequestException(CONFLICTING_INDEXES_ERROR + IndexMetadata.joinNames(conflictingIndexes));
        }

        // Ensure that all nodes in the cluster are in a version that supports index hints, including this one
        Set<InetAddressAndPort> badNodes = MessagingService.instance().endpointsWithConnectionsOnVersionBelow(MessagingService.VERSION_51);
        if (MessagingService.current_version < MessagingService.VERSION_51)
            badNodes.add(FBUtilities.getBroadcastAddressAndPort());
        if (!badNodes.isEmpty())
            throw new InvalidRequestException("Index hints are not supported in clusters below 14.");

        return hints;
    }

    private static Set<IndexMetadata> fetchIndexes(Set<QualifiedName> indexNames, TableMetadata table, IndexRegistry indexRegistry)
    {
        if (indexNames == null || indexNames.isEmpty())
            return Collections.emptySet();

        Set<IndexMetadata> indexes = new HashSet<>(indexNames.size());

        for (QualifiedName indexName : indexNames)
        {
            IndexMetadata index = fetchIndex(indexName, table, indexRegistry);
            indexes.add(index);
        }

        return indexes;
    }

    private static IndexMetadata fetchIndex(QualifiedName indexName, TableMetadata table, IndexRegistry indexRegistry)
    {
        String name = indexName.getName();
        String keyspace = indexName.getKeyspace();

        if (keyspace != null && !table.keyspace.equals(keyspace))
            throw new InvalidRequestException(format(WRONG_KEYSPACE_ERROR, indexName));

        Index index = indexRegistry.getIndexByName(name);
        if (index == null)
            throw new InvalidRequestException(format(MISSING_INDEX_ERROR, table.name, name));

        return index.getIndexMetadata();
    }

    /**
     * Returns a comparator of index query plans based on which one has the most included indexes, so it can be used to
     * select the plans that satisfy the index hints first, and the plans that are closest to satisfy them later.
     *
     * @return a comparator of index query plans based on which one has the most included indexes
     */
    public Comparator<Index.QueryPlan> comparator()
    {
        return Comparator.comparing(plan -> Sets.intersection(included, metadata(plan.getIndexes())).size());
    }

    @Override
    public String toString()
    {
        return "IndexHints{" +
               "included=" + IndexMetadata.joinNames(included) +
               ", excluded=" + IndexMetadata.joinNames(excluded) +
               '}';
    }

    private static Set<IndexMetadata> metadata(Collection<Index> indexes)
    {
        Set<IndexMetadata> metadata = new HashSet<>(indexes.size());
        for (Index index : indexes)
            metadata.add(index.getIndexMetadata());
        return metadata;
    }

    /**
     * Serializer for {@link IndexHints}.
     * </p>
     * This serializer writes a byte containing bit flags that indicate which types of hints are present, allowing the
     * future addition of new types of hints without necessarily increasing the messaging version. We should be able to
     * create compatible messages in the future if we add new types of hints, and those are not explicitly set in the
     * user query. If we receive a message with unknown newer types of hints from a newer node, we will reject it.
     * </p>
     * Also, the bit flags are used to skip writing empty sets of indexes, which is the common case.
     */
    public static class Serializer
    {
        /** Bit flags mask to check if there are included indexes. */
        private static final short INCLUDED_MASK = 1;

        /** Bit flags mask to check if there are excluded indexes. */
        private static final short EXCLUDED_MASK = 2;

        /** Bit flags mask to check if there are any unknown hints. It's the negation of all the known flags. */
        private static final short UNKNOWN_HINTS_MASK = ~(INCLUDED_MASK | EXCLUDED_MASK);

        private static final IndexSetSerializer indexSetSerializer = new IndexSetSerializer();

        public void serialize(IndexHints hints, DataOutputPlus out, int version) throws IOException
        {
            // index hints are only supported in 14 and above, so don't serialize anything if the messaging version is lower
            if (version < MessagingService.VERSION_51)
            {
                if (hints != NONE)
                    throw new IllegalStateException("Unable to serialize index hints with messaging version: " + version);
                return;
            }

            byte flags = flags(hints);
            out.writeByte(flags);

            indexSetSerializer.serialize(hints.included, out, version);
            indexSetSerializer.serialize(hints.excluded, out, version);
        }

        public IndexHints deserialize(DataInputPlus in, int version, TableMetadata table) throws IOException
        {
            // index hints are only supported in 14 and above, so don't read anything if the messaging version is lower
            if (version < MessagingService.VERSION_51)
                return IndexHints.NONE;

            // read the flags first to determine which types of hints are present
            byte flags = in.readByte();

            // Reject any flags for unknown hints that may have been written by a node running newer code.
            if ((flags & UNKNOWN_HINTS_MASK) != 0)
                throw new IOException("Found unsupported index hints, likely due to the index hints containing " +
                                      "new types of hint that are not supported by this node.");

            // read included and excluded indexes
            Set<IndexMetadata> included = hasIncluded(flags) ? indexSetSerializer.deserialize(in, version, table) : Collections.emptySet();
            Set<IndexMetadata> excluded = hasExcluded(flags) ? indexSetSerializer.deserialize(in, version, table) : Collections.emptySet();

            return IndexHints.create(included, excluded);
        }

        public long serializedSize(IndexHints hints, int version)
        {
            // index hints are only supported in 14 and above, so no size if the messaging version is lower
            if (version < MessagingService.VERSION_51)
                return 0;

            // size of flags
            long size = TypeSizes.BYTE_SIZE;

            // size of included and excluded indexes
            size += indexSetSerializer.serializedSize(hints.included, version);
            size += indexSetSerializer.serializedSize(hints.excluded, version);

            return size;
        }

        private static byte flags(IndexHints hints)
        {
            if (hints == NONE)
                return 0;

            byte flags = 0;
            if (!hints.included.isEmpty())
                flags |= INCLUDED_MASK;

            if (!hints.excluded.isEmpty())
                flags |= EXCLUDED_MASK;

            return flags;
        }

        private static boolean hasIncluded(int flags)
        {
            return (flags & INCLUDED_MASK) == INCLUDED_MASK;
        }

        private static boolean hasExcluded(int flags)
        {
            return (flags & EXCLUDED_MASK) == EXCLUDED_MASK;
        }
    }

    /**
     * Serializer for a set of indexes. Nothing is written if the set is empty. Otherwise, we write first the number of
     * indexes and then the indexes themselves. Each index is represented by the serialization of its metadata.
     */
    private static class IndexSetSerializer
    {
        private void serialize(Set<IndexMetadata> indexes, DataOutputPlus out, int version) throws IOException
        {
            if (indexes.isEmpty())
                return;

            int n = indexes.size();
            assert n < Short.MAX_VALUE : TOO_MANY_INDEXES_ERROR + n;

            out.writeShort(n);
            for (IndexMetadata index : indexes)
                IndexMetadata.serializer.serialize(index, out, version);
        }

        private Set<IndexMetadata> deserialize(DataInputPlus in, int version, TableMetadata table) throws IOException
        {
            short n = in.readShort();
            Set<IndexMetadata> indexes = new HashSet<>(n);
            for (short i = 0; i < n; i++)
            {
                IndexMetadata metadata = IndexMetadata.serializer.deserialize(in, version, table);
                indexes.add(metadata);
            }
            return indexes;
        }

        private long serializedSize(Set<IndexMetadata> indexes, int version)
        {
            if (indexes.isEmpty())
                return 0;

            long size = TypeSizes.SHORT_SIZE;
            for (IndexMetadata index : indexes)
                size += IndexMetadata.serializer.serializedSize(index, version);
            return size;
        }
    }
}
