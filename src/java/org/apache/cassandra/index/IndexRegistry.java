package org.apache.cassandra.index;

import java.util.Collection;

import org.apache.cassandra.schema.IndexMetadata;

/**
 * The collection of all Index instances for a base table.
 * The SecondaryIndexManager for a ColumnFamilyStore contains an IndexRegistry
 * (actually it implements this interface at present) and Index implementations
 * register in order to:
 * i) subscribe to the stream of updates being applied to partitions in the base table
 * ii) provide searchers to support queries with the relevant search predicates
 */
public interface IndexRegistry
{
    void registerIndex(Index index);
    void unregisterIndex(Index index);

    Index getIndex(IndexMetadata indexMetadata);
    Collection<Index> listIndexes();
}
