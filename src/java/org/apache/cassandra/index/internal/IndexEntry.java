package org.apache.cassandra.index.internal;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;

/**
 * Entries in indexes on non-compact tables (tables with composite comparators)
 * can be encapsulated as IndexedEntry instances. These are not used when dealing
 * with indexes on static/compact/thrift tables (i.e. KEYS indexes).
 */
public final class IndexEntry
{
    public final DecoratedKey indexValue;
    public final Clustering indexClustering;
    public final long timestamp;

    public final ByteBuffer indexedKey;
    public final Clustering indexedEntryClustering;

    public IndexEntry(DecoratedKey indexValue,
                      Clustering indexClustering,
                      long timestamp,
                      ByteBuffer indexedKey,
                      Clustering indexedEntryClustering)
    {
        this.indexValue = indexValue;
        this.indexClustering = indexClustering;
        this.timestamp = timestamp;
        this.indexedKey = indexedKey;
        this.indexedEntryClustering = indexedEntryClustering;
    }
}
