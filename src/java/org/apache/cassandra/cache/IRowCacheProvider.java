package org.apache.cassandra.cache;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;

/**
 * Provides cache objects with a requested capacity.
 */
public interface IRowCacheProvider
{
    public ICache<DecoratedKey, ColumnFamily> create(int capacity);
}
