package org.apache.cassandra.cache;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;

public class ConcurrentLinkedHashCacheProvider implements IRowCacheProvider
{
    public ICache<DecoratedKey, ColumnFamily> create(int capacity)
    {
        return ConcurrentLinkedHashCache.create(capacity);
    }
}
