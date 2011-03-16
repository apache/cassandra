package org.apache.cassandra.cache;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;

public class AutoSavingRowCache<K extends DecoratedKey, V> extends AutoSavingCache<K, V>
{
    public AutoSavingRowCache(String tableName, String cfName, int capacity)
    {
        super(tableName, cfName, ColumnFamilyStore.CacheType.ROW_CACHE_TYPE, capacity);
    }

    @Override
    public double getConfiguredCacheSize(CFMetaData cfm)
    {
        return cfm == null ? CFMetaData.DEFAULT_ROW_CACHE_SIZE : cfm.getRowCacheSize();
    }

    @Override
    public ByteBuffer translateKey(K key)
    {
        return key.key;
    }
}
