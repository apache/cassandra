package org.apache.cassandra.cache;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.Pair;

public class AutoSavingKeyCache<K extends Pair<Descriptor, DecoratedKey>, V> extends AutoSavingCache<K, V>
{
    public AutoSavingKeyCache(String tableName, String cfName, int capacity)
    {
        super(tableName, cfName, ColumnFamilyStore.CacheType.KEY_CACHE_TYPE, capacity);
    }

    @Override
    public double getConfiguredCacheSize(CFMetaData cfm)
    {
        return cfm == null ? CFMetaData.DEFAULT_KEY_CACHE_SIZE : cfm.getKeyCacheSize();
    }

    @Override
    public ByteBuffer translateKey(K key)
    {
        return key.right.key;
    }
}
