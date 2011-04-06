package org.apache.cassandra.cache;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;

import com.sun.jna.Memory;

public class SerializingCacheProvider implements IRowCacheProvider
{
    public SerializingCacheProvider() throws ConfigurationException
    {
        try
        {
            Memory.class.getName();
        }
        catch (NoClassDefFoundError e)
        {
            throw new ConfigurationException("Cannot intialize SerializationCache without JNA in the class path");
        }
    }

    public ICache<DecoratedKey, ColumnFamily> create(int capacity)
    {
        return new SerializingCache<DecoratedKey, ColumnFamily>(capacity, ColumnFamily.serializer());
    }
}
