package org.apache.cassandra.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;

import static org.apache.cassandra.Util.column;
import static org.junit.Assert.*;

public class CacheProviderTest extends SchemaLoader
{
    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    String key4 = "key4";
    String key5 = "key5";
    private static final int CAPACITY = 4;

    private void simpleCase(ColumnFamily cf, ICache<String, ColumnFamily> cache)
    {
        cache.put(key1, cf);
        assertDigests(cache.get(key1), cf);
        cache.put(key2, cf);
        cache.put(key3, cf);
        cache.put(key4, cf);
        cache.put(key5, cf);
        
        assertEquals(CAPACITY, cache.size());
    }

    private void assertDigests(ColumnFamily one, ColumnFamily two)
    {
        // CF does not implement .equals
        assert ColumnFamily.digest(one).equals(ColumnFamily.digest(two));
    }

    // TODO this isn't terribly useful
    private void concurrentCase(final ColumnFamily cf, final ICache<String, ColumnFamily> cache) throws InterruptedException
    {
        Runnable runable = new Runnable()
        {
            public void run()
            {
                for (int j = 0; j < 10; j++)
                {
                    cache.put(key1, cf);
                    cache.put(key2, cf);
                    cache.put(key3, cf);
                    cache.put(key4, cf);
                    cache.put(key5, cf);
                }
            }
        };

        List<Thread> threads = new ArrayList<Thread>(100);
        for (int i = 0; i < 100; i++)
        {
            Thread thread = new Thread(runable);
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads)
            thread.join();
    }

    private ColumnFamily createCF()
    {
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
        cf.addColumn(column("vijay", "great", 1));
        cf.addColumn(column("awesome", "vijay", 1));
        return cf;
    }
    
    @Test
    public void testHeapCache() throws InterruptedException
    {
        ICache<String, ColumnFamily> cache = ConcurrentLinkedHashCache.create(CAPACITY);
        ColumnFamily cf = createCF();
        simpleCase(cf, cache);
        concurrentCase(cf, cache);
    }
}
