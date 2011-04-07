package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.migration.AddColumnFamily;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MeteredFlusherTest extends CleanupHelper
{
    @Test
    public void testManyMemtables() throws IOException, ConfigurationException
    {
        Table table = Table.open("Keyspace1");
        for (int i = 0; i < 100; i++)
        {
            CFMetaData metadata = new CFMetaData(table.name, "_CF" + i, ColumnFamilyType.Standard, UTF8Type.instance, null);
            new AddColumnFamily(metadata).apply();
        }

        ByteBuffer name = ByteBufferUtil.bytes("c");
        for (int j = 0; j < 200; j++)
        {
            for (int i = 0; i < 100; i++)
            {
                RowMutation rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key" + j));
                ColumnFamily cf = ColumnFamily.create("Keyspace1", "_CF" + i);
                // don't cheat by allocating this outside of the loop; that defeats the purpose of deliberately using lots of memory
                ByteBuffer value = ByteBuffer.allocate(100000);
                cf.addColumn(new Column(name, value));
                rm.add(cf);
                rm.applyUnsafe();
            }
        }

        int flushes = 0;
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            if (cfs.getColumnFamilyName().startsWith("_CF"))
                flushes += cfs.getMemtableSwitchCount();
        }
        assert flushes > 0;
    }
}

