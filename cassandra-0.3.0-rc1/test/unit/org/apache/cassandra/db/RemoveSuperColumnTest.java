package org.apache.cassandra.db;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.List;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import static org.junit.Assert.assertNull;

public class RemoveSuperColumnTest
{
    @Test
    public void testRemoveSuperColumn() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Table1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Super1");
        RowMutation rm;

        // add data
        rm = new RowMutation("Table1", "key1");
        rm.add("Super1:SC1:Column1", "asdf".getBytes(), 0);
        rm.apply();
        store.forceBlockingFlush();

        // remove
        rm = new RowMutation("Table1", "key1");
        rm.delete("Super1:SC1", 1);
        rm.apply();

        List<ColumnFamily> families = store.getColumnFamilies("key1", "Super1", new IdentityFilter());
        assert families.size() == 2 : StringUtils.join(families, ", ");
        assert families.get(0).getAllColumns().first().getMarkedForDeleteAt() == 1; // delete marker, just added
        assert !families.get(1).getAllColumns().first().isMarkedForDelete(); // flushed old version
        ColumnFamily resolved = ColumnFamily.resolve(families);
        assert resolved.getAllColumns().first().getMarkedForDeleteAt() == 1;
        Collection<IColumn> subColumns = resolved.getAllColumns().first().getSubColumns();
        assert subColumns.size() == 1;
        assert subColumns.iterator().next().timestamp() == 0;
        assertNull(ColumnFamilyStore.removeDeleted(resolved, Integer.MAX_VALUE));
    }
}
