package org.apache.cassandra.io.sstable;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.TimestampClock;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

public class SSTableWriterTest extends CleanupHelper {

    @Test
    public void testRecoverAndOpen() throws IOException
    {
        RowMutation rm;

        rm = new RowMutation("Keyspace1", "k1".getBytes());
        rm.add(new QueryPath("Indexed1", null, "birthdate".getBytes("UTF8")), FBUtilities.toByteArray(1L), new TimestampClock(0));
        rm.apply();
        
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Indexed1");        
        cf.addColumn(new Column("birthdate".getBytes(), FBUtilities.toByteArray(1L), new TimestampClock(0)));
        cf.addColumn(new Column("anydate".getBytes(), FBUtilities.toByteArray(1L), new TimestampClock(0)));
        
        Map<byte[], byte[]> entries = new HashMap<byte[], byte[]>();
        
        DataOutputBuffer buffer = new DataOutputBuffer();
        ColumnFamily.serializer().serializeWithIndexes(cf, buffer);
        entries.put("k2".getBytes(), Arrays.copyOf(buffer.getData(), buffer.getLength()));        
        cf.clear();
        
        cf.addColumn(new Column("anydate".getBytes(), FBUtilities.toByteArray(1L), new TimestampClock(0)));
        buffer = new DataOutputBuffer();
        ColumnFamily.serializer().serializeWithIndexes(cf, buffer);               
        entries.put("k3".getBytes(), Arrays.copyOf(buffer.getData(), buffer.getLength()));
        
        SSTableReader orig = SSTableUtils.writeRawSSTable("Keyspace1", "Indexed1", entries);        
        // whack the index to trigger the recover
        new File(orig.indexFilename()).delete();
        new File(orig.filterFilename()).delete();
        
        SSTableReader sstr = SSTableWriter.recoverAndOpen(orig.desc);
        
        ColumnFamilyStore cfs = Table.open("Keyspace1").getColumnFamilyStore("Indexed1");
        cfs.addSSTable(sstr);
        
        IndexExpression expr = new IndexExpression("birthdate".getBytes("UTF8"), IndexOperator.EQ, FBUtilities.toByteArray(1L));
        IndexClause clause = new IndexClause(Arrays.asList(expr), 100);
        IFilter filter = new IdentityQueryFilter();
        List<Row> rows = cfs.scan(clause, filter);
        
        assertEquals("IndexExpression should return two rows on recoverAndOpen",2, rows.size());
        assertTrue("First result should be 'k1'",Arrays.equals("k1".getBytes(), rows.get(0).key.key));
    }
}
