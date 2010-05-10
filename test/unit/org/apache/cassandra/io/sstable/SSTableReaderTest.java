package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.cassandra.Util;

import static org.junit.Assert.assertEquals;

public class SSTableReaderTest extends CleanupHelper
{
    @Test
    public void testSpannedIndexPositions() throws IOException, ExecutionException, InterruptedException
    {
        RowIndexedReader.BUFFER_SIZE = 40; // each index entry is ~11 bytes, so this will generate lots of spanned entries

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        // insert a bunch of data and compact to a single sstable
        CompactionManager.instance.disableAutoCompaction();
        for (int j = 0; j < 100; j += 2)
        {
            byte[] key = String.valueOf(j).getBytes();
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath("Standard1", null, "0".getBytes()), new byte[0], j);
            rm.apply();
        }
        store.forceBlockingFlush();
        CompactionManager.instance.submitMajor(store).get();

        // check that all our keys are found correctly
        SSTableReader sstable = store.getSSTables().iterator().next();
        for (int j = 0; j < 100; j += 2)
        {
            DecoratedKey dk = Util.dk(String.valueOf(j));
            FileDataInput file = sstable.getFileDataInput(dk, DatabaseDescriptor.getIndexedReadBufferSizeInKB() * 1024);
            DecoratedKey keyInDisk = sstable.getPartitioner().convertFromDiskFormat(FBUtilities.readShortByteArray(file));
            assert keyInDisk.equals(dk) : String.format("%s != %s in %s", keyInDisk, dk, file.getPath());
        }

        // check no false positives
        for (int j = 1; j < 110; j += 2)
        {
            DecoratedKey dk = Util.dk(String.valueOf(j));
            assert sstable.getPosition(dk) == null;
        }

        // check positionsize information
        assert sstable.indexSummary.getSpannedIndexDataPositions().entrySet().size() > 0;
        for (Map.Entry<IndexSummary.KeyPosition, SSTable.PositionSize> entry : sstable.indexSummary.getSpannedIndexDataPositions().entrySet())
        {
            IndexSummary.KeyPosition kp = entry.getKey();
            SSTable.PositionSize info = entry.getValue();

            long nextIndexPosition = kp.indexPosition + 2 + StorageService.getPartitioner().convertToDiskFormat(kp.key).length + 8;
            BufferedRandomAccessFile indexFile = new BufferedRandomAccessFile(sstable.indexFilename(), "r");
            indexFile.seek(nextIndexPosition);
            String nextKey = indexFile.readUTF();

            BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.getFilename(), "r");
            file.seek(info.position + info.size);
            assertEquals(nextKey, file.readUTF());
        }
    }
}
