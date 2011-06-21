package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOError;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionType;
import org.apache.cassandra.streaming.OperationType;

/**
 * Removes the given SSTable from temporary status and opens it, rebuilding the
 * bloom filter and row index from the data file.
 */
public class Rebuilder implements CompactionInfo.Holder
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    private final Descriptor desc;
    private final OperationType type;
    private final ColumnFamilyStore cfs;
    private SSTableWriter.RowIndexer indexer;

    public Rebuilder(Descriptor desc, OperationType type)
    {
        this.desc = desc;
        this.type = type;
        cfs = Table.open(desc.ksname).getColumnFamilyStore(desc.cfname);
    }

    public CompactionInfo getCompactionInfo()
    {
        maybeOpenIndexer();
        try
        {
            // both file offsets are still valid post-close
            return new CompactionInfo(desc.ksname,
                                      desc.cfname,
                                      CompactionType.SSTABLE_BUILD,
                                      indexer.dfile.getFilePointer(),
                                      indexer.dfile.length());
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    // lazy-initialize the file to avoid opening it until it's actually executing on the CompactionManager,
    // since the 8MB buffers can use up heap quickly
    private void maybeOpenIndexer()
    {
        if (indexer != null)
            return;
        try
        {
            if (cfs.metadata.getDefaultValidator().isCommutative())
                indexer = new SSTableWriter.CommutativeRowIndexer(desc, cfs, type);
            else
                indexer = new SSTableWriter.RowIndexer(desc, cfs, type);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public SSTableReader build() throws IOException
    {
        if (cfs.isInvalid())
            return null;
        maybeOpenIndexer();

        File ifile = new File(desc.filenameFor(SSTable.COMPONENT_INDEX));
        File ffile = new File(desc.filenameFor(SSTable.COMPONENT_FILTER));
        assert !ifile.exists();
        assert !ffile.exists();

        long estimatedRows = indexer.prepareIndexing();

        // build the index and filter
        long rows = indexer.index();

        logger.debug("estimated row count was {} of real count", ((double)estimatedRows) / rows);
        return SSTableReader.open(SSTableWriter.rename(desc, SSTable.componentsFor(desc, false)));
    }
}
