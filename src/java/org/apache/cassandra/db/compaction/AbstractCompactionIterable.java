package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throttle;

public abstract class AbstractCompactionIterable implements Iterable<AbstractCompactedRow>, CompactionInfo.Holder
{
    public static final int FILE_BUFFER_SIZE = 1024 * 1024;

    private static Logger logger = LoggerFactory.getLogger(CompactionIterable.class);

    protected final OperationType type;
    protected final CompactionController controller;
    protected long totalBytes;
    protected volatile long bytesRead;

    protected final Throttle throttle;

    public AbstractCompactionIterable(CompactionController controller, OperationType type)
    {
        this.controller = controller;
        this.type = type;
        this.throttle = new Throttle(toString(), new Throttle.ThroughputFunction()
        {
            /** @return Instantaneous throughput target in bytes per millisecond. */
            public int targetThroughput()
            {
                if (DatabaseDescriptor.getCompactionThroughputMbPerSec() < 1 || StorageService.instance.isBootstrapMode())
                    // throttling disabled
                    return 0;
                // total throughput
                int totalBytesPerMS = DatabaseDescriptor.getCompactionThroughputMbPerSec() * 1024 * 1024 / 1000;
                // per stream throughput (target bytes per MS)
                return totalBytesPerMS / Math.max(1, CompactionManager.instance.getActiveCompactions());
            }
        });
    }

    protected static List<SSTableScanner> getScanners(Iterable<SSTableReader> sstables) throws IOException
    {
        ArrayList<SSTableScanner> scanners = new ArrayList<SSTableScanner>();
        for (SSTableReader sstable : sstables)
            scanners.add(sstable.getDirectScanner(FILE_BUFFER_SIZE));
        return scanners;
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(this.hashCode(),
                                  controller.getKeyspace(),
                                  controller.getColumnFamily(),
                                  type,
                                  bytesRead,
                                  totalBytes);
    }

    public abstract CloseableIterator<AbstractCompactedRow> iterator();
}
