package org.apache.cassandra.io;

import java.io.File;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

public class SSTableDeletingReference extends PhantomReference<SSTableReader>
{
    private static final Logger logger = Logger.getLogger(SSTableDeletingReference.class);

    private static final Timer timer = new Timer("SSTABLE-CLEANUP-TIMER");
    public static final int RETRY_DELAY = 10000;

    public final String path;
    private boolean deleteOnCleanup;

    SSTableDeletingReference(SSTableReader referent, ReferenceQueue<? super SSTableReader> q)
    {
        super(referent, q);
        this.path = referent.path;
    }

    public void deleteOnCleanup()
    {
        deleteOnCleanup = true;
    }

    public void cleanup() throws IOException
    {
        if (deleteOnCleanup)
        {
            // this is tricky because the mmapping might not have been finalized yet,
            // and delete will fail until it is.  additionally, we need to make sure to
            // delete the data file first, so on restart the others will be recognized as GCable
            // even if the compaction marker gets deleted next.
            timer.schedule(new CleanupTask(), RETRY_DELAY);
        }
    }

    private class CleanupTask extends TimerTask
    {
        int attempts = 0;

        @Override
        public void run()
        {
            File datafile = new File(path);
            if (!datafile.delete())
            {
                if (attempts++ < DeletionService.MAX_RETRIES)
                {
                    timer.schedule(this, RETRY_DELAY);
                    return;
                }
                else
                {
                    throw new RuntimeException("Unable to delete " + path);
                }
            }
            logger.info("Deleted " + path);
            DeletionService.submitDeleteWithRetry(SSTable.indexFilename(path));
            DeletionService.submitDeleteWithRetry(SSTable.filterFilename(path));
            DeletionService.submitDeleteWithRetry(SSTable.compactedFilename(path));
        }
    }
}
