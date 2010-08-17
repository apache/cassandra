/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.StatisticsTable;
import org.apache.cassandra.io.DeletionService;
import org.apache.cassandra.io.util.FileUtils;

public class SSTableDeletingReference extends PhantomReference<SSTableReader>
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableDeletingReference.class);

    private static final Timer timer = new Timer("SSTABLE-CLEANUP-TIMER");
    public static final int RETRY_DELAY = 10000;

    private final SSTableTracker tracker;
    public final String path;
    private final long size;
    private boolean deleteOnCleanup;

    SSTableDeletingReference(SSTableTracker tracker, SSTableReader referent, ReferenceQueue<? super SSTableReader> q)
    {
        super(referent, q);
        this.tracker = tracker;
        this.path = referent.getFilename();
        this.size = referent.bytesOnDisk();
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
            try
            {
                FileUtils.deleteWithConfirm(new File(SSTable.indexFilename(path)));
                FileUtils.deleteWithConfirm(new File(SSTable.filterFilename(path)));
                FileUtils.deleteWithConfirm(new File(SSTable.compactedFilename(path)));
                StatisticsTable.deleteSSTableStatistics(datafile.getAbsolutePath());
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            tracker.spaceReclaimed(size);
            logger.info("Deleted " + path);
        }
    }
}
