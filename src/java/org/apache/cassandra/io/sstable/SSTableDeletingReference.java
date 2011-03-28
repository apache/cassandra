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
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DataTracker;
import org.apache.cassandra.io.DeletionService;
import org.apache.cassandra.service.StorageService;

public class SSTableDeletingReference extends PhantomReference<SSTableReader>
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableDeletingReference.class);

    public static final int RETRY_DELAY = 10000;

    private final DataTracker tracker;
    public final Descriptor desc;
    public final Set<Component> components;
    private final long size;
    private boolean deleteOnCleanup;

    SSTableDeletingReference(DataTracker tracker, SSTableReader referent, ReferenceQueue<? super SSTableReader> q)
    {
        super(referent, q);
        this.tracker = tracker;
        this.desc = referent.descriptor;
        this.components = referent.components;
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
            // and delete will fail (on Windows) until it is.  additionally, we need to make sure to
            // delete the data file first, so on restart the others will be recognized as GCable
            StorageService.tasks.schedule(new CleanupTask(), RETRY_DELAY, TimeUnit.MILLISECONDS);
        }
    }

    private class CleanupTask implements Runnable
    {
        int attempts = 0;

        public void run()
        {
            // retry until we can successfully delete the DATA component: see above
            File datafile = new File(desc.filenameFor(Component.DATA));
            if (!datafile.delete())
            {
                if (attempts++ < DeletionService.MAX_RETRIES)
                {
                    StorageService.tasks.schedule(this, RETRY_DELAY, TimeUnit.MILLISECONDS);
                    return;
                }
                else
                {
                    logger.error("Unable to delete " + datafile + " (it will be removed on server restart)");
                    return;
                }
            }
            // let the remainder be cleaned up by delete
            SSTable.delete(desc, Sets.difference(components, Collections.singleton(Component.DATA)));
            tracker.spaceReclaimed(size);
        }
    }
}
