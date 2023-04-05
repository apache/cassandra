/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils.binlog;

import java.io.File; // checkstyle: permit this import
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeletingArchiver implements BinLogArchiver
{
    private static final Logger logger = LoggerFactory.getLogger(DeletingArchiver.class);
    /**
     * The files in the chronicle queue that have already rolled
     */
    private final Queue<File> chronicleStoreFiles = new ConcurrentLinkedQueue<>();
    private final long maxLogSize;
    /**
     * The number of bytes in store files that have already rolled
     */
    private long bytesInStoreFiles;

    public DeletingArchiver(long maxLogSize)
    {
        Preconditions.checkArgument(maxLogSize > 0, "maxLogSize must be > 0");
        this.maxLogSize = maxLogSize;
    }

    /**
     * Track store files as they are added and their storage impact. Delete them if over storage limit.
     * @param cycle
     * @param file
     */
    public synchronized void onReleased(int cycle, File file)
    {
        chronicleStoreFiles.offer(file);
        //This isn't accurate because the files are sparse, but it's at least pessimistic
        bytesInStoreFiles += file.length();
        logger.debug("Chronicle store file {} rolled file size {}", file.getPath(), file.length());
        while (bytesInStoreFiles > maxLogSize & !chronicleStoreFiles.isEmpty())
        {
            File toDelete = chronicleStoreFiles.poll();
            long toDeleteLength = toDelete.length();
            if (!toDelete.delete())
            {
                logger.error("Failed to delete chronicle store file: {} store file size: {} bytes in store files: {}. " +
                             "You will need to clean this up manually or reset full query logging.",
                             toDelete.getPath(), toDeleteLength, bytesInStoreFiles);
            }
            else
            {
                bytesInStoreFiles -= toDeleteLength;
                logger.info("Deleted chronicle store file: {} store file size: {} bytes in store files: {} max log size: {}.",
                            file.getPath(), toDeleteLength, bytesInStoreFiles, maxLogSize);
            }
        }
    }

    @VisibleForTesting
    long getBytesInStoreFiles()
    {
        return bytesInStoreFiles;
    }

    public void stop()
    {
    }
}
