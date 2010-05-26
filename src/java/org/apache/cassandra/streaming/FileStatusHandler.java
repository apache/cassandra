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

package org.apache.cassandra.streaming;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

/**
 * This is the callback handler that is invoked on the receiving node when a file changes status from RECEIVE to either
 * FileStatus.STREAM (needs to be restreamed) or FileStatus.DELETE (successfully completed).
*/
class FileStatusHandler
{
    private static Logger logger = LoggerFactory.getLogger(FileStatusHandler.class);

    public void onStatusChange(InetAddress host, PendingFile pendingFile, FileStatus streamStatus) throws IOException
    {
        if (FileStatus.Action.STREAM == streamStatus.getAction())
        {
            // file needs to be restreamed
            logger.warn("Streaming of file " + pendingFile + " from " + host + " failed: requesting a retry.");
            MessagingService.instance.sendOneWay(streamStatus.makeStreamStatusMessage(), host);
            return;
        }
        assert FileStatus.Action.DELETE == streamStatus.getAction() :
            "Unknown stream action: " + streamStatus.getAction();

        // file was successfully streamed: if it was the last component of an sstable, assume that the rest
        // have already arrived
        if (pendingFile.getFilename().endsWith("-Data.db"))
        {
            // last component triggers add: see TODO in SSTable.getAllComponents()
            String tableName = pendingFile.getDescriptor().ksname;
            File file = new File(pendingFile.getFilename());
            String fileName = file.getName();
            String [] temp = fileName.split("-");

            try
            {
                SSTableReader sstable = SSTableWriter.renameAndOpen(pendingFile.getFilename());
                Table.open(tableName).getColumnFamilyStore(temp[0]).addSSTable(sstable);
                logger.info("Streaming added " + sstable.getFilename());
            }
            catch (IOException e)
            {
                throw new RuntimeException("Not able to add streamed file " + pendingFile.getFilename(), e);
            }
        }

        // send a StreamStatus message telling the source node it can delete this file
        if (logger.isDebugEnabled())
            logger.debug("Sending a streaming finished message for " + pendingFile + " to " + host);
        MessagingService.instance.sendOneWay(streamStatus.makeStreamStatusMessage(), host);

        // if all files have been received from this host, remove from bootstrap sources
        if (StreamInManager.isDone(host) && StorageService.instance.isBootstrapMode())
        {
            StorageService.instance.removeBootstrapSource(host, pendingFile.getDescriptor().ksname);
        }
    }
}
