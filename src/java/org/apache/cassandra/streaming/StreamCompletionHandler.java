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

import org.apache.log4j.Logger;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.IStreamComplete;
import org.apache.cassandra.streaming.StreamInManager;
import org.apache.cassandra.service.StorageService;

/**
 * This is the callback handler that is invoked when we have
 * completely received a single file from a remote host.
 *
 * TODO if we move this into CFS we could make addSSTables private, improving encapsulation.
*/
class StreamCompletionHandler implements IStreamComplete
{
    private static Logger logger = Logger.getLogger(StreamCompletionHandler.class);

    public void onStreamCompletion(InetAddress host, PendingFile pendingFile, CompletedFileStatus streamStatus) throws IOException
    {
        /* Parse the stream context and the file to the list of SSTables in the associated Column Family Store. */
        if (pendingFile.getFilename().contains("-Data.db"))
        {
            String tableName = pendingFile.getDescriptor().ksname;
            File file = new File( pendingFile.getFilename() );
            String fileName = file.getName();
            String [] temp = fileName.split("-");

            //Open the file to see if all parts are now here
            try
            {
                SSTableReader sstable = SSTableWriter.renameAndOpen(pendingFile.getFilename());
                //TODO add a sanity check that this sstable has all its parts and is ok
                Table.open(tableName).getColumnFamilyStore(temp[0]).addSSTable(sstable);
                logger.info("Streaming added " + sstable.getFilename());
            }
            catch (IOException e)
            {
                throw new RuntimeException("Not able to add streamed file " + pendingFile.getFilename(), e);
            }
        }

        if (logger.isDebugEnabled())
          logger.debug("Sending a streaming finished message with " + streamStatus + " to " + host);
        /* Send a StreamStatus message which may require the source node to re-stream certain files. */
        MessagingService.instance.sendOneWay(streamStatus.makeStreamStatusMessage(), host);

        /* If we're done with everything for this host, remove from bootstrap sources */
        if (StreamInManager.isDone(host) && StorageService.instance.isBootstrapMode())
        {
            StorageService.instance.removeBootstrapSource(host, pendingFile.getDescriptor().ksname);
        }
    }
}
