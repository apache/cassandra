/**
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

package org.apache.cassandra.streaming;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import java.net.InetAddress;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.SimpleCondition;

import org.apache.log4j.Logger;

/**
 * This class manages the streaming of multiple files one after the other.
*/
class StreamOutManager
{   
    private static Logger logger = Logger.getLogger( StreamOutManager.class );
        
    private static ConcurrentMap<InetAddress, StreamOutManager> streamManagers = new ConcurrentHashMap<InetAddress, StreamOutManager>();

    public static StreamOutManager get(InetAddress to)
    {
        StreamOutManager manager = streamManagers.get(to);
        if (manager == null)
        {
            StreamOutManager possibleNew = new StreamOutManager(to);
            if ((manager = streamManagers.putIfAbsent(to, possibleNew)) == null)
                manager = possibleNew;
        }
        return manager;
    }
    
    private final List<File> files = new ArrayList<File>();
    private final InetAddress to;
    private long totalBytes = 0L;
    private final SimpleCondition condition = new SimpleCondition();
    
    private StreamOutManager(InetAddress to)
    {
        this.to = to;
    }
    
    public void addFilesToStream(PendingFile[] pendingFiles)
    {
        for (PendingFile pendingFile : pendingFiles)
        {
            if (logger.isDebugEnabled())
              logger.debug("Adding file " + pendingFile.getTargetFile() + " to be streamed.");
            files.add( new File( pendingFile.getTargetFile() ) );
            totalBytes += pendingFile.getExpectedBytes();
        }
    }
    
    public void startNext()
    {
        if (files.size() > 0)
        {
            File file = files.get(0);
            if (logger.isDebugEnabled())
              logger.debug("Streaming " + file.length() + " length file " + file + " ...");
            MessagingService.instance.stream(file.getAbsolutePath(), 0L, file.length(), FBUtilities.getLocalAddress(), to);
        }
    }

    public void finishAndStartNext(String file) throws IOException
    {
        File f = new File(file);
        if (logger.isDebugEnabled())
          logger.debug("Deleting file " + file + " after streaming " + f.length() + "/" + totalBytes + " bytes.");
        FileUtils.delete(file);
        files.remove(0);
        if (files.size() > 0)
        {
            startNext();
        }
        else
        {
            if (logger.isDebugEnabled())
              logger.debug("Signalling that streaming is done for " + to);
            condition.signalAll();
        }
    }
    
    public void waitForStreamCompletion()
    {
        try
        {
            condition.await();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }
}
