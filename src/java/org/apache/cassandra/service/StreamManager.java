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

package org.apache.cassandra.service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import java.net.InetAddress;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.io.StreamContextManager;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.io.util.FileUtils;

import org.apache.log4j.Logger;

/*
 * This class manages the streaming of multiple files 
 * one after the other. 
*/
public final class StreamManager
{   
    private static Logger logger_ = Logger.getLogger( StreamManager.class );
        
    private static ConcurrentMap<InetAddress, StreamManager> streamManagers_ = new ConcurrentHashMap<InetAddress, StreamManager>();
    
    public static StreamManager instance(InetAddress to)
    {
        StreamManager streamManager = streamManagers_.get(to);
        if ( streamManager == null )
        {
            StreamManager possibleNew = new StreamManager(to);
            if ((streamManager = streamManagers_.putIfAbsent(to, possibleNew)) == null)
                streamManager = possibleNew;
        }
        return streamManager;
    }
    
    private List<File> filesToStream_ = new ArrayList<File>();
    private InetAddress to_;
    private long totalBytesToStream_ = 0L;
    
    private StreamManager(InetAddress to)
    {
        to_ = to;
    }
    
    public void addFilesToStream(StreamContextManager.StreamContext[] streamContexts)
    {
        for ( StreamContextManager.StreamContext streamContext : streamContexts )
        {
            if (logger_.isDebugEnabled())
              logger_.debug("Adding file " + streamContext.getTargetFile() + " to be streamed.");
            filesToStream_.add( new File( streamContext.getTargetFile() ) );
            totalBytesToStream_ += streamContext.getExpectedBytes();
        }
    }
    
    public void start()
    {
        if ( filesToStream_.size() > 0 )
        {
            File file = filesToStream_.get(0);
            if (logger_.isDebugEnabled())
              logger_.debug("Streaming " + file.length() + " length file " + file + " ...");
            MessagingService.instance().stream(file.getAbsolutePath(), 0L, file.length(), FBUtilities.getLocalAddress(), to_);
        }
    }
    
    public void repeat()
    {
        if ( filesToStream_.size() > 0 )
            start();
    }
    
    public void finish(String file) throws IOException
    {
        File f = new File(file);
        if (logger_.isDebugEnabled())
          logger_.debug("Deleting file " + file + " after streaming " + f.length() + "/" + totalBytesToStream_ + " bytes.");
        FileUtils.delete(file);
        filesToStream_.remove(0);
        if ( filesToStream_.size() > 0 )
            start();
        else
        {
            synchronized(this)
            {
                if (logger_.isDebugEnabled())
                  logger_.debug("Signalling that streaming is done for " + to_);
                notifyAll();
            }
        }
    }
    
    public synchronized void waitForStreamCompletion()
    {
        try
        {
            wait();
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
    }
}
