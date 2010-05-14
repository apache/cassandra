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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import java.net.InetAddress;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.SimpleCondition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the streaming of multiple files one after the other.
*/
public class StreamOutManager
{   
    private static Logger logger = LoggerFactory.getLogger( StreamOutManager.class );
        
    private static ConcurrentMap<InetAddress, StreamOutManager> streamManagers = new ConcurrentHashMap<InetAddress, StreamOutManager>();
    public static final Set<InetAddress> pendingDestinations = Collections.synchronizedSet(new HashSet<InetAddress>());

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
    
    public static void remove(InetAddress to)
    {
        if (streamManagers.containsKey(to) && streamManagers.get(to).files.size() == 0)
            streamManagers.remove(to);
        pendingDestinations.remove(to);
    }

    public static Set<InetAddress> getDestinations()
    {
        // the results of streamManagers.keySet() isn't serializable, so create a new set.
        Set<InetAddress> hosts = new HashSet<InetAddress>();
        hosts.addAll(streamManagers.keySet());
        hosts.addAll(pendingDestinations);
        return hosts;
    }
    
    /** 
     * this method exists so that we don't have to call StreamOutManager.get() which has a nasty side-effect of 
     * indicating that we are streaming to a particular host.
     **/     
    public static List<PendingFile> getPendingFiles(InetAddress host)
    {
        List<PendingFile> list = new ArrayList<PendingFile>();
        StreamOutManager manager = streamManagers.get(host);
        if (manager != null)
            list.addAll(manager.getFiles());
        return list;
    }

    // we need sequential and random access to the files. hence, the map and the list.
    private final List<PendingFile> files = new ArrayList<PendingFile>();
    private final Map<String, PendingFile> fileMap = new HashMap<String, PendingFile>();
    
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
              logger.debug("Adding file " + pendingFile.getFilename() + " to be streamed.");
            files.add(pendingFile);
            fileMap.put(pendingFile.getFilename(), pendingFile);
            totalBytes += pendingFile.getExpectedBytes();
        }
    }

    public void update(String path, long pos)
    {
        PendingFile pf = fileMap.get(path);
        if (pf != null)
            pf.update(pos);
    }
    
    public void startNext()
    {
        if (files.size() > 0)
        {
            File file = new File(files.get(0).getFilename());
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
        PendingFile pf = files.remove(0);
        if (pf != null)
            fileMap.remove(pf.getFilename());
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

    List<PendingFile> getFiles()
    {
        return Collections.unmodifiableList(files);
    }

    public class StreamFile extends File
    {
        private long ptr = 0;
        public StreamFile(String path)
        {
            super(path);
            ptr = 0;
        }

        private void update(long ptr)
        {
            this.ptr = ptr;
        }

        public long getPtr()
        {
            return ptr;
        }
    }
}
