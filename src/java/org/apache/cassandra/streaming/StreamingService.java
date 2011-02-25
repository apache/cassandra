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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectName;


public class StreamingService implements StreamingServiceMBean
{
    public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.net:type=StreamingService";
    public static final StreamingService instance = new StreamingService();

    private StreamingService()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_OBJECT_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public String getStatus()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Receiving from:\n");
        for (InetAddress source : StreamInSession.getSources())
        {
            sb.append(String.format(" %s:%n", source.getHostAddress()));
            for (PendingFile pf : StreamInSession.getIncomingFiles(source))
            {
                sb.append(String.format("  %s%n", pf.toString()));
            }
        }
        sb.append("Sending to:%n");
        for (InetAddress dest : StreamOutSession.getDestinations())
        {
            sb.append(String.format(" %s:%n", dest.getHostAddress()));
            for (PendingFile pf : StreamOutSession.getOutgoingFiles(dest))
            {
                sb.append(String.format("  %s%n", pf.toString()));
            }
        }
        return sb.toString();
    }

    /** hosts receiving outgoing streams. */
    public Set<InetAddress> getStreamDestinations()
    {
        return StreamOutSession.getDestinations();
    }

    /** outgoing streams */
    public List<String> getOutgoingFiles(String host) throws IOException
    {
        List<String> files = new ArrayList<String>();
        // first, verify that host is a destination. calling StreamOutManager.get will put it in the collection
        // leading to false positives in the future.
        Set<InetAddress> existingDestinations = getStreamDestinations();
        InetAddress dest = InetAddress.getByName(host);
        if (!existingDestinations.contains(dest))
            return files;
        
        for (PendingFile f : StreamOutSession.getOutgoingFiles(dest))
            files.add(String.format("%s", f.toString()));
        return files;
    }

    /** hosts sending incoming streams */
    public Set<InetAddress> getStreamSources()
    {
        return StreamInSession.getSources();
    }

    /** details about incoming streams. */
    public List<String> getIncomingFiles(String host) throws IOException
    {
        List<String> files = new ArrayList<String>();
        for (PendingFile pf : StreamInSession.getIncomingFiles(InetAddress.getByName(host)))
        {
            files.add(String.format("%s: %s", pf.desc.ksname, pf.toString()));
        }
        return files;
    }
}
