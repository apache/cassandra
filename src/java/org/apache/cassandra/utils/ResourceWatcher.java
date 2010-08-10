package org.apache.cassandra.utils;
/*
 * 
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
 * 
 */


import java.io.File;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceWatcher
{
    private static Timer timer = new Timer("RESOURCE-WATCHER");

    public static void watch(String resource, Runnable callback, int period)
    {
        timer.schedule(new WatchedResource(resource, callback), period, period);
    }
    
    public static class WatchedResource extends TimerTask
    {
        private static Logger logger = LoggerFactory.getLogger(WatchedResource.class);
        private String resource;
        private Runnable callback;
        private long lastLoaded;

        public WatchedResource(String resource, Runnable callback)
        {
            this.resource = resource;
            this.callback = callback;
            lastLoaded = 0;
        }

        public void run()
        {
            try
            {
                String filename = FBUtilities.resourceToFile(resource);
                long lastModified = new File(filename).lastModified();
                if (lastModified > lastLoaded)
                {
                    callback.run();
                    lastLoaded = lastModified;
                }
            }
            catch (Throwable t)
            {
                logger.error(String.format("Timed run of %s failed.", callback.getClass()), t);
            }
        }
    }
}
