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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;


public class ZookeeperWatcher implements Watcher
{
    private static final Logger logger_ = Logger.getLogger(ZookeeperWatcher.class);
    private static final String leader_ = "/Cassandra/" + DatabaseDescriptor.getClusterName() + "/Leader";
    private static final String lock_ = "/Cassandra/" + DatabaseDescriptor.getClusterName() + "/Locks";
    
    public void process(WatchedEvent we)
    {                            
        String eventPath = we.getPath();
        logger_.debug("PROCESS EVENT : " + eventPath);
        if ( eventPath != null && (eventPath.indexOf(leader_) != -1) )
        {                                                           
            logger_.debug("Signalling the leader instance ...");
            LeaderElector.instance().signal();                                        
        }
        
    }
}
