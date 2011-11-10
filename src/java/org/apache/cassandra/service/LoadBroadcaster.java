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

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.*;

public class LoadBroadcaster implements IEndpointStateChangeSubscriber
{
    static final int BROADCAST_INTERVAL = 60 * 1000;

    public static final LoadBroadcaster instance = new LoadBroadcaster();

    private static final Logger logger_ = LoggerFactory.getLogger(LoadBroadcaster.class);

    private Map<InetAddress, Double> loadInfo_ = new HashMap<InetAddress, Double>();

    private LoadBroadcaster()
    {
        Gossiper.instance.register(this);
    }

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state != ApplicationState.LOAD)
            return;
        loadInfo_.put(endpoint, Double.valueOf(value.value));
    }

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        VersionedValue localValue = epState.getApplicationState(ApplicationState.LOAD);
        if (localValue != null)
        {
            onChange(endpoint, ApplicationState.LOAD, localValue);
        }
    }

    public void onAlive(InetAddress endpoint, EndpointState state) {}

    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRestart(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        loadInfo_.remove(endpoint);
    }

    public Map<InetAddress, Double> getLoadInfo()
    {
        return loadInfo_;
    }

    public void startBroadcasting()
    {
        // send the first broadcast "right away" (i.e., in 2 gossip heartbeats, when we should have someone to talk to);
        // after that send every BROADCAST_INTERVAL.
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                if (logger_.isDebugEnabled())
                    logger_.debug("Disseminating load info ...");
                Gossiper.instance.addLocalApplicationState(ApplicationState.LOAD,
                                                           StorageService.instance.valueFactory.load(StorageService.instance.getLoad()));
            }
        };
        StorageService.scheduledTasks.scheduleWithFixedDelay(runnable, 2 * Gossiper.intervalInMillis, BROADCAST_INTERVAL, TimeUnit.MILLISECONDS);
    }
}

