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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.metrics.StorageMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.gms.*;

public class LoadBroadcaster implements IEndpointStateChangeSubscriber
{
    static final int BROADCAST_INTERVAL = Integer.getInteger("cassandra.broadcast_interval_ms", 60 * 1000);

    public static final LoadBroadcaster instance = new LoadBroadcaster();

    private static final Logger logger = LoggerFactory.getLogger(LoadBroadcaster.class);

    private ConcurrentMap<InetAddress, Double> loadInfo = new ConcurrentHashMap<InetAddress, java.lang.Double>();

    private LoadBroadcaster()
    {
        Gossiper.instance.register(this);
    }

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state != ApplicationState.LOAD)
            return;
        loadInfo.put(endpoint, Double.valueOf(value.value));
    }

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        VersionedValue localValue = epState.getApplicationState(ApplicationState.LOAD);
        if (localValue != null)
        {
            onChange(endpoint, ApplicationState.LOAD, localValue);
        }
    }
    
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}

    public void onAlive(InetAddress endpoint, EndpointState state) {}

    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRestart(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        loadInfo.remove(endpoint);
    }

    public Map<InetAddress, Double> getLoadInfo()
    {
        return Collections.unmodifiableMap(loadInfo);
    }

    public void startBroadcasting()
    {
        // send the first broadcast "right away" (i.e., in 2 gossip heartbeats, when we should have someone to talk to);
        // after that send every BROADCAST_INTERVAL.
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                if (logger.isTraceEnabled())
                    logger.trace("Disseminating load info ...");
                Gossiper.instance.addLocalApplicationState(ApplicationState.LOAD,
                                                           StorageService.instance.valueFactory.load(StorageMetrics.load.getCount()));
            }
        };
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(runnable, 2 * Gossiper.intervalInMillis, BROADCAST_INTERVAL, TimeUnit.MILLISECONDS);
    }
}

