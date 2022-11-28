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

package org.apache.cassandra.service.accord;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;

import accord.impl.SimpleProgressLog;
import accord.impl.SizeOfIntersectionSorter;
import accord.local.Node;
import accord.messages.Reply;
import accord.messages.Request;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.AccordScheduler;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;

public class AccordService implements Shutdownable
{
    public final Node node;
    private final Shutdownable nodeShutdown;
    private final AccordMessageSink messageSink;
    public final AccordConfigurationService configService;
    private final AccordScheduler scheduler;
    private final AccordVerbHandler verbHandler;

    private static class Handle
    {
        public static final AccordService instance = new AccordService();
    }

    public static AccordService instance()
    {
        return Handle.instance;
    }

    public static long uniqueNow()
    {
        return TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
    }

    private AccordService()
    {
        Node.Id localId = EndpointMapping.endpointToId(FBUtilities.getBroadcastAddressAndPort());
        this.messageSink = new AccordMessageSink();
        this.configService = new AccordConfigurationService(localId);
        this.scheduler = new AccordScheduler();
        this.node = new Node(localId,
                             messageSink,
                             configService,
                             AccordService::uniqueNow,
                             () -> null,
                             new AccordAgent(),
                             new Random(),
                             scheduler,
                             SizeOfIntersectionSorter.SUPPLIER,
                             SimpleProgressLog::new,
                             AccordCommandStores::new);
        this.nodeShutdown = toShutdownable(node);
        this.verbHandler = new AccordVerbHandler(this.node);
    }

    public <T extends Request> IVerbHandler<T> verbHandler()
    {
        return verbHandler;
    }

    @VisibleForTesting
    public void createEpochFromConfigUnsafe()
    {
        configService.createEpochFromConfig();
    }

    public static long nowInMicros()
    {
        return TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
    }

    @VisibleForTesting
    AccordMessageSink messageSink()
    {
        return messageSink;
    }

    public void setCacheSize(long kb)
    {
        long bytes = kb << 10;
        AccordCommandStores commandStores = (AccordCommandStores) node.commandStores();
        commandStores.setCacheSize(bytes);
    }

    @Override
    public boolean isTerminated()
    {
        return scheduler.isTerminated();
    }

    @Override
    public void shutdown()
    {
        ExecutorUtils.shutdown(Arrays.asList(scheduler, nodeShutdown));
    }

    @Override
    public Object shutdownNow()
    {
        ExecutorUtils.shutdownNow(Arrays.asList(scheduler, nodeShutdown));
        return null;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        try
        {
            ExecutorUtils.awaitTermination(timeout, units, Arrays.asList(scheduler, nodeShutdown));
            return true;
        }
        catch (TimeoutException e)
        {
            return false;
        }
    }

    @VisibleForTesting
    public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownAndWait(timeout, unit, this);
    }

    private static Shutdownable toShutdownable(Node node)
    {
        return new Shutdownable() {
            private volatile boolean isShutdown = false;

            @Override
            public boolean isTerminated()
            {
                // we don't know about terminiated... so settle for shutdown!
                return isShutdown;
            }

            @Override
            public void shutdown()
            {
                isShutdown = true;
                node.shutdown();
            }

            @Override
            public Object shutdownNow()
            {
                // node doesn't offer shutdownNow
                shutdown();
                return null;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
            {
                // node doesn't offer
                return true;
            }
        };
    }
}
