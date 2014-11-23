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
package org.apache.cassandra.db;

import java.lang.management.ManagementFactory;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.hints.HintsService;

/**
 * A proxy class that implement the deprecated legacy HintedHandoffManagerMBean interface.
 *
 * TODO: remove in 4.0.
 */
@SuppressWarnings("deprecation")
@Deprecated
public final class HintedHandOffManager implements HintedHandOffManagerMBean
{
    public static final HintedHandOffManager instance = new HintedHandOffManager();

    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=HintedHandoffManager";

    private HintedHandOffManager()
    {
    }

    public void registerMBean()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void deleteHintsForEndpoint(String host)
    {
        HintsService.instance.deleteAllHintsForEndpoint(host);
    }

    public void truncateAllHints()
    {
        HintsService.instance.deleteAllHints();
    }

    // TODO
    public List<String> listEndpointsPendingHints()
    {
        throw new UnsupportedOperationException();
    }

    // TODO
    public void scheduleHintDelivery(String host)
    {
        throw new UnsupportedOperationException();
    }

    public void pauseHintsDelivery(boolean doPause)
    {
        if (doPause)
            HintsService.instance.pauseDispatch();
        else
            HintsService.instance.resumeDispatch();
    }
}
