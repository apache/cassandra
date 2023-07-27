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

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public class RateLimiterService implements RateLimiterServiceMBean {
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=RateLimiterService";

    // TODO: add more vars here and rename them properly down the road
    private int rateLimiterVar1;

    public static final RateLimiterService instance = new RateLimiterService();

    private RateLimiterService()
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

    @Override
    public void setRateLimiterVar1(int rateLimiterVar1)
    {
        this.rateLimiterVar1 = rateLimiterVar1;
    }

    @Override
    // TODO: revisit the thread safety of this function once it is getting called in the real rate limiter logic
    public int getRateLimiterVar1()
    {
        return rateLimiterVar1;
    }
}
