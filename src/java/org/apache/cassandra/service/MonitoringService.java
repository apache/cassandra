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

import org.apache.cassandra.utils.MBeanWrapper;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public class MonitoringService implements MonitoringServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=MonitoringService";

    private int badQueryMaxSamplesInSyslog;
    private double badQueryTracingFraction;
    private long badQueryReadMaxPartitionSizeInbytes;
    private long badQueryWriteMaxPartitionSizeInbytes;
    private int badQueryReadSlowLocalLatencyInms;
    private int badQueryWriteSlowLocalLatencyInms;
    private int badQueryReadSlowCoordLatencyInms;
    private int badQueryWriteSlowCoordLatencyInms;
    private int badQueryTombstoneLimit;
    private String badQueryIgnoreKeyspaces;

    public static final MonitoringService instance = new MonitoringService();

    private MonitoringService()
    {
    }

    static
    {
        MBeanWrapper.instance.registerMBean(instance, MBEAN_NAME);
    }

    public int getBadQueryMaxSamplesInSyslog()
    {
        return badQueryMaxSamplesInSyslog;
    }

    public void setBadQueryMaxSamplesInSyslog(int badQueryMaxSamplesInSyslog)
    {
        this.badQueryMaxSamplesInSyslog = badQueryMaxSamplesInSyslog;
    }

    public double getBadQueryTracingFraction()
    {
        return badQueryTracingFraction;
    }

    public void setBadQueryTracingFraction(double badQueryTracingFraction)
    {
        this.badQueryTracingFraction = badQueryTracingFraction;
    }

    public long getBadQueryReadMaxPartitionSizeInbytes()
    {
        return badQueryReadMaxPartitionSizeInbytes;
    }

    public void setBadQueryReadMaxPartitionSizeInbytes(long badQueryReadMaxPartitionSizeInbytes)
    {
        this.badQueryReadMaxPartitionSizeInbytes = badQueryReadMaxPartitionSizeInbytes;
    }

    public long getBadQueryWriteMaxPartitionSizeInbytes()
    {
        return badQueryWriteMaxPartitionSizeInbytes;
    }

    public void setBadQueryWriteMaxPartitionSizeInbytes(long badQueryWriteMaxPartitionSizeInbytes)
    {
        this.badQueryWriteMaxPartitionSizeInbytes = badQueryWriteMaxPartitionSizeInbytes;
    }

    public int getBadQueryReadSlowLocalLatencyInms()
    {
        return badQueryReadSlowLocalLatencyInms;
    }

    public void setBadQueryReadSlowLocalLatencyInms(int badQueryReadSlowLocalLatencyInms)
    {
        this.badQueryReadSlowLocalLatencyInms = badQueryReadSlowLocalLatencyInms;
    }

    public int getBadQueryWriteSlowLocalLatencyInms()
    {
        return badQueryWriteSlowLocalLatencyInms;
    }

    public void setBadQueryWriteSlowLocalLatencyInms(int badQueryWriteSlowLocalLatencyInms)
    {
        this.badQueryWriteSlowLocalLatencyInms = badQueryWriteSlowLocalLatencyInms;
    }

    public int getBadQueryReadSlowCoordLatencyInms()
    {
        return badQueryReadSlowCoordLatencyInms;
    }

    public void setBadQueryReadSlowCoordLatencyInms(int badQueryReadSlowCoordLatencyInms)
    {
        this.badQueryReadSlowCoordLatencyInms = badQueryReadSlowCoordLatencyInms;
    }

    public int getBadQueryWriteSlowCoordLatencyInms()
    {
        return badQueryWriteSlowCoordLatencyInms;
    }

    public void setBadQueryWriteSlowCoordLatencyInms(int badQueryWriteSlowLocalLatencyInms)
    {
        this.badQueryWriteSlowCoordLatencyInms = badQueryWriteSlowLocalLatencyInms;
    }

    public int getBadQueryTombstoneLimit()
    {
        return badQueryTombstoneLimit;
    }

    public void setBadQueryTombstoneLimit(int badQueryTombstoneLimit)
    {
        this.badQueryTombstoneLimit = badQueryTombstoneLimit;
    }

    public String getBadQueryIgnoreKeyspaces()
    {
        return badQueryIgnoreKeyspaces;
    }

    public void setBadQueryIgnoreKeyspaces(String badQueryIgnoreKeyspaces)
    {
        this.badQueryIgnoreKeyspaces = badQueryIgnoreKeyspaces;
    }
}
