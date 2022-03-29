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

import java.util.regex.Pattern;

public interface MonitoringServiceMBean
{
    public boolean isBadQueryTracingEnabled();

    public void stopBadQueryReporter();

    public int getBadQueryMaxSamplesInSyslog();

    public void setBadQueryMaxSamplesInSyslog(int badQueryMaxSamplesInSyslog);

    public double getBadQueryTracingFraction();

    public void setBadQueryTracingFraction(double badQueryTracingFraction);

    public long getBadQueryReadMaxPartitionSizeInbytes();

    public void setBadQueryReadMaxPartitionSizeInbytes(long badQueryReadMaxPartitionSizeInbytes);

    public long getBadQueryWriteMaxPartitionSizeInbytes();

    public void setBadQueryWriteMaxPartitionSizeInbytes(long badQueryWriteMaxPartitionSizeInbytes);

    public int getBadQueryReadSlowLocalLatencyInms();

    public void setBadQueryReadSlowLocalLatencyInms(int badQueryReadSlowLocalLatencyInms);

    public int getBadQueryWriteSlowLocalLatencyInms();

    public void setBadQueryWriteSlowLocalLatencyInms(int badQueryWriteSlowLocalLatencyInms);

    public int getBadQueryReadSlowCoordLatencyInms();

    public void setBadQueryReadSlowCoordLatencyInms(int badQueryReadSlowCoordLatencyInms);

    public int getBadQueryWriteSlowCoordLatencyInms();

    public void setBadQueryWriteSlowCoordLatencyInms(int badQueryWriteSlowLocalLatencyInms);

    public int getBadQueryTombstoneLimit();

    public void setBadQueryTombstoneLimit(int badQueryTombstoneLimit);

    public Pattern getBadQueryIgnoreKeyspaces();

    public void setBadQueryIgnoreKeyspaces(Pattern badQueryIgnoreKeyspaces);
}
