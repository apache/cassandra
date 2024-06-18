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
package org.apache.cassandra.locator;

import java.net.UnknownHostException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.MBeanWrapper;

public class LocationInfo implements LocationInfoMBean
{
    public static void create()
    {
        MBeanWrapper.instance.registerMBean(new LocationInfo(), "org.apache.cassandra.db:type=LocationInfo");
    }

    public String getDatacenter(String host) throws UnknownHostException
    {
        return DatabaseDescriptor.getLocator().location(InetAddressAndPort.getByName(host)).datacenter;
    }

    public String getRack(String host) throws UnknownHostException
    {
        return DatabaseDescriptor.getLocator().location(InetAddressAndPort.getByName(host)).rack;
    }

    public String getDatacenter()
    {
        return DatabaseDescriptor.getLocator().local().datacenter;
    }

    public String getRack()
    {
        return DatabaseDescriptor.getLocator().local().rack;
    }

    public String getNodeProximityName()
    {
        NodeProximity proximity = DatabaseDescriptor.getNodeProximity();
        Class<?> clazz = proximity instanceof SnitchAdapter ? ((SnitchAdapter)proximity).snitch.getClass() : proximity.getClass();
        return clazz.getName();
    }

    public boolean hasLegacySnitchAdapter()
    {
        NodeProximity proximity = DatabaseDescriptor.getNodeProximity();
        if (proximity instanceof DynamicEndpointSnitch)
            return ((DynamicEndpointSnitch)proximity).delegate instanceof SnitchAdapter;
        return proximity instanceof SnitchAdapter;
    }
}
