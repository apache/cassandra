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


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.EnumMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundTcpConnectionPool;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;

public class EC2SnitchTest
{
    private static String az;

    @BeforeClass
    public static void setup() throws Exception
    {
        System.setProperty(Gossiper.Props.DISABLE_THREAD_VALIDATION, "true");
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        Keyspace.setInitialized();
        StorageService.instance.initServer(0);
    }

    private class TestEC2Snitch extends Ec2Snitch
    {
        public TestEC2Snitch() throws IOException, ConfigurationException
        {
            super();
        }

        @Override
        String awsApiCall(String url) throws IOException, ConfigurationException
        {
            return az;
        }
    }

    @Test
    public void testRac() throws IOException, ConfigurationException
    {
        az = "us-east-1d";
        Ec2Snitch snitch = new TestEC2Snitch();
        InetAddress local = InetAddress.getByName("127.0.0.1");
        InetAddress nonlocal = InetAddress.getByName("127.0.0.7");

        Gossiper.instance.addSavedEndpoint(nonlocal);
        Map<ApplicationState, VersionedValue> stateMap = new EnumMap<>(ApplicationState.class);
        stateMap.put(ApplicationState.DC, StorageService.instance.valueFactory.datacenter("us-west"));
        stateMap.put(ApplicationState.RACK, StorageService.instance.valueFactory.datacenter("1a"));
        Gossiper.instance.getEndpointStateForEndpoint(nonlocal).addApplicationStates(stateMap);

        assertEquals("us-west", snitch.getDatacenter(nonlocal));
        assertEquals("1a", snitch.getRack(nonlocal));

        assertEquals("us-east", snitch.getDatacenter(local));
        assertEquals("1d", snitch.getRack(local));
    }
    
    @Test
    public void testNewRegions() throws IOException, ConfigurationException
    {
        az = "us-east-2d";
        Ec2Snitch snitch = new TestEC2Snitch();
        InetAddress local = InetAddress.getByName("127.0.0.1");
        assertEquals("us-east-2", snitch.getDatacenter(local));
        assertEquals("2d", snitch.getRack(local));
    }

    @Test
    public void testEc2MRSnitch() throws UnknownHostException
    {
        InetAddress me = InetAddress.getByName("127.0.0.2");
        InetAddress com_ip = InetAddress.getByName("127.0.0.3");

        OutboundTcpConnectionPool pool = MessagingService.instance().getConnectionPool(me);
        Assert.assertEquals(me, pool.endPoint());
        pool.reset(com_ip);
        Assert.assertEquals(com_ip, pool.endPoint());

        MessagingService.instance().destroyConnectionPool(me);
        pool = MessagingService.instance().getConnectionPool(me);
        Assert.assertEquals(com_ip, pool.endPoint());
    }

    @AfterClass
    public static void tearDown()
    {
        StorageService.instance.stopClient();
    }
}
