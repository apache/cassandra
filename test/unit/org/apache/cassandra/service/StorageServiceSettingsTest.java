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

import java.net.UnknownHostException;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.PropertyFileSnitch;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchHistogram;
import org.apache.cassandra.locator.dynamicsnitch.DynamicEndpointSnitchLegacyHistogram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


public class StorageServiceSettingsTest
{
    private static final int UPDATE_INTERVAL = 103;
    private static final int UPDATE_SAMPLE_INTERVAL = 2002;
    private static final double BADNESS = 0.23;
    private static final String TP_NAME = "LatencyProbes";
    private static final Pattern PATTERN_TP_NAME = Pattern.compile(".*" + TP_NAME + ".*");

    @BeforeClass
    public static void setUp() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setupTest() throws ClassNotFoundException
    {
        StorageService.instance.updateEndpointSnitch("SimpleSnitch", "", UPDATE_INTERVAL, UPDATE_SAMPLE_INTERVAL, BADNESS);
    }

    @After
    public void teardown() throws ClassNotFoundException
    {
        StorageService.instance.updateEndpointSnitch("SimpleSnitch", "", UPDATE_INTERVAL, UPDATE_SAMPLE_INTERVAL, BADNESS);
    }

    @Test
    public void testDESConfigurationChange() throws ClassNotFoundException
    {
        IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();
        StorageService.instance.setDynamicUpdateInterval(12);
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch);
        assertEquals(12, DatabaseDescriptor.getDynamicUpdateInterval());
        assertEquals(oldSnitch, DatabaseDescriptor.getEndpointSnitch());

        StorageService.instance.updateEndpointSnitch(null, null, 111, 2003, 0.27);
        assertEquals(111, DatabaseDescriptor.getDynamicUpdateInterval());
        assertEquals(2003, DatabaseDescriptor.getDynamicSampleUpdateInterval());
        assertEquals(0.27, DatabaseDescriptor.getDynamicBadnessThreshold(), 0.01);
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch);
        assertEquals(oldSnitch, DatabaseDescriptor.getEndpointSnitch());

        // Configuration should stay the same even when changing snitches
        StorageService.instance.updateEndpointSnitch(null, "DynamicEndpointSnitchHistogram", null, null, null);
        assertEquals(111, DatabaseDescriptor.getDynamicUpdateInterval());
        assertEquals(2003, DatabaseDescriptor.getDynamicSampleUpdateInterval());
        assertEquals(0.27, DatabaseDescriptor.getDynamicBadnessThreshold(), 0.01);
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch);
        assertNotEquals(oldSnitch, DatabaseDescriptor.getEndpointSnitch());
    }

    @Test
    public void testUpdateEndpointSnitchTypo()
    {
        IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();

        try
        {
            StorageService.instance.updateEndpointSnitch(null, "NotARealSnitch", 111, 2003, 0.27);
        }
        catch (ClassNotFoundException ignored) {}
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch);
        assertEquals(oldSnitch, DatabaseDescriptor.getEndpointSnitch());

        try
        {
            StorageService.instance.updateEndpointSnitch("NotARealSnitc", "", 111, 2003, 0.27);
        }
        catch (ClassNotFoundException ignored) {}
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch);
        assertEquals(oldSnitch, DatabaseDescriptor.getEndpointSnitch());
    }

    @Test
    public void testDESThreadLeaks() throws ClassNotFoundException
    {
        // Since the DES now has a dedicated thread pool, let's make sure that we don't leak anything when we stop
        // and start the DES. There should only ever be one thread.
        for (int i = 0; i < 1000; i ++)
        {
            StorageService.instance.updateEndpointSnitch(null, "DynamicEndpointSnitchLegacyHistogram", null, null, null);
            assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitchLegacyHistogram);

            StorageService.instance.updateEndpointSnitch("PropertyFileSnitch", "DynamicEndpointSnitchHistogram", null, null, null);
            assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitchHistogram);

            // Force the thread pool to actually do something so a thread spawns
            ScheduledExecutors.getOrCreateSharedExecutor(DynamicEndpointSnitch.LATENCY_PROBE_TP_NAME).submit(() -> {});
            assertEquals(1, ScheduledExecutors.getOrCreateSharedExecutor(DynamicEndpointSnitch.LATENCY_PROBE_TP_NAME).getMaximumPoolSize());
        }

        int matchingThreadCount = 0;
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        for (Thread t: threadSet)
        {
            if (PATTERN_TP_NAME.matcher(t.getName()).matches())
            {
                matchingThreadCount++;
            }
        }

        // There should only ever be a single thread for latency probes
        assertEquals(matchingThreadCount, 1);
    }

    @Test
    public void testDESDynamicOffAndOn() throws ClassNotFoundException
    {
        // Able to turn off dynamic snitching with string+null
        StorageService.instance.updateEndpointSnitch("", null, null, null, null);
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof SimpleSnitch);

        // Able to turn DES back on
        StorageService.instance.updateEndpointSnitch("", "DynamicEndpointSnitchLegacyHistogram", null, null, null);
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitchLegacyHistogram);
        assertEquals(UPDATE_INTERVAL, DatabaseDescriptor.getDynamicUpdateInterval());
        assertEquals(UPDATE_SAMPLE_INTERVAL, DatabaseDescriptor.getDynamicSampleUpdateInterval());
        assertEquals(BADNESS, DatabaseDescriptor.getDynamicBadnessThreshold(), 0.01);

        // Able to turn off dynamic snitching with empty string + null
        StorageService.instance.updateEndpointSnitch("", null, null, null, null);
        assertFalse(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch);
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof SimpleSnitch);

        // Able to turn DES back on
        StorageService.instance.updateEndpointSnitch("", "DynamicEndpointSnitchLegacyHistogram", null, null, null);
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitchLegacyHistogram);
        assertEquals(UPDATE_INTERVAL, DatabaseDescriptor.getDynamicUpdateInterval());
        assertEquals(UPDATE_SAMPLE_INTERVAL, DatabaseDescriptor.getDynamicSampleUpdateInterval());
        assertEquals(BADNESS, DatabaseDescriptor.getDynamicBadnessThreshold(), 0.01);
    }

    @Test
    public void testDESSnitchSwaps() throws ClassNotFoundException, UnknownHostException
    {
        // Able to change the underlying snitch
        StorageService.instance.updateEndpointSnitch("PropertyFileSnitch", "", null, null, null);
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch);
        assertTrue(((DynamicEndpointSnitch)DatabaseDescriptor.getEndpointSnitch()).subsnitch instanceof PropertyFileSnitch);

        // Able to change the DES wrapper
        assertFalse(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitchLegacyHistogram);
        StorageService.instance.updateEndpointSnitch("", "DynamicEndpointSnitchLegacyHistogram", null, null, null);
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitchLegacyHistogram);
        assertTrue(((DynamicEndpointSnitch)DatabaseDescriptor.getEndpointSnitch()).subsnitch instanceof PropertyFileSnitch);

        StorageService.instance.updateEndpointSnitch(null, "DynamicEndpointSnitchHistogram", null, null, null);
        assertFalse(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitchLegacyHistogram);
        assertTrue(((DynamicEndpointSnitch)DatabaseDescriptor.getEndpointSnitch()).subsnitch instanceof PropertyFileSnitch);

        // Able to change both at the same time
        StorageService.instance.updateEndpointSnitch("SimpleSnitch", "DynamicEndpointSnitchLegacyHistogram", null, null, null);
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch);
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitchLegacyHistogram);
        assertTrue(((DynamicEndpointSnitch)DatabaseDescriptor.getEndpointSnitch()).subsnitch instanceof SimpleSnitch);
    }

    @Test
    public void testOldDESUpdateViaJMX() throws ClassNotFoundException
    {
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch);
        StorageService.instance.updateSnitch("PropertyFileSnitch", false, null, null, null);
        assertFalse(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch);
        StorageService.instance.updateSnitch(null, true, null, null, null);
        assertTrue(DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch);
    }
}
