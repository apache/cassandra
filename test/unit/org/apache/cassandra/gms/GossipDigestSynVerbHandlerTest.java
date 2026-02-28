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

package org.apache.cassandra.gms;

import java.net.UnknownHostException;
import java.util.Collections;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GossipDigestSynVerbHandlerTest
{
    private static final String FOREIGN_CLUSTER_NAME = "foreign-cluster";
    private static final String FOREIGN_PARTITIONER_NAME = "foreign-partitioner";

    private static final Logger handlerLogger = (Logger) LoggerFactory.getLogger(GossipDigestSynVerbHandler.class);
    private static final ListAppender<ILoggingEvent> logs = new ListAppender<>();

    private InetAddressAndPort foreignEndpoint;

    @BeforeClass
    public static void init()
    {
        System.setProperty(Gossiper.Props.DISABLE_THREAD_VALIDATION, "true");
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();

        if (!Gossiper.instance.isEnabled())
            Gossiper.instance.start(0);

        logs.start();
        handlerLogger.addAppender(logs);
    }

    @Before
    public void setUp() throws UnknownHostException
    {
        foreignEndpoint = InetAddressAndPort.getByName("127.0.0.250");
        Gossiper.instance.endpointStateMap.remove(foreignEndpoint);
        Gossiper.instance.liveEndpoints.remove(foreignEndpoint);
        logs.list.clear();
    }

    @After
    public void tearDown()
    {
        Gossiper.instance.endpointStateMap.remove(foreignEndpoint);
        Gossiper.instance.liveEndpoints.remove(foreignEndpoint);
        logs.list.clear();
    }

    @Test
    public void rejectsForeignClusterNameWithoutAdmission()
    {
        long lastProcessed = Gossiper.instance.getLastProcessedMessageAt();

        GossipDigestSyn syn = new GossipDigestSyn(FOREIGN_CLUSTER_NAME, DatabaseDescriptor.getPartitionerName(), Collections.emptyList());
        Message<GossipDigestSyn> message = Message.synthetic(foreignEndpoint, Verb.GOSSIP_DIGEST_SYN, syn);
        GossipDigestSynVerbHandler.instance.doVerb(message);

        assertTrue(logs.list.stream().anyMatch(e -> e.getFormattedMessage().contains("ClusterName mismatch")));
        assertTrue(logs.list.stream().anyMatch(e -> e.getFormattedMessage().contains(FOREIGN_CLUSTER_NAME)));
        assertEquals(lastProcessed, Gossiper.instance.getLastProcessedMessageAt());
        assertFalse(Gossiper.instance.endpointStateMap.containsKey(foreignEndpoint));
        assertFalse(Gossiper.instance.liveEndpoints.contains(foreignEndpoint));
    }

    @Test
    public void rejectsForeignPartitionerWithoutAdmission()
    {
        long lastProcessed = Gossiper.instance.getLastProcessedMessageAt();

        GossipDigestSyn syn = new GossipDigestSyn(DatabaseDescriptor.getClusterName(), FOREIGN_PARTITIONER_NAME, Collections.emptyList());
        Message<GossipDigestSyn> message = Message.synthetic(foreignEndpoint, Verb.GOSSIP_DIGEST_SYN, syn);
        GossipDigestSynVerbHandler.instance.doVerb(message);

        assertTrue(logs.list.stream().anyMatch(e -> e.getFormattedMessage().contains("Partitioner mismatch")));
        assertTrue(logs.list.stream().anyMatch(e -> e.getFormattedMessage().contains(FOREIGN_PARTITIONER_NAME)));
        assertEquals(lastProcessed, Gossiper.instance.getLastProcessedMessageAt());
        assertFalse(Gossiper.instance.endpointStateMap.containsKey(foreignEndpoint));
        assertFalse(Gossiper.instance.liveEndpoints.contains(foreignEndpoint));
    }
}
