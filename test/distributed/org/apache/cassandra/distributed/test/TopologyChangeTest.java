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

package org.apache.cassandra.distributed.test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.impl.INodeProvisionStrategy.Strategy;
import org.apache.cassandra.distributed.test.TopologyChangeTest.EventStateListener.Event;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.impl.INodeProvisionStrategy.Strategy.MultipleNetworkInterfaces;
import static org.apache.cassandra.distributed.impl.INodeProvisionStrategy.Strategy.OneNetworkInterface;
import static org.apache.cassandra.distributed.test.TopologyChangeTest.EventStateListener.EventType.Down;
import static org.apache.cassandra.distributed.test.TopologyChangeTest.EventStateListener.EventType.Remove;
import static org.apache.cassandra.distributed.test.TopologyChangeTest.EventStateListener.EventType.Up;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(Parameterized.class)
public class TopologyChangeTest extends TestBaseImpl
{
    static class EventStateListener implements Host.StateListener
    {
        enum EventType
        {
            Add,
            Up,
            Down,
            Remove,
        }

        static class Event
        {
            InetSocketAddress host;
            EventType type;

            Event(EventType type, Host host)
            {
                this.type = type;
                this.host = host.getBroadcastSocketAddress();
            }

            public Event(EventType type, IInvokableInstance iInvokableInstance)
            {
                this.type = type;
                this.host = iInvokableInstance.broadcastAddress();
            }


            public String toString()
            {
                return "Event{" +
                       "host='" + host + '\'' +
                       ", type=" + type +
                       '}';
            }

            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Event event = (Event) o;
                return Objects.equals(host, event.host) &&
                       type == event.type;
            }

            public int hashCode()
            {
                return Objects.hash(host, type);
            }
        }

        private final List<Event> events = new CopyOnWriteArrayList<>();

        public void onAdd(Host host)
        {
            events.add(new Event(EventType.Add, host));
        }

        public void onUp(Host host)
        {
            events.add(new Event(Up, host));
        }

        public void onDown(Host host)
        {
            events.add(new Event(EventType.Down, host));
        }

        public void onRemove(Host host)
        {
            events.add(new Event(Remove, host));
        }

        public void onRegister(com.datastax.driver.core.Cluster cluster)
        {
        }

        public void onUnregister(com.datastax.driver.core.Cluster cluster)
        {
        }
    }

    @Parameterized.Parameter(0)
    public Strategy strategy;

    @Parameterized.Parameters(name = "{index}: provision strategy={0}")
    public static Collection<Strategy[]> data()
    {
        return Arrays.asList(new Strategy[][]{ { MultipleNetworkInterfaces },
                                               { OneNetworkInterface }
        });
    }

    @Test
    public void testDecommission() throws Throwable
    {
        try (Cluster control = init(Cluster.build().withNodes(3).withNodeProvisionStrategy(strategy)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)).start());
             com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
             Session session = cluster.connect())
        {
            EventStateListener eventStateListener = new EventStateListener();
            session.getCluster().register(eventStateListener);

            control.get(3).nodetoolResult("disablebinary").asserts().success();
            control.get(3).nodetoolResult("decommission", "-f").asserts().success();
            await().atMost(5, TimeUnit.SECONDS)
                   .untilAsserted(() -> Assert.assertEquals(2, cluster.getMetadata().getAllHosts().size()));
            session.getCluster().unregister(eventStateListener);
            // DOWN UP can also be seen if the jvm is slow and connections are closed; to avoid this make sure to use
            // containsSequence to check that down/remove happen in this order
            assertThat(eventStateListener.events).containsSequence(new Event(Down, control.get(3)), new Event(Remove, control.get(3)));
        }
    }

    @Test
    public void testRestartNode() throws Throwable
    {
        try (Cluster control = init(Cluster.build().withNodes(3).withNodeProvisionStrategy(strategy)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)).start());
             com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
             Session session = cluster.connect())
        {
            EventStateListener eventStateListener = new EventStateListener();
            session.getCluster().register(eventStateListener);

            control.get(3).shutdown().get();
            await().atMost(5, TimeUnit.SECONDS)
                   .untilAsserted(() -> Assert.assertEquals(2, cluster.getMetadata().getAllHosts().stream().filter(h -> h.isUp()).count()));

            control.get(3).startup();
            await().atMost(30, TimeUnit.SECONDS)
                   .untilAsserted(() -> Assert.assertEquals(3, cluster.getMetadata().getAllHosts().stream().filter(h -> h.isUp()).count()));

            // DOWN UP can also be seen if the jvm is slow and connections are closed, but make sure it at least happens once
            // given the node restarts
            assertThat(eventStateListener.events).containsSequence(new Event(Down, control.get(3)),
                                                                   new Event(Up, control.get(3)));
        }
    }
}

