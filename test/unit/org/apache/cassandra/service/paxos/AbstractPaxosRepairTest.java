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

package org.apache.cassandra.service.paxos;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.service.paxos.AbstractPaxosRepair.State;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.service.paxos.AbstractPaxosRepair.DONE;

/**
 * test the state change logic of AbstractPaxosRepair
 */
public class AbstractPaxosRepairTest
{
    private static DecoratedKey dk(int k)
    {
        return Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(k));
    }

    private static final DecoratedKey DK1 = dk(1);

    private static State STARTED = new State();

    private static class PaxosTestRepair extends AbstractPaxosRepair
    {
        public PaxosTestRepair()
        {
            super(Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1)), null);
        }

        public State restart(State state, long waitUntil)
        {
            return STARTED;
        }

        public void setState(State update)
        {
            updateState(state(), null, (i1, i2) -> update);
        }
    }

    private static class Event
    {
        final AbstractPaxosRepair repair;
        final AbstractPaxosRepair.Result result;

        public Event(AbstractPaxosRepair repair, AbstractPaxosRepair.Result result)
        {
            this.repair = repair;
            this.result = result;
        }

        public String toString()
        {
            return "Event{" +
                   "repair=" + repair +
                   ", result=" + result +
                   '}';
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Event event = (Event) o;
            return Objects.equals(repair, event.repair) && Objects.equals(result, event.result);
        }

        public int hashCode()
        {
            return Objects.hash(repair, result);
        }
    }

    private static class Listener implements AbstractPaxosRepair.Listener
    {

        final List<Event> events = new ArrayList<>();

        public void onComplete(AbstractPaxosRepair repair, AbstractPaxosRepair.Result result)
        {
            events.add(new Event(repair, result));
        }

        public Event getOnlyEvent()
        {
            return Iterables.getOnlyElement(events);
        }
    }

    @Test
    public void stateUpdate()
    {
        // listeners shoulnd't be called on state updates
        PaxosTestRepair repair = new PaxosTestRepair();
        Listener listener = new Listener();
        repair.addListener(listener);
        Assert.assertNull(repair.state());
        repair.start();
        Assert.assertSame(STARTED, repair.state());
        Assert.assertTrue(listener.events.isEmpty());
    }

    @Test
    public void resultUpdate()
    {
        // listeners should be called on state updates
        PaxosTestRepair repair = new PaxosTestRepair();
        Listener listener = new Listener();
        repair.addListener(listener);
        repair.start();
        Assert.assertTrue(listener.events.isEmpty());

        repair.setState(DONE);
        Assert.assertEquals(new Event(repair, DONE), listener.getOnlyEvent());
    }

    @Test
    public void stateUpdateException()
    {
        // state should be set to failure and listeners called on exception
        Throwable e = new Throwable();

        PaxosTestRepair repair = new PaxosTestRepair();
        Listener listener = new Listener();
        repair.addListener(listener);
        repair.start();

        repair.updateState(repair.state(), null, (i1, i2) -> {throw e;});
        Assert.assertEquals(new Event(repair, new AbstractPaxosRepair.Failure(e)), listener.getOnlyEvent());

    }

    @Test
    public void postResultListenerAttachment()
    {
        // listener should be called immediately if the repair is already complete
        PaxosTestRepair repair = new PaxosTestRepair();
        repair.start();

        repair.setState(DONE);

        Listener listener = new Listener();
        Assert.assertTrue(listener.events.isEmpty());
        repair.addListener(listener);
        Assert.assertEquals(new Event(repair, DONE), listener.getOnlyEvent());
    }
}
