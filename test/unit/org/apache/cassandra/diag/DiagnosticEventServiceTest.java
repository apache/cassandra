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

package org.apache.cassandra.diag;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DiagnosticEventServiceTest
{

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void cleanup()
    {
        DiagnosticEventService.instance().cleanup();
    }

    @Test
    public void testSubscribe()
    {
        DiagnosticEventService instance = DiagnosticEventService.instance();
        assertFalse(instance.hasSubscribers(TestEvent1.class));
        assertFalse(instance.hasSubscribers(TestEvent2.class));
        Consumer<TestEvent1> consumer1 = (event) ->
        {
        };
        Consumer<TestEvent1> consumer2 = (event) ->
        {
        };
        Consumer<TestEvent1> consumer3 = (event) ->
        {
        };
        instance.subscribe(TestEvent1.class, consumer1);
        instance.subscribe(TestEvent1.class, consumer2);
        instance.subscribe(TestEvent1.class, consumer3);
        assertTrue(instance.hasSubscribers(TestEvent1.class));
        assertFalse(instance.hasSubscribers(TestEvent2.class));
        instance.unsubscribe(consumer1);
        assertTrue(instance.hasSubscribers(TestEvent1.class));
        assertFalse(instance.hasSubscribers(TestEvent2.class));
        instance.unsubscribe(consumer2);
        instance.unsubscribe(consumer3);
        assertFalse(instance.hasSubscribers(TestEvent1.class));
        assertFalse(instance.hasSubscribers(TestEvent2.class));
    }

    @Test
    public void testSubscribeByType()
    {
        DiagnosticEventService instance = DiagnosticEventService.instance();
        assertFalse(instance.hasSubscribers(TestEvent1.class));
        assertFalse(instance.hasSubscribers(TestEvent2.class));
        Consumer<TestEvent1> consumer1 = (event) ->
        {
        };
        Consumer<TestEvent1> consumer2 = (event) ->
        {
        };
        Consumer<TestEvent1> consumer3 = (event) ->
        {
        };

        assertFalse(instance.hasSubscribers(TestEvent1.class, TestEventType.TEST1));
        instance.subscribe(TestEvent1.class, TestEventType.TEST1, consumer1);
        assertTrue(instance.hasSubscribers(TestEvent1.class, TestEventType.TEST1));
        assertFalse(instance.hasSubscribers(TestEvent1.class, TestEventType.TEST2));

        instance.subscribe(TestEvent1.class, TestEventType.TEST2, consumer2);
        instance.subscribe(TestEvent1.class, TestEventType.TEST2, consumer2);
        instance.subscribe(TestEvent1.class, TestEventType.TEST2, consumer2);
        assertTrue(instance.hasSubscribers(TestEvent1.class, TestEventType.TEST2));

        assertFalse(instance.hasSubscribers(TestEvent2.class));

        instance.subscribe(TestEvent1.class, consumer3);
        assertTrue(instance.hasSubscribers(TestEvent1.class));
        assertTrue(instance.hasSubscribers(TestEvent1.class, TestEventType.TEST1));
        assertTrue(instance.hasSubscribers(TestEvent1.class, TestEventType.TEST2));
        assertTrue(instance.hasSubscribers(TestEvent1.class, TestEventType.TEST3));

        instance.unsubscribe(consumer1);
        assertTrue(instance.hasSubscribers(TestEvent1.class));
        assertFalse(instance.hasSubscribers(TestEvent2.class));
        instance.unsubscribe(consumer2);
        instance.unsubscribe(consumer3);
        assertFalse(instance.hasSubscribers(TestEvent1.class));
        assertFalse(instance.hasSubscribers(TestEvent2.class));
    }

    @Test
    public void testSubscribeAll()
    {
        DiagnosticEventService instance = DiagnosticEventService.instance();
        assertFalse(instance.hasSubscribers(TestEvent1.class));
        assertFalse(instance.hasSubscribers(TestEvent2.class));
        Consumer<DiagnosticEvent> consumerAll1 = (event) ->
        {
        };
        Consumer<DiagnosticEvent> consumerAll2 = (event) ->
        {
        };
        Consumer<DiagnosticEvent> consumerAll3 = (event) ->
        {
        };
        instance.subscribeAll(consumerAll1);
        instance.subscribeAll(consumerAll2);
        instance.subscribeAll(consumerAll3);
        assertTrue(instance.hasSubscribers(TestEvent1.class));
        assertTrue(instance.hasSubscribers(TestEvent2.class));
        instance.unsubscribe(consumerAll1);
        assertTrue(instance.hasSubscribers(TestEvent1.class));
        assertTrue(instance.hasSubscribers(TestEvent2.class));
        instance.unsubscribe(consumerAll2);
        instance.unsubscribe(consumerAll3);
        assertFalse(instance.hasSubscribers(TestEvent1.class));
        assertFalse(instance.hasSubscribers(TestEvent2.class));
    }

    @Test
    public void testCleanup()
    {
        DiagnosticEventService instance = DiagnosticEventService.instance();
        assertFalse(instance.hasSubscribers(TestEvent1.class));
        assertFalse(instance.hasSubscribers(TestEvent2.class));
        Consumer<TestEvent1> consumer = (event) ->
        {
        };
        instance.subscribe(TestEvent1.class, consumer);
        Consumer<DiagnosticEvent> consumerAll = (event) ->
        {
        };
        instance.subscribeAll(consumerAll);
        assertTrue(instance.hasSubscribers(TestEvent1.class));
        assertTrue(instance.hasSubscribers(TestEvent2.class));
        instance.cleanup();
        assertFalse(instance.hasSubscribers(TestEvent1.class));
        assertFalse(instance.hasSubscribers(TestEvent2.class));
    }

    @Test
    public void testPublish()
    {
        DiagnosticEventService instance = DiagnosticEventService.instance();
        TestEvent1 a = new TestEvent1();
        TestEvent1 b = new TestEvent1();
        TestEvent1 c = new TestEvent1();
        List<TestEvent1> events = ImmutableList.of(a, b, c, c, c);

        List<DiagnosticEvent> consumed = new LinkedList<>();
        Consumer<TestEvent1> consumer = consumed::add;
        Consumer<DiagnosticEvent> consumerAll = consumed::add;

        DatabaseDescriptor.setDiagnosticEventsEnabled(true);
        instance.publish(c);
        instance.subscribe(TestEvent1.class, consumer);
        instance.publish(a);
        instance.unsubscribe(consumer);
        instance.publish(c);
        instance.subscribeAll(consumerAll);
        instance.publish(b);
        instance.subscribe(TestEvent1.class, TestEventType.TEST3, consumer);
        instance.publish(c);
        instance.subscribe(TestEvent1.class, TestEventType.TEST1, consumer);
        instance.publish(c);

        assertEquals(events, consumed);
    }

    @Test
    public void testEnabled()
    {
        DatabaseDescriptor.setDiagnosticEventsEnabled(false);
        DiagnosticEventService.instance().subscribe(TestEvent1.class, (event) -> fail());
        DiagnosticEventService.instance().publish(new TestEvent1());
        DatabaseDescriptor.setDiagnosticEventsEnabled(true);
    }

    public static class TestEvent1 extends DiagnosticEvent
    {
        public TestEventType getType()
        {
            return TestEventType.TEST1;
        }

        public HashMap<String, Serializable> toMap()
        {
            return null;
        }
    }

    public static class TestEvent2 extends DiagnosticEvent
    {
        public TestEventType getType()
        {
            return TestEventType.TEST2;
        }

        public HashMap<String, Serializable> toMap()
        {
            return null;
        }
    }

    public enum TestEventType { TEST1, TEST2, TEST3 }
}
