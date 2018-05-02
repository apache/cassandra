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

import java.io.InvalidClassException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.diag.store.DiagnosticEventMemoryStore;
import org.apache.cassandra.diag.store.DiagnosticEventStore;


/**
 * Manages storing and retrieving events based on enabled {@link DiagnosticEventStore} implementation.
 */
public final class DiagnosticEventPersistence
{
    private static final Logger logger = LoggerFactory.getLogger(DiagnosticEventPersistence.class);

    private static final DiagnosticEventPersistence instance = new DiagnosticEventPersistence();

    private final Map<Class, DiagnosticEventStore<Long>> stores = new ConcurrentHashMap<>();

    private final Consumer<DiagnosticEvent> eventConsumer = this::onEvent;

    public static void start()
    {
        // make sure id broadcaster is initialized (registered as MBean)
        LastEventIdBroadcaster.instance();
    }

    public static DiagnosticEventPersistence instance()
    {
        return instance;
    }

    public SortedMap<Long, Map<String, Serializable>> getEvents(String eventClazz, Long key, int limit, boolean includeKey)
    {
        assert eventClazz != null;
        assert key != null;
        assert limit >= 0;

        Class cls;
        try
        {
            cls = getEventClass(eventClazz);
        }
        catch (ClassNotFoundException | InvalidClassException e)
        {
            throw new RuntimeException(e);
        }
        DiagnosticEventStore<Long> store = getStore(cls);

        NavigableMap<Long, DiagnosticEvent> events = store.scan(key, includeKey ? limit : limit + 1);
        if (!includeKey && !events.isEmpty()) events = events.tailMap(key, false);
        TreeMap<Long, Map<String, Serializable>> ret = new TreeMap<>();
        for (Map.Entry<Long, DiagnosticEvent> entry : events.entrySet())
        {
            DiagnosticEvent event = entry.getValue();
            HashMap<String, Serializable> val = new HashMap<>(event.toMap());
            val.put("class", event.getClass().getName());
            val.put("type", event.getType().name());
            val.put("ts", event.timestamp);
            val.put("thread", event.threadName);
            ret.put(entry.getKey(), val);
        }
        logger.debug("Returning {} {} events for key {} (limit {}) (includeKey {})", ret.size(), eventClazz, key, limit, includeKey);
        return ret;
    }

    public void enableEventPersistence(String eventClazz)
    {
        try
        {
            logger.debug("Enabling events: {}", eventClazz);
            DiagnosticEventService.instance().subscribe(getEventClass(eventClazz), eventConsumer);
        }
        catch (ClassNotFoundException | InvalidClassException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void disableEventPersistence(String eventClazz)
    {
        try
        {
            logger.debug("Disabling events: {}", eventClazz);
            DiagnosticEventService.instance().unsubscribe(getEventClass(eventClazz), eventConsumer);
        }
        catch (ClassNotFoundException | InvalidClassException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void onEvent(DiagnosticEvent event)
    {
        Class<? extends DiagnosticEvent> cls = event.getClass();
        if (logger.isTraceEnabled())
            logger.trace("Persisting received {} event", cls.getName());
        DiagnosticEventStore<Long> store = getStore(cls);
        store.store(event);
        LastEventIdBroadcaster.instance().setLastEventId(event.getClass().getName(), store.getLastEventId());
    }

    private Class<DiagnosticEvent> getEventClass(String eventClazz) throws ClassNotFoundException, InvalidClassException
    {
        // get class by eventClazz argument name
        // restrict class loading for security reasons
        if (!eventClazz.startsWith("org.apache.cassandra."))
            throw new RuntimeException("Not a Cassandra event class: " + eventClazz);

        Class<DiagnosticEvent> clazz = (Class<DiagnosticEvent>) Class.forName(eventClazz);

        if (!(DiagnosticEvent.class.isAssignableFrom(clazz)))
            throw new InvalidClassException("Event class must be of type DiagnosticEvent");

        return clazz;
    }

    private DiagnosticEventStore<Long> getStore(Class cls)
    {
        return stores.computeIfAbsent(cls, (storeKey) -> new DiagnosticEventMemoryStore());
    }
}
