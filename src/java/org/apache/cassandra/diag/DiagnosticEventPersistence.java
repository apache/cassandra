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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableSetMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.diag.store.DiagnosticEventMemoryStore;
import org.apache.cassandra.diag.store.DiagnosticEventStore;
import org.apache.cassandra.utils.FBUtilities;


/**
 * Manages storing and retrieving events based on enabled {@link DiagnosticEventStore} implementation.
 */
public final class DiagnosticEventPersistence
{
    private static final Logger logger = LoggerFactory.getLogger(DiagnosticEventPersistence.class);

    private static final DiagnosticEventPersistence instance = new DiagnosticEventPersistence();

    private final InMemoryDiagnosticLogger inMemoryLogger = new InMemoryDiagnosticLogger();
    private volatile DiagnosticLogOptions diagnosticLogOptions;
    private volatile IDiagnosticLogger diagnosticLogger;
    private final Collection<Consumer<DiagnosticEvent>> consumers = new HashSet<>();
    private volatile boolean initialized = false;

    public synchronized void initialize()
    {
        if (initialized || !DatabaseDescriptor.diagnosticEventsEnabled())
            return;

        consumers.add(inMemoryLogger);
        diagnosticLogOptions = DatabaseDescriptor.getDiagnosticLoggingOptions();

        enablePersistentDiagnosticLog(diagnosticLogOptions);

        initialized = true;
    }

    public static void start()
    {
        // make sure id broadcaster is initialized (registered as MBean)
        LastEventIdBroadcaster.instance();
    }

    public static DiagnosticEventPersistence instance()
    {
        return instance;
    }

    private void unsubscribeLogger(IDiagnosticLogger logger)
    {
        for (Class clazz : DiagnosticEventService.instance().getSubscribersByClass())
            DiagnosticEventService.instance().unsubscribe(clazz, logger);

        for (Map.Entry<Class, ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>>> entry : DiagnosticEventService.instance().getSubscribesByClassAndType().entrySet())
        {
            for (Enum<?> type : entry.getValue().keySet())
                DiagnosticEventService.instance().unsubscribe(entry.getKey(), type, logger);
        }
    }

    private Map<Class, Set<Enum<?>>> previousSubscriptions = new HashMap<>();

    public synchronized void disableDiagnosticLog()
    {
        disableDiagnosticLog(false);
    }

    public synchronized void disableDiagnosticLog(boolean clean)
    {
        //        Map<Class, Set<Enum<?>>> previousSubscriptions = new HashMap<>();
//
//        for (Map.Entry<Class, ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>>> entry : DiagnosticEventService.instance().getSubscribesByClassAndType().entrySet())
//        {
//            for (Map.Entry<Enum<?>, Consumer<DiagnosticEvent>> entryValue : entry.getValue().entries())
//            {
//                if (entryValue.getValue() == inMemoryLogger)
//                {
//                    if (!previousSubscriptions.containsKey(entry.getKey()))
//                        previousSubscriptions.put(entry.getKey(), new HashSet<>());
//
//                    previousSubscriptions.get(entry.getKey()).add(entryValue.getKey());
//                }
//            }
//        }
//
//        this.previousSubscriptions = previousSubscriptions;

        unsubscribeLogger(inMemoryLogger);

        if (clean)
            inMemoryLogger.stop();

        consumers.remove(inMemoryLogger);
    }

    public synchronized void enableDiagnosticLog()
    {
        if (!consumers.contains(inMemoryLogger))
            consumers.add(inMemoryLogger);

//        for (Map.Entry<Class, Set<Enum<?>>> entry : previousSubscriptions.entrySet())
//        {
//            for (Enum type : entry.getValue())
//                DiagnosticEventService.instance().subscribe(entry.getKey(), type, inMemoryLogger);
//        }
    }

    public synchronized void disablePersistentDiagnosticLog()
    {
        if (diagnosticLogger == null)
            return;

        unsubscribeLogger(diagnosticLogger);
        diagnosticLogger.stop();
        consumers.remove(diagnosticLogger);
        diagnosticLogger = null;
        diagnosticLogOptions = null;
    }

    public synchronized DiagnosticLogOptions getDiagnosticLogOptions()
    {
        if (diagnosticLogOptions == null)
            return DatabaseDescriptor.getDiagnosticLoggingOptions();

        return diagnosticLogOptions.enabled ? diagnosticLogOptions : DatabaseDescriptor.getDiagnosticLoggingOptions();
    }

    public synchronized void enablePersistentDiagnosticLog(DiagnosticLogOptions options)
    {
        if (!options.enabled)
            return;

        logger.info("Enabling persistent diagnostic logging");

        IDiagnosticLogger oldLogger = diagnosticLogger;

        diagnosticLogger = getDiagnosticLogger(options);
        this.diagnosticLogOptions = options;

        // subscribe to all events there are some subscriptions for to log all events
        // which are somewhere subscribed
        for (Class clazz : DiagnosticEventService.instance().getSubscribersByClass())
            DiagnosticEventService.instance().subscribe(clazz, diagnosticLogger);

        // TODO - subscribe by class and type too

        consumers.add(diagnosticLogger);

        if (oldLogger != null)
        {
            oldLogger.stop();
            consumers.remove(oldLogger);
        }
    }

    public boolean isPersistentDiagnosticLogEnabled()
    {
        return diagnosticLogger != null && diagnosticLogger.isEnabled() && consumers.contains(diagnosticLogger);
    }

    public boolean isInMemoryDiagnosticsEnabled()
    {
        return consumers.contains(inMemoryLogger);
    }

    public void enableEventPersistence(String eventClazz)
    {
        enableEventPersistence(eventClazz, null);
    }

    /**
     * Enables event persistence for a given event class and event type.
     *
     * @param eventClass event class to enable persistence for
     * @param type       type of event to enable persistence for, when null, all types of given event class will be enabled
     * @param <T>        type parameter of event type
     */
    public <T extends Enum<T>> void enableEventPersistence(String eventClass, T type)
    {
        try
        {
            if (!consumers.isEmpty())
            {
                if (type != null)
                    logger.info("Enabling events {} for type {}", eventClass, type);
                else
                    logger.info("Enabling events {}", eventClass);

                Class<DiagnosticEvent> eventClazz = getEventClass(eventClass);

                for (Consumer<DiagnosticEvent> consumer : consumers)
                {
                    if (type != null)
                        DiagnosticEventService.instance().subscribe(eventClazz, type, consumer);
                    else
                        DiagnosticEventService.instance().subscribe(eventClazz, consumer);
                }
            }
        }
        catch (ClassNotFoundException | InvalidClassException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Disables event persistence for a given event class.
     *
     * @param eventClass event class to disable persistence for
     */
    public void disableEventPersistence(String eventClass)
    {
        disableEventPersistence(eventClass, null);
    }

    /**
     * Disables event persistence for a given event class and event type.
     *
     * @param eventClass event class to disable event for
     * @param eventType  type of event of given event class, when null, all types of given event class will be disabled
     * @param <T>        type parameter of event type
     */
    public <T extends Enum<T>> void disableEventPersistence(String eventClass, T eventType)
    {
        try
        {
            if (!consumers.isEmpty())
            {
                if (eventType == null)
                {
                    logger.info("Disabling events {}", eventClass);
                    DiagnosticEventService.instance().unsubscribe(getEventClass(eventClass), consumers);
                }
                else
                {
                    logger.info("Disabling events {} for type {}", eventClass, eventType);
                    DiagnosticEventService.instance().unsubscribe(getEventClass(eventClass), eventType, consumers);
                }
            }
        }
        catch (ClassNotFoundException | InvalidClassException e)
        {
            throw new RuntimeException(e);
        }
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

    private IDiagnosticLogger getDiagnosticLogger(DiagnosticLogOptions options)
    {
        if (!options.enabled)
            return new NoOpDiagnosticLogger(Collections.emptyMap());

        return FBUtilities.newDiagnosticLogger(options.logger.class_name, options.toMap());
    }


    public SortedMap<Long, Map<String, Serializable>> getEvents(String eventClazz)
    {
        return getEvents(eventClazz, 0L, 0, true);
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

        return getEventsInternal(inMemoryLogger.getStore(cls), eventClazz, key, limit, includeKey);
    }

    public Map<Long, Map<String, Serializable>> getAllEvents()
    {
        if (inMemoryLogger == null)
            return new TreeMap<>();

        TreeMap<Long, Map<String, Serializable>> allEvents = new TreeMap<>();

        for (Map.Entry<String, DiagnosticEventStore<Long>> entry : inMemoryLogger.stores.entrySet())
        {
            DiagnosticEventStore<Long> store = entry.getValue();
            SortedMap<Long, Map<String, Serializable>> events = getEventsInternal(store, entry.getKey(), 0L, 0, true);
            allEvents.putAll(events);
        }

        return allEvents;
    }

    private SortedMap<Long, Map<String, Serializable>> getEventsInternal(DiagnosticEventStore<Long> store,
                                                                         String eventClazz,
                                                                         Long key,
                                                                         int limit,
                                                                         boolean includeKey)
    {
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

    /**
     * Remove diagnostic events from memory stores.
     */
    public void removeEvents()
    {
        for (Map.Entry<String, DiagnosticEventStore<Long>> storeEntry : inMemoryLogger.stores.entrySet())
            storeEntry.getValue().reset();
    }

    /**
     * Remove diagnostic events from memory store of given event class.
     *
     * @param eventClass event class to remove all events of
     */
    public void removeEvents(String eventClass)
    {
        inMemoryLogger.getStoreIfExists(eventClass).ifPresent(DiagnosticEventStore::reset);
    }

    private static class InMemoryDiagnosticLogger implements IDiagnosticLogger
    {
        private final Map<String, DiagnosticEventStore<Long>> stores = new ConcurrentHashMap<>();

        @Override
        public boolean isEnabled()
        {
            return DatabaseDescriptor.diagnosticEventsEnabled();
        }

        @Override
        public void stop()
        {
            stores.forEach((aClass, store) -> store.reset());
            stores.clear();
        }

        @Override
        public void accept(DiagnosticEvent event)
        {
            if (!isEnabled())
                return;

            Class<? extends DiagnosticEvent> cls = event.getClass();
            if (logger.isTraceEnabled())
                logger.trace("Persisting received {} event", cls.getName());
            DiagnosticEventStore<Long> store = getStore(cls);
            store.store(event);
            LastEventIdBroadcaster.instance().setLastEventId(event.getClass().getName(), store.getLastEventId());
        }

        /**
         * Return store if exists, otherwise return null, do not create it.
         *
         * @param eventClassName class name of diagnostic event class
         * @return store if exists, empty optional otherwise
         */
        public Optional<DiagnosticEventStore<Long>> getStoreIfExists(String eventClassName)
        {
            return Optional.ofNullable(stores.get(eventClassName));
        }

        /**
         * Return store, if it does not exist, create empty store and memoize it.
         *
         * @param eventClass class of event to create store for
         * @return store of given class
         */
        public DiagnosticEventStore<Long> getStore(Class eventClass)
        {
            return stores.computeIfAbsent(eventClass.getName(), (storeKey) -> new DiagnosticEventMemoryStore());
        }
    }
}
