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
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.MBeanWrapper;

/**
 * Service for publishing and consuming {@link DiagnosticEvent}s.
 */
public final class DiagnosticEventService implements DiagnosticEventServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(DiagnosticEventService.class);

    // Subscribers interested in consuming all kind of events
    private ImmutableSet<Consumer<DiagnosticEvent>> subscribersAll = ImmutableSet.of();

    // Subscribers for particular event class, e.g. BootstrapEvent
    private ImmutableSetMultimap<Class<? extends DiagnosticEvent>, Consumer<DiagnosticEvent>> subscribersByClass = ImmutableSetMultimap.of();

    // Subscribers for event class and type, e.g. BootstrapEvent#TOKENS_ALLOCATED
    private ImmutableMap<Class, ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>>> subscribersByClassAndType = ImmutableMap.of();

    private static final DiagnosticEventService instance = new DiagnosticEventService();

    private DiagnosticEventService()
    {
        MBeanWrapper.instance.registerMBean(this,"org.apache.cassandra.diag:type=DiagnosticEventService");

        // register broadcasters for JMX events
        DiagnosticEventPersistence.start();
    }

    /**
     * Makes provided event available to all subscribers.
     */
    public void publish(DiagnosticEvent event)
    {
        if (!DatabaseDescriptor.diagnosticEventsEnabled())
            return;

        logger.trace("Publishing: {}={}", event.getClass().getName(), event.toMap());

        // event class + type
        ImmutableMultimap<Enum<?>, Consumer<DiagnosticEvent>> consumersByType = subscribersByClassAndType.get(event.getClass());
        if (consumersByType != null)
        {
            ImmutableCollection<Consumer<DiagnosticEvent>> consumers = consumersByType.get(event.getType());
            if (consumers != null)
            {
                for (Consumer<DiagnosticEvent> consumer : consumers)
                    consumer.accept(event);
            }
        }

        // event class
        Set<Consumer<DiagnosticEvent>> consumersByEvents = subscribersByClass.get(event.getClass());
        if (consumersByEvents != null)
        {
            for (Consumer<DiagnosticEvent> consumer : consumersByEvents)
                consumer.accept(event);
        }

        // all events
        for (Consumer<DiagnosticEvent> consumer : subscribersAll)
            consumer.accept(event);
    }

    /**
     * Registers event handler for specified class of events.
     * @param event DiagnosticEvent class implementation
     * @param consumer Consumer for received events
     */
    public synchronized <E extends DiagnosticEvent> void subscribe(Class<E> event, Consumer<E> consumer)
    {
        logger.debug("Adding subscriber: {}", consumer);
        subscribersByClass = ImmutableSetMultimap.<Class<? extends DiagnosticEvent>, Consumer<DiagnosticEvent>>builder()
                              .putAll(subscribersByClass)
                              .put(event, new TypedConsumerWrapper<>(consumer))
                              .build();
        logger.debug("Total subscribers: {}", subscribersByClass.values().size());
    }

    /**
     * Registers event handler for specified class of events.
     * @param event DiagnosticEvent class implementation
     * @param consumer Consumer for received events
     */
    public synchronized <E extends DiagnosticEvent, T extends Enum<T>> void subscribe(Class<E> event,
                                                                                      T eventType,
                                                                                      Consumer<E> consumer)
    {
        ImmutableSetMultimap.Builder<Enum<?>, Consumer<DiagnosticEvent>> byTypeBuilder = ImmutableSetMultimap.builder();
        if (subscribersByClassAndType.containsKey(event))
            byTypeBuilder.putAll(subscribersByClassAndType.get(event));
        byTypeBuilder.put(eventType, new TypedConsumerWrapper<>(consumer));

        ImmutableMap.Builder<Class, ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>>> byClassBuilder = ImmutableMap.builder();
        for (Class clazz : subscribersByClassAndType.keySet())
        {
            if (!clazz.equals(event))
                byClassBuilder.put(clazz, subscribersByClassAndType.get(clazz));
        }

        subscribersByClassAndType = byClassBuilder
                                    .put(event, byTypeBuilder.build())
                                    .build();
    }

    /**
     * Registers event handler for all DiagnosticEvents published from this point.
     * @param consumer Consumer for received events
     */
    public synchronized void subscribeAll(Consumer<DiagnosticEvent> consumer)
    {
        subscribersAll = ImmutableSet.<Consumer<DiagnosticEvent>>builder()
                         .addAll(subscribersAll)
                         .add(consumer)
                         .build();
    }

    /**
     * De-registers event handler from receiving any further events.
     * @param consumer Consumer registered for receiving events
     */
    public synchronized <E extends DiagnosticEvent> void unsubscribe(Consumer<E> consumer)
    {
        unsubscribe(null, consumer);
    }

    /**
     * De-registers event handler from receiving any further events.
     * @param event DiagnosticEvent class to unsubscribe from
     * @param consumer Consumer registered for receiving events
     */
    public synchronized <E extends DiagnosticEvent> void unsubscribe(@Nullable Class<E> event, Consumer<E> consumer)
    {
        // all events
        subscribersAll = ImmutableSet.copyOf(Iterables.filter(subscribersAll, (c) -> c != consumer));

        // event class
        ImmutableSetMultimap.Builder<Class<? extends DiagnosticEvent>, Consumer<DiagnosticEvent>> byClassBuilder = ImmutableSetMultimap.builder();
        Collection<Map.Entry<Class<? extends DiagnosticEvent>, Consumer<DiagnosticEvent>>> entries = subscribersByClass.entries();
        for (Map.Entry<Class<? extends DiagnosticEvent>, Consumer<DiagnosticEvent>> entry : entries)
        {
            Consumer<DiagnosticEvent> subscriber = entry.getValue();
            if (subscriber instanceof TypedConsumerWrapper)
                subscriber = ((TypedConsumerWrapper)subscriber).wrapped;

            // other consumers or other events
            if (subscriber != consumer || (event != null && !entry.getKey().equals(event)))
            {
                byClassBuilder = byClassBuilder.put(entry);
            }
        }
        subscribersByClass = byClassBuilder.build();


        // event class + type
        ImmutableMap.Builder<Class, ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>>> byClassAndTypeBuilder = ImmutableMap.builder();
        for (Map.Entry<Class, ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>>> byClassEntry : subscribersByClassAndType.entrySet())
        {
            ImmutableSetMultimap.Builder<Enum<?>, Consumer<DiagnosticEvent>> byTypeBuilder = ImmutableSetMultimap.builder();
            ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>> byTypeConsumers = byClassEntry.getValue();
            Iterables.filter(byTypeConsumers.entries(), (e) ->
            {
                if (e == null || e.getValue() == null) return false;
                Consumer<DiagnosticEvent> subscriber = e.getValue();
                if (subscriber instanceof TypedConsumerWrapper)
                    subscriber = ((TypedConsumerWrapper) subscriber).wrapped;
                return subscriber != consumer || (event != null && !byClassEntry.getKey().equals(event));
            }).forEach(byTypeBuilder::put);

            ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>> byType = byTypeBuilder.build();
            if (!byType.isEmpty())
                byClassAndTypeBuilder.put(byClassEntry.getKey(), byType);
        }

        subscribersByClassAndType = byClassAndTypeBuilder.build();
    }

    /**
     * Indicates if any {@link Consumer} has been registered for the specified class of events.
     * @param event DiagnosticEvent class implementation
     */
    public <E extends DiagnosticEvent> boolean hasSubscribers(Class<E> event)
    {
        return !subscribersAll.isEmpty() || subscribersByClass.containsKey(event) || subscribersByClassAndType.containsKey(event);
    }

    /**
     * Indicates if any {@link Consumer} has been registered for the specified class of events.
     * @param event DiagnosticEvent class implementation
     * @param eventType Subscribed event type matched against {@link DiagnosticEvent#getType()}
     */
    public <E extends DiagnosticEvent, T extends Enum<T>> boolean hasSubscribers(Class<E> event, T eventType)
    {
        if (!subscribersAll.isEmpty())
            return true;

        ImmutableSet<Consumer<DiagnosticEvent>> subscribers = subscribersByClass.get(event);
        if (subscribers != null && !subscribers.isEmpty())
            return true;

        ImmutableSetMultimap<Enum<?>, Consumer<DiagnosticEvent>> byType = subscribersByClassAndType.get(event);
        if (byType == null || byType.isEmpty()) return false;

        Set<Consumer<DiagnosticEvent>> consumers = byType.get(eventType);
        return consumers != null && !consumers.isEmpty();
    }

    /**
     * Indicates if events are enabled for specified event class based on {@link DatabaseDescriptor#diagnosticEventsEnabled()}
     * and {@link #hasSubscribers(Class)}.
     * @param event DiagnosticEvent class implementation
     */
    public <E extends DiagnosticEvent> boolean isEnabled(Class<E> event)
    {
        return DatabaseDescriptor.diagnosticEventsEnabled() && hasSubscribers(event);
    }

    /**
     * Indicates if events are enabled for specified event class based on {@link DatabaseDescriptor#diagnosticEventsEnabled()}
     * and {@link #hasSubscribers(Class, Enum)}.
     * @param event DiagnosticEvent class implementation
     * @param eventType Subscribed event type matched against {@link DiagnosticEvent#getType()}
     */
    public <E extends DiagnosticEvent, T extends Enum<T>> boolean isEnabled(Class<E> event, T eventType)
    {
        return DatabaseDescriptor.diagnosticEventsEnabled() && hasSubscribers(event, eventType);
    }

    public static DiagnosticEventService instance()
    {
        return instance;
    }

    /**
     * Removes all active subscribers. Should only be called from testing.
     */
    public synchronized void cleanup()
    {
        subscribersByClass = ImmutableSetMultimap.of();
        subscribersAll = ImmutableSet.of();
        subscribersByClassAndType = ImmutableMap.of();
    }

    public boolean isDiagnosticsEnabled()
    {
        return DatabaseDescriptor.diagnosticEventsEnabled();
    }

    public void disableDiagnostics()
    {
        DatabaseDescriptor.setDiagnosticEventsEnabled(false);
    }

    public SortedMap<Long, Map<String, Serializable>> readEvents(String eventClazz, Long lastKey, int limit)
    {
        return DiagnosticEventPersistence.instance().getEvents(eventClazz, lastKey, limit, false);
    }

    public void enableEventPersistence(String eventClazz)
    {
        DiagnosticEventPersistence.instance().enableEventPersistence(eventClazz);
    }

    public void disableEventPersistence(String eventClazz)
    {
        DiagnosticEventPersistence.instance().disableEventPersistence(eventClazz);
    }

    /**
     * Wrapper class for supporting typed event handling for consumers.
     */
    private static class TypedConsumerWrapper<E> implements Consumer<DiagnosticEvent>
    {
        private final Consumer<E> wrapped;

        private TypedConsumerWrapper(Consumer<E> wrapped)
        {
            this.wrapped = wrapped;
        }

        public void accept(DiagnosticEvent e)
        {
            wrapped.accept((E)e);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TypedConsumerWrapper<?> that = (TypedConsumerWrapper<?>) o;
            return Objects.equals(wrapped, that.wrapped);
        }

        public int hashCode()
        {
            return Objects.hash(wrapped);
        }
    }
}
