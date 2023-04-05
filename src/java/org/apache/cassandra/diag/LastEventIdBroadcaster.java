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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.progress.jmx.JMXBroadcastExecutor;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Broadcaster for notifying JMX clients on newly available data. Periodically sends {@link Notification}s
 * containing a list of event types and greatest event IDs. Consumers may use this information to
 * query or poll events based on this data.
 */
final class LastEventIdBroadcaster extends NotificationBroadcasterSupport implements LastEventIdBroadcasterMBean
{

    private final static LastEventIdBroadcaster instance = new LastEventIdBroadcaster();

    private final static int PERIODIC_BROADCAST_INTERVAL_MILLIS = 30000;
    private final static int SHORT_TERM_BROADCAST_DELAY_MILLIS = 1000;

    private final AtomicLong notificationSerialNumber = new AtomicLong();
    private final AtomicReference<ScheduledFuture<?>> scheduledPeriodicalBroadcast = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> scheduledShortTermBroadcast = new AtomicReference<>();

    private final Map<String, Comparable> summary = new ConcurrentHashMap<>();


    private LastEventIdBroadcaster()
    {
        // use dedicated executor for handling JMX notifications
        super(JMXBroadcastExecutor.executor);

        summary.put("last_updated_at", 0L);

        MBeanWrapper.instance.registerMBean(this, "org.apache.cassandra.diag:type=LastEventIdBroadcaster");
    }

    public static LastEventIdBroadcaster instance()
    {
        return instance;
    }

    public Map<String, Comparable> getLastEventIds()
    {
        return summary;
    }

    public Map<String, Comparable> getLastEventIdsIfModified(long lastUpdate)
    {
        if (lastUpdate >= (long)summary.get("last_updated_at")) return summary;
        else return getLastEventIds();
    }

    public synchronized void addNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback)
    {
        super.addNotificationListener(listener, filter, handback);

        // lazily schedule periodical broadcast once we got our first subscriber
        if (scheduledPeriodicalBroadcast.get() == null)
        {
            ScheduledFuture<?> scheduledFuture = ScheduledExecutors.scheduledTasks
                                                 .scheduleAtFixedRate(this::broadcastEventIds,
                                                                      PERIODIC_BROADCAST_INTERVAL_MILLIS,
                                                                      PERIODIC_BROADCAST_INTERVAL_MILLIS,
                                                                      TimeUnit.MILLISECONDS);
            if (!this.scheduledPeriodicalBroadcast.compareAndSet(null, scheduledFuture))
                scheduledFuture.cancel(false);
        }
    }

    public void setLastEventId(String key, Comparable id)
    {
        // ensure monotonic properties of ids
        if (summary.compute(key, (k, v) -> v == null ? id : id.compareTo(v) > 0 ? id : v) == id) {
            summary.put("last_updated_at", currentTimeMillis());
            scheduleBroadcast();
        }
    }

    private void scheduleBroadcast()
    {
        // schedule broadcast for timely announcing new events before next periodical broadcast
        // this should allow us to buffer new updates for a while, while keeping broadcasts near-time
        ScheduledFuture<?> running = scheduledShortTermBroadcast.get();
        if (running == null || running.isDone())
        {
            ScheduledFuture<?> scheduledFuture = ScheduledExecutors.scheduledTasks
                                                 .schedule((Runnable)this::broadcastEventIds,
                                                           SHORT_TERM_BROADCAST_DELAY_MILLIS,
                                                           TimeUnit.MILLISECONDS);
            if (!this.scheduledShortTermBroadcast.compareAndSet(running, scheduledFuture))
                scheduledFuture.cancel(false);
        }
    }

    private void broadcastEventIds()
    {
        if (!summary.isEmpty())
            broadcastEventIds(summary);
    }

    private void broadcastEventIds(Map<String, Comparable> summary)
    {
        Notification notification = new Notification("event_last_id_summary",
                                                     "LastEventIdBroadcaster",
                                                     notificationSerialNumber.incrementAndGet(),
                                                     currentTimeMillis(),
                                                     "Event last IDs summary");
        notification.setUserData(summary);
        sendNotification(notification);
    }
}
