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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.InitialSSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Tracks all sstables in use on the local node.
 *
 * <p>Each table tracks its own SSTables in {@link ColumnFamilyStore} (through {@link Tracker}) for most purposes, but
 * this class groups information we need on all the sstables the node has.
 */
public class SSTablesGlobalTracker implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(SSTablesGlobalTracker.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);

    /*
     * As of CASSANDRA-15897, the only thing we track here is the set of sstable versions in use.
     *
     * That set is maintained in `versionsInUse`, an immutable set replaced when changed (so that it can be read safely
     * and cheaply). To track when that set changes and needs to be re-computed, we essentially maintain a map of
     * sstables versions in use to the number of sstables for that version. But as we know that sstables will
     * overwhelmingly be on the "current" version, we special case said current version (makes things cheaper without
     * too much complexity here). Those are `sstablesForCurrentVersion` and `sstablesForOtherVersions`.
     *
     * This would be sufficient if we could guarantee that for any sstable, we only ever have 1 addition notification
     * and 1 corresponding remove notification. But sstables can have complex lifecycles and relying on this property
     * could be fragile. As a matter of fact, at the time of this writing, the removal notification is sometimes fired
     * twice for the same sstable. To keep this component more resilient, we also maintain the set of all sstables for
     * which we've received an addition, which allows us to ignore removals for sstables we don't know.
     *
     * Concurrency handling: the 'allSSTables' set handles concurrency directly as it is updated in all cases. The rest
     * of the data structures of this class are only updated together within a synchronized block when handling new
     * sstables additions/removals.
     */

    private final Set<Descriptor> allSSTables = ConcurrentHashMap.newKeySet();

    private final Version currentVersion;
    private int sstablesForCurrentVersion;
    private final Map<Version, Integer> sstablesForOtherVersions = new HashMap<>();

    private volatile ImmutableSet<Version> versionsInUse = ImmutableSet.of();

    private final Set<INotificationConsumer> subscribers = new CopyOnWriteArraySet<>();

    public SSTablesGlobalTracker(SSTableFormat<?, ?> currentSSTableFormat)
    {
        this.currentVersion = currentSSTableFormat.getLatestVersion();
    }

    /**
     * The set of all sstable versions currently in use on this node.
     */
    public Set<Version> versionsInUse()
    {
        return versionsInUse;
    }

    /**
     * Register a new subscriber to this tracker.
     *
     * Registered subscribers are currently notified when the set of sstable versions in use changes, using a
     * {@link SSTablesVersionsInUseChangeNotification}.
     *
     * @param subscriber the new subscriber to register. If this subscriber is already registered, this method does
     * nothing (meaning that even if a subscriber is registered multiple times, it will only be notified once on every
     * change).
     * @return whether the subscriber was register (so whether it was not already registered).
     */
    public boolean register(INotificationConsumer subscriber)
    {
        return subscribers.add(subscriber);
    }

    /**
     * Unregister a subscriber from this tracker.
     *
     * @param subscriber the subscriber to unregister. If this subscriber is not registered, this method does nothing.
     * @return whether the subscriber was unregistered (so whether it was registered subscriber of this tracker).
     */
    public boolean unregister(INotificationConsumer subscriber)
    {
        return subscribers.remove(subscriber);
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        Iterable<Descriptor> removed = removedSSTables(notification);
        Iterable<Descriptor> added = addedSSTables(notification);
        if (Iterables.isEmpty(removed) && Iterables.isEmpty(added))
            return;

        boolean triggerUpdate = handleSSTablesChange(removed, added);
        if (triggerUpdate)
        {
            SSTablesVersionsInUseChangeNotification changeNotification = new SSTablesVersionsInUseChangeNotification(versionsInUse);
            subscribers.forEach(s -> s.handleNotification(changeNotification, this));
        }
    }

    @VisibleForTesting
    boolean handleSSTablesChange(Iterable<Descriptor> removed, Iterable<Descriptor> added)
    {
        /*
         We collect changes to 'sstablesForCurrentVersion' and 'sstablesForOtherVersions' as delta first, and then
         apply those delta within a synchronized block below. The goal being to reduce the work done in that
         synchronized block.
        */
        int currentDelta = 0;
        Map<Version, Integer> othersDelta = null;
        /*
         Note: we deal with removes first as if a notification both removes and adds, it's a compaction and while
         it should never remove and add the same descriptor in practice, doing the remove first is more logical.
        */
        for (Descriptor desc  : removed)
        {
            if (!allSSTables.remove(desc))
                continue;

            Version version = desc.version;
            if (currentVersion.equals(version))
                --currentDelta;
            else
                othersDelta = update(othersDelta, version, -1);
        }
        for (Descriptor desc : added)
        {
            if (!allSSTables.add(desc))
                continue;

            Version version = desc.version;
            if (currentVersion.equals(version))
                ++currentDelta;
            else
                othersDelta = update(othersDelta, version, +1);
        }

        if (currentDelta == 0 && (othersDelta == null))
            return false;

        /*
         Set to true if the set of versions in use is changed by this update. That is, if a version having no
         version prior now has some, or if the count for some version reaches 0.
        */
        boolean triggerUpdate;
        synchronized (this)
        {
            triggerUpdate = (currentDelta > 0 && sstablesForCurrentVersion == 0)
                            || (currentDelta < 0 && sstablesForCurrentVersion <= -currentDelta);
            sstablesForCurrentVersion += currentDelta;
            sstablesForCurrentVersion = sanitizeSSTablesCount(sstablesForCurrentVersion, currentVersion);

            if (othersDelta != null)
            {
                for (Map.Entry<Version, Integer> entry : othersDelta.entrySet())
                {
                    Version version = entry.getKey();
                    int delta = entry.getValue();
                    /*
                     Updates the count, removing the version if it reaches 0 (note: we could use Map#compute for this,
                     but we wouldn't be able to modify `triggerUpdate` without making it an Object, so we don't bother).
                    */
                    Integer oldValue = sstablesForOtherVersions.get(version);
                    int newValue = oldValue == null ? delta : oldValue + delta;
                    newValue = sanitizeSSTablesCount(newValue, version);
                    triggerUpdate |= oldValue == null || newValue == 0;
                    if (newValue == 0)
                        sstablesForOtherVersions.remove(version);
                    else
                        sstablesForOtherVersions.put(version, newValue);
                }
            }

            if (triggerUpdate)
                versionsInUse = computeVersionsInUse(sstablesForCurrentVersion, currentVersion, sstablesForOtherVersions);
        }
        return triggerUpdate;
    }

    private static ImmutableSet<Version> computeVersionsInUse(int sstablesForCurrentVersion, Version currentVersion, Map<Version, Integer> sstablesForOtherVersions)
    {
        ImmutableSet.Builder<Version> builder = ImmutableSet.builder();
        if (sstablesForCurrentVersion > 0)
            builder.add(currentVersion);
        builder.addAll(sstablesForOtherVersions.keySet());
        return builder.build();
    }

    private static int sanitizeSSTablesCount(int sstableCount, Version version)
    {
        if (sstableCount >= 0)
            return sstableCount;

        /*
         This shouldn't happen and indicate a bug either in the tracking of this class, or on the passed notification.
         That said, it's not worth bringing the node down, so we log the problem but otherwise "correct" it.
        */
        noSpamLogger.error("Invalid state while handling sstables change notification: the number of sstables for " +
                           "version {} was computed to {}. This indicate a bug and please report it, but it should " +
                           "not have adverse consequences.", version.toFormatAndVersionString(), sstableCount, new RuntimeException());
        return 0;
    }

    private static Iterable<Descriptor> addedSSTables(INotification notification)
    {
        if (notification instanceof SSTableAddedNotification)
            return Iterables.transform(((SSTableAddedNotification)notification).added, s -> s.descriptor);
        if (notification instanceof SSTableListChangedNotification)
            return Iterables.transform(((SSTableListChangedNotification)notification).added, s -> s.descriptor);
        if (notification instanceof InitialSSTableAddedNotification)
            return Iterables.transform(((InitialSSTableAddedNotification)notification).added, s -> s.descriptor);
        else
            return Collections.emptyList();
    }

    private static Iterable<Descriptor> removedSSTables(INotification notification)
    {
        if (notification instanceof SSTableDeletingNotification)
            return Collections.singletonList(((SSTableDeletingNotification)notification).deleting.descriptor);
        if (notification instanceof SSTableListChangedNotification)
            return Iterables.transform(((SSTableListChangedNotification)notification).removed, s -> s.descriptor);
        else
            return Collections.emptyList();
    }

    private static Map<Version, Integer> update(Map<Version, Integer> counts,
                                                       Version toUpdate,
                                                       int delta)
    {
        Map<Version, Integer> m = counts == null ? new HashMap<>() : counts;
        m.merge(toUpdate, delta, (a, b) -> (a + b == 0) ? null : (a + b));
        return m;
    }

}
