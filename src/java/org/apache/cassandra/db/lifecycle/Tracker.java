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
package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.*;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.Iterables.filter;
import static java.util.Collections.singleton;
import static org.apache.cassandra.db.lifecycle.Helpers.*;
import static org.apache.cassandra.db.lifecycle.View.permitCompacting;
import static org.apache.cassandra.db.lifecycle.View.updateCompacting;
import static org.apache.cassandra.db.lifecycle.View.updateLiveSet;
import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;
import static org.apache.cassandra.utils.concurrent.Refs.release;
import static org.apache.cassandra.utils.concurrent.Refs.selfRefs;

public class Tracker
{
    private static final Logger logger = LoggerFactory.getLogger(Tracker.class);

    public final Collection<INotificationConsumer> subscribers = new CopyOnWriteArrayList<>();
    public final ColumnFamilyStore cfstore;
    final AtomicReference<View> view;
    public final boolean loadsstables;

    public Tracker(ColumnFamilyStore cfstore, boolean loadsstables)
    {
        this.cfstore = cfstore;
        this.view = new AtomicReference<>();
        this.loadsstables = loadsstables;
        this.reset();
    }

    public LifecycleTransaction tryModify(SSTableReader sstable, OperationType operationType)
    {
        return tryModify(singleton(sstable), operationType);
    }

    /**
     * @return a Transaction over the provided sstables if we are able to mark the given @param sstables as compacted, before anyone else
     */
    public LifecycleTransaction tryModify(Iterable<SSTableReader> sstables, OperationType operationType)
    {
        if (Iterables.isEmpty(sstables))
            return new LifecycleTransaction(this, operationType, sstables);
        if (null == apply(permitCompacting(sstables), updateCompacting(emptySet(), sstables)))
            return null;
        return new LifecycleTransaction(this, operationType, sstables);
    }


    // METHODS FOR ATOMICALLY MODIFYING THE VIEW

    Pair<View, View> apply(Function<View, View> function)
    {
        return apply(Predicates.<View>alwaysTrue(), function);
    }

    Throwable apply(Function<View, View> function, Throwable accumulate)
    {
        try
        {
            apply(function);
        }
        catch (Throwable t)
        {
            accumulate = merge(accumulate, t);
        }
        return accumulate;
    }

    /**
     * atomically tests permit against the view and applies function to it, if permit yields true, returning the original;
     * otherwise the method aborts, returning null
     */
    Pair<View, View> apply(Predicate<View> permit, Function<View, View> function)
    {
        while (true)
        {
            View cur = view.get();
            if (!permit.apply(cur))
                return null;
            View updated = function.apply(cur);
            if (view.compareAndSet(cur, updated))
                return Pair.create(cur, updated);
        }
    }

    Throwable updateSizeTracking(Iterable<SSTableReader> oldSSTables, Iterable<SSTableReader> newSSTables, Throwable accumulate)
    {
        if (isDummy())
            return accumulate;

        long add = 0;
        for (SSTableReader sstable : newSSTables)
        {
            if (logger.isTraceEnabled())
                logger.trace("adding {} to list of files tracked for {}.{}", sstable.descriptor, cfstore.keyspace.getName(), cfstore.name);
            try
            {
                add += sstable.bytesOnDisk();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        long subtract = 0;
        for (SSTableReader sstable : oldSSTables)
        {
            if (logger.isTraceEnabled())
                logger.trace("removing {} from list of files tracked for {}.{}", sstable.descriptor, cfstore.keyspace.getName(), cfstore.name);
            try
            {
                subtract += sstable.bytesOnDisk();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        StorageMetrics.load.inc(add - subtract);
        cfstore.metric.liveDiskSpaceUsed.inc(add - subtract);
        // we don't subtract from total until the sstable is deleted
        cfstore.metric.totalDiskSpaceUsed.inc(add);
        return accumulate;
    }

    // SETUP / CLEANUP

    public void addInitialSSTables(Iterable<SSTableReader> sstables)
    {
        if (!isDummy())
            setupKeycache(sstables);
        apply(updateLiveSet(emptySet(), sstables));
        maybeFail(updateSizeTracking(emptySet(), sstables, null));
        // no notifications or backup necessary
    }

    public void addSSTables(Iterable<SSTableReader> sstables)
    {
        addInitialSSTables(sstables);
        for (SSTableReader sstable : sstables)
        {
            maybeIncrementallyBackup(sstable);
            notifyAdded(sstable);
        }
    }

    /** (Re)initializes the tracker, purging all references. */
    @VisibleForTesting
    public void reset()
    {
        view.set(new View(
                         !isDummy() ? ImmutableList.of(new Memtable(new AtomicReference<>(CommitLog.instance.getContext()), cfstore))
                                    : ImmutableList.<Memtable>of(),
                         ImmutableList.<Memtable>of(),
                         Collections.<SSTableReader, SSTableReader>emptyMap(),
                         Collections.<SSTableReader>emptySet(),
                         Collections.<SSTableReader>emptySet(),
                         SSTableIntervalTree.empty()));
    }

    public Throwable dropSSTablesIfInvalid(Throwable accumulate)
    {
        if (!isDummy() && !cfstore.isValid())
            accumulate = dropSSTables(accumulate);
        return accumulate;
    }

    public void dropSSTables()
    {
        maybeFail(dropSSTables(null));
    }

    public Throwable dropSSTables(Throwable accumulate)
    {
        return dropSSTables(Predicates.<SSTableReader>alwaysTrue(), OperationType.UNKNOWN, accumulate);
    }

    /**
     * removes all sstables that are not busy compacting.
     */
    public Throwable dropSSTables(final Predicate<SSTableReader> remove, OperationType operationType, Throwable accumulate)
    {
        Pair<View, View> result = apply(new Function<View, View>()
        {
            public View apply(View view)
            {
                Set<SSTableReader> toremove = copyOf(filter(view.sstables, and(remove, notIn(view.compacting))));
                return updateLiveSet(toremove, emptySet()).apply(view);
            }
        });

        Set<SSTableReader> removed = Sets.difference(result.left.sstables, result.right.sstables);
        assert Iterables.all(removed, remove);

        if (!removed.isEmpty())
        {
            // notifySSTablesChanged -> LeveledManifest.promote doesn't like a no-op "promotion"
            accumulate = notifySSTablesChanged(removed, Collections.<SSTableReader>emptySet(), operationType, accumulate);
            accumulate = updateSizeTracking(removed, emptySet(), accumulate);
            accumulate = markObsolete(this, removed, accumulate);
            accumulate = release(selfRefs(removed), accumulate);
        }
        return accumulate;
    }

    /**
     * Removes every SSTable in the directory from the Tracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    public void removeUnreadableSSTables(final File directory)
    {
        maybeFail(dropSSTables(new Predicate<SSTableReader>()
        {
            public boolean apply(SSTableReader reader)
            {
                return reader.descriptor.directory.equals(directory);
            }
        }, OperationType.UNKNOWN, null));
    }



    // FLUSHING

    /**
     * get the Memtable that the ordered writeOp should be directed to
     */
    public Memtable getMemtableFor(OpOrder.Group opGroup, ReplayPosition replayPosition)
    {
        // since any new memtables appended to the list after we fetch it will be for operations started
        // after us, we can safely assume that we will always find the memtable that 'accepts' us;
        // if the barrier for any memtable is set whilst we are reading the list, it must accept us.

        // there may be multiple memtables in the list that would 'accept' us, however we only ever choose
        // the oldest such memtable, as accepts() only prevents us falling behind (i.e. ensures we don't
        // assign operations to a memtable that was retired/queued before we started)
        for (Memtable memtable : view.get().liveMemtables)
        {
            if (memtable.accepts(opGroup, replayPosition))
                return memtable;
        }
        throw new AssertionError(view.get().liveMemtables.toString());
    }

    /**
     * Switch the current memtable. This atomically appends a new memtable to the end of the list of active memtables,
     * returning the previously last memtable. It leaves the previous Memtable in the list of live memtables until
     * discarding(memtable) is called. These two methods must be synchronized/paired, i.e. m = switchMemtable
     * must be followed by discarding(m), they cannot be interleaved.
     *
     * @return the previously active memtable
     */
    public Memtable switchMemtable(boolean truncating, Memtable newMemtable)
    {
        Pair<View, View> result = apply(View.switchMemtable(newMemtable));
        if (truncating)
            notifyRenewed(newMemtable);

        return result.left.getCurrentMemtable();
    }

    public void markFlushing(Memtable memtable)
    {
        apply(View.markFlushing(memtable));
    }

    public void replaceFlushed(Memtable memtable, SSTableReader sstable)
    {
        assert !isDummy();
        if (sstable == null)
        {
            // sstable may be null if we flushed batchlog and nothing needed to be retained
            // if it's null, we don't care what state the cfstore is in, we just replace it and continue
            apply(View.replaceFlushed(memtable, null));
            return;
        }

        sstable.setupKeyCache();
        // back up before creating a new Snapshot (which makes the new one eligible for compaction)
        maybeIncrementallyBackup(sstable);

        apply(View.replaceFlushed(memtable, sstable));

        Throwable fail;
        fail = updateSizeTracking(emptySet(), singleton(sstable), null);

        maybeFail(fail);
    }

    /**
     * permit compaction of the provided sstable; this translates to notifying compaction
     * strategies of its existence, and potentially submitting a background task
     */
    public void permitCompactionOfFlushed(SSTableReader sstable)
    {
        if (sstable == null)
            return;

        apply(View.permitCompactionOfFlushed(sstable));

        if (isDummy())
            return;

        if (cfstore.isValid())
        {
            notifyAdded(sstable);
            CompactionManager.instance.submitBackground(cfstore);
        }
        else
        {
            dropSSTables();
        }
    }



    // MISCELLANEOUS public utility calls

    public Set<SSTableReader> getSSTables()
    {
        return view.get().sstables;
    }

    public Iterable<SSTableReader> getPermittedToCompact()
    {
        View view = this.view.get();
        return filter(view.sstables, not(in(view.premature)));
    }

    public Set<SSTableReader> getCompacting()
    {
        return view.get().compacting;
    }

    public Set<SSTableReader> getUncompacting()
    {
        return view.get().nonCompactingSStables();
    }

    public Iterable<SSTableReader> getUncompacting(Iterable<SSTableReader> candidates)
    {
        return view.get().getUncompacting(candidates);
    }

    public void maybeIncrementallyBackup(final SSTableReader sstable)
    {
        if (!DatabaseDescriptor.isIncrementalBackupsEnabled())
            return;

        File backupsDir = Directories.getBackupsDirectory(sstable.descriptor);
        sstable.createLinks(FileUtils.getCanonicalPath(backupsDir));
    }


    // NOTIFICATION

    Throwable notifySSTablesChanged(Collection<SSTableReader> removed, Collection<SSTableReader> added, OperationType compactionType, Throwable accumulate)
    {
        INotification notification = new SSTableListChangedNotification(added, removed, compactionType);
        for (INotificationConsumer subscriber : subscribers)
        {
            try
            {
                subscriber.handleNotification(notification, this);
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        return accumulate;
    }

    Throwable notifyAdded(SSTableReader added, Throwable accumulate)
    {
        INotification notification = new SSTableAddedNotification(added);
        for (INotificationConsumer subscriber : subscribers)
        {
            try
            {
                subscriber.handleNotification(notification, this);
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        return accumulate;
    }

    public void notifyAdded(SSTableReader added)
    {
        maybeFail(notifyAdded(added, null));
    }

    public void notifySSTableRepairedStatusChanged(Collection<SSTableReader> repairStatusesChanged)
    {
        INotification notification = new SSTableRepairStatusChanged(repairStatusesChanged);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);
    }

    public void notifyDeleting(SSTableReader deleting)
    {
        INotification notification = new SSTableDeletingNotification(deleting);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);
    }

    public void notifyRenewed(Memtable renewed)
    {
        INotification notification = new MemtableRenewedNotification(renewed);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);
    }

    public void notifyTruncated(long truncatedAt)
    {
        INotification notification = new TruncationNotification(truncatedAt);
        for (INotificationConsumer subscriber : subscribers)
            subscriber.handleNotification(notification, this);
    }

    public boolean isDummy()
    {
        return cfstore == null;
    }

    public void subscribe(INotificationConsumer consumer)
    {
        subscribers.add(consumer);
    }

    public void unsubscribe(INotificationConsumer consumer)
    {
        subscribers.remove(consumer);
    }

    private static Set<SSTableReader> emptySet()
    {
        return Collections.emptySet();
    }

    public View getView()
    {
        return view.get();
    }
}
