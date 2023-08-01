/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.lifecycle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.InitialSSTableAddedNotification;
import org.apache.cassandra.notifications.MemtableDiscardedNotification;
import org.apache.cassandra.notifications.MemtableRenewedNotification;
import org.apache.cassandra.notifications.MemtableSwitchedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableMetadataChanged;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static com.google.common.collect.ImmutableSet.copyOf;
import static java.util.Collections.singleton;

public class TrackerTest
{

    private static final class MockListener implements INotificationConsumer
    {
        final boolean throwException;
        final List<INotification> received = new ArrayList<>();
        final List<Object> senders = new ArrayList<>();

        private MockListener(boolean throwException)
        {
            this.throwException = throwException;
        }

        public void handleNotification(INotification notification, Object sender)
        {
            if (throwException)
                throw new RuntimeException();
            received.add(notification);
            senders.add(sender);
        }
    }

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        MockSchema.cleanup();
    }

    @Test
    public void testTryModify()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Tracker tracker = Tracker.newDummyTracker();
        List<SSTableReader> readers = ImmutableList.of(MockSchema.sstable(0, true, cfs), MockSchema.sstable(1, cfs), MockSchema.sstable(2, cfs));
        tracker.addInitialSSTables(copyOf(readers));
        Assert.assertNull(tracker.tryModify(ImmutableList.of(MockSchema.sstable(0, cfs)), OperationType.COMPACTION));
        try (LifecycleTransaction txn = tracker.tryModify(readers.get(0), OperationType.COMPACTION))
        {
            Assert.assertNotNull(txn);
            Assert.assertNull(tracker.tryModify(readers.get(0), OperationType.COMPACTION));
            Assert.assertEquals(1, txn.originals().size());
            Assert.assertTrue(txn.originals().contains(readers.get(0)));
        }
        try (LifecycleTransaction txn = tracker.tryModify(Collections.<SSTableReader>emptyList(), OperationType.COMPACTION))
        {
            Assert.assertNotNull(txn);
            Assert.assertEquals(0, txn.originals().size());
        }
        readers.get(0).selfRef().release();
    }

    @Test
    public void testApply()
    {
        final ColumnFamilyStore cfs = MockSchema.newCFS();
        final Tracker tracker = Tracker.newDummyTracker();
        final View resultView = ViewTest.fakeView(0, 0, cfs);
        final AtomicInteger count = new AtomicInteger();
        tracker.apply(new Predicate<View>()
        {
            public boolean apply(View view)
            {
                // confound the CAS by swapping the view, and check we retry
                if (count.incrementAndGet() < 3)
                    tracker.view.set(ViewTest.fakeView(0, 0, cfs));
                return true;
            }
        }, new Function<View, View>()
        {
            @Nullable
            public View apply(View view)
            {
                return resultView;
            }
        });
        Assert.assertEquals(3, count.get());
        Assert.assertEquals(resultView, tracker.getView());

        count.set(0);
        // check that if the predicate returns false, we stop immediately and return null
        Assert.assertNull(tracker.apply(new Predicate<View>()
        {
            public boolean apply(View view)
            {
                count.incrementAndGet();
                return false;
            }
        }, null));
        Assert.assertEquals(1, count.get());
        Assert.assertEquals(resultView, tracker.getView());
    }

    @Test
    public void testAddInitialSSTables()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(metadata -> metadata.caching(CachingParams.CACHE_KEYS));
        Tracker tracker = cfs.getTracker();
        MockListener listener = new MockListener(false);
        tracker.subscribe(listener);
        List<SSTableReader> readers = ImmutableList.of(MockSchema.sstable(0, 17, cfs),
                                                       MockSchema.sstable(1, 121, cfs),
                                                       MockSchema.sstable(2, 9, cfs));
        tracker.addInitialSSTables(copyOf(readers));

        Assert.assertEquals(3, tracker.view.get().sstables.size());
        Assert.assertEquals(1, listener.senders.size());
        Assert.assertEquals(1, listener.received.size());
        Assert.assertTrue(listener.received.get(0) instanceof InitialSSTableAddedNotification);

        for (SSTableReader reader : readers)
            if (reader instanceof KeyCacheSupport<?>)
                Assert.assertTrue(((KeyCacheSupport<?>)reader).getKeyCache().isEnabled());

        Assert.assertEquals(17 + 121 + 9, cfs.metric.liveDiskSpaceUsed.getCount());
    }

    @Test
    public void testAddSSTables()
    {
        boolean backups = DatabaseDescriptor.isIncrementalBackupsEnabled();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
        ColumnFamilyStore cfs = MockSchema.newCFS(metadata -> metadata.caching(CachingParams.CACHE_KEYS));
        Tracker tracker = cfs.getTracker();
        MockListener listener = new MockListener(false);
        tracker.subscribe(listener);
        List<SSTableReader> readers = ImmutableList.of(MockSchema.sstable(0, 17, cfs),
                                                       MockSchema.sstable(1, 121, cfs),
                                                       MockSchema.sstable(2, 9, cfs));
        tracker.addSSTables(copyOf(readers));

        Assert.assertEquals(3, tracker.view.get().sstables.size());

        for (SSTableReader reader : readers)
        {
            if (reader instanceof KeyCacheSupport<?>)
                Assert.assertTrue(((KeyCacheSupport<?>)reader).getKeyCache().isEnabled());
        }

        Assert.assertEquals(17 + 121 + 9, cfs.metric.liveDiskSpaceUsed.getCount());
        Assert.assertEquals(1, listener.senders.size());
        Assert.assertEquals(1, listener.received.size());
        Assert.assertEquals(tracker, listener.senders.get(0));
        Assert.assertTrue(listener.received.get(0) instanceof SSTableAddedNotification);
        DatabaseDescriptor.setIncrementalBackupsEnabled(backups);
    }

    @Test
    public void testDropSSTables()
    {
        testDropSSTables(false);
        LogTransaction.waitForDeletions();
        testDropSSTables(true);
        LogTransaction.waitForDeletions();
    }

    private void testDropSSTables(boolean invalidate)
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Tracker tracker = cfs.getTracker();
        MockListener listener = new MockListener(false);
        tracker.subscribe(listener);
        final List<SSTableReader> readers = ImmutableList.of(MockSchema.sstable(0, 9, true, cfs),
                                                             MockSchema.sstable(1, 15, true, cfs),
                                                             MockSchema.sstable(2, 71, true, cfs));
        tracker.addInitialSSTables(copyOf(readers));

        try (LifecycleTransaction txn = tracker.tryModify(readers.get(0), OperationType.COMPACTION))
        {
            if (invalidate)
            {
                cfs.invalidate(false);
            }
            else
            {
                tracker.dropSSTables();
                LogTransaction.waitForDeletions();
            }
            Assert.assertEquals(9, cfs.metric.totalDiskSpaceUsed.getCount());
            Assert.assertEquals(9, cfs.metric.liveDiskSpaceUsed.getCount());
            Assert.assertEquals(1, tracker.getView().sstables.size());
        }
        if (!invalidate)
        {
            Assert.assertEquals(1, tracker.getView().sstables.size());
            Assert.assertEquals(readers.get(0), Iterables.getFirst(tracker.getView().sstables, null));
            Assert.assertEquals(1, readers.get(0).selfRef().globalCount());
            Assert.assertFalse(readers.get(0).isMarkedCompacted());
            for (SSTableReader reader : readers.subList(1, 3))
            {
                Assert.assertEquals(0, reader.selfRef().globalCount());
                Assert.assertTrue(reader.isMarkedCompacted());
            }

            Assert.assertNull(tracker.dropSSTables(reader -> reader != readers.get(0), OperationType.UNKNOWN, null));

            Assert.assertEquals(1, tracker.getView().sstables.size());
            Assert.assertEquals(4, listener.received.size());
            Assert.assertEquals(tracker, listener.senders.get(0));
            Assert.assertTrue(listener.received.get(0) instanceof InitialSSTableAddedNotification);
            Assert.assertTrue(listener.received.get(1) instanceof SSTableDeletingNotification);
            Assert.assertTrue(listener.received.get(2) instanceof  SSTableDeletingNotification);
            Assert.assertTrue(listener.received.get(3) instanceof SSTableListChangedNotification);
            Assert.assertEquals(readers.get(1), ((SSTableDeletingNotification) listener.received.get(1)).deleting);
            Assert.assertEquals(readers.get(2), ((SSTableDeletingNotification)listener.received.get(2)).deleting);
            Assert.assertEquals(2, ((SSTableListChangedNotification) listener.received.get(3)).removed.size());
            Assert.assertEquals(0, ((SSTableListChangedNotification) listener.received.get(3)).added.size());
            Assert.assertEquals(9, cfs.metric.liveDiskSpaceUsed.getCount());
            readers.get(0).selfRef().release();
        }
        else
        {
            Assert.assertEquals(0, tracker.getView().sstables.size());
            Assert.assertEquals(0, cfs.metric.liveDiskSpaceUsed.getCount());
            for (SSTableReader reader : readers)
                Assert.assertTrue(reader.isMarkedCompacted());
        }
    }

    @Test
    public void testMemtableReplacement()
    {
        boolean backups = DatabaseDescriptor.isIncrementalBackupsEnabled();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
        ColumnFamilyStore cfs = MockSchema.newCFS(metadata -> metadata.caching(CachingParams.CACHE_KEYS));
        MockListener listener = new MockListener(false);
        Tracker tracker = cfs.getTracker();
        tracker.subscribe(listener);

        Memtable prev1 = tracker.switchMemtable(true, cfs.createMemtable(new AtomicReference<>(CommitLog.instance.getCurrentPosition())));
        OpOrder.Group write1 = cfs.keyspace.writeOrder.getCurrent();
        OpOrder.Barrier barrier1 = cfs.keyspace.writeOrder.newBarrier();
        prev1.switchOut(barrier1, new AtomicReference<>(CommitLog.instance.getCurrentPosition()));
        barrier1.issue();
        Memtable prev2 = tracker.switchMemtable(false, cfs.createMemtable(new AtomicReference<>(CommitLog.instance.getCurrentPosition())));
        OpOrder.Group write2 = cfs.keyspace.writeOrder.getCurrent();
        OpOrder.Barrier barrier2 = cfs.keyspace.writeOrder.newBarrier();
        prev2.switchOut(barrier2, new AtomicReference<>(CommitLog.instance.getCurrentPosition()));
        barrier2.issue();
        Memtable cur = tracker.getView().getCurrentMemtable();
        OpOrder.Group writecur = cfs.keyspace.writeOrder.getCurrent();
        Assert.assertEquals(prev1, tracker.getMemtableFor(write1, CommitLogPosition.NONE));
        Assert.assertEquals(prev2, tracker.getMemtableFor(write2, CommitLogPosition.NONE));
        Assert.assertEquals(cur, tracker.getMemtableFor(writecur, CommitLogPosition.NONE));
        Assert.assertEquals(2, listener.received.size());
        Assert.assertTrue(listener.received.get(0) instanceof MemtableRenewedNotification);
        Assert.assertTrue(listener.received.get(1) instanceof MemtableSwitchedNotification);
        listener.received.clear();

        tracker.markFlushing(prev2);
        Assert.assertEquals(1, tracker.getView().flushingMemtables.size());
        Assert.assertTrue(tracker.getView().flushingMemtables.contains(prev2));

        tracker.markFlushing(prev1);
        Assert.assertTrue(tracker.getView().flushingMemtables.contains(prev1));
        Assert.assertEquals(2, tracker.getView().flushingMemtables.size());

        tracker.replaceFlushed(prev1, Collections.emptyList());
        Assert.assertEquals(1, tracker.getView().flushingMemtables.size());
        Assert.assertTrue(tracker.getView().flushingMemtables.contains(prev2));

        SSTableReader reader = MockSchema.sstable(0, 10, false, cfs);
        tracker.replaceFlushed(prev2, singleton(reader));
        Assert.assertEquals(1, tracker.getView().sstables.size());
        Assert.assertEquals(2, listener.received.size());
        Assert.assertEquals(singleton(reader), ((SSTableAddedNotification) listener.received.get(0)).added);
        Assert.assertEquals(Optional.of(prev2), ((SSTableAddedNotification) listener.received.get(0)).memtable());
        Assert.assertEquals(prev2, ((MemtableDiscardedNotification) listener.received.get(1)).memtable);
        listener.received.clear();
        if (reader instanceof KeyCacheSupport<?>)
            Assert.assertTrue(((KeyCacheSupport<?>) reader).getKeyCache().isEnabled());
        Assert.assertEquals(10, cfs.metric.liveDiskSpaceUsed.getCount());

        // test invalidated CFS
        cfs = MockSchema.newCFS();
        tracker = cfs.getTracker();
        listener = new MockListener(false);
        tracker.subscribe(listener);
        prev1 = tracker.switchMemtable(false, cfs.createMemtable(new AtomicReference<>(CommitLog.instance.getCurrentPosition())));
        tracker.markFlushing(prev1);
        reader = MockSchema.sstable(0, 10, true, cfs);
        cfs.invalidate(false);
        tracker.replaceFlushed(prev1, singleton(reader));
        Assert.assertEquals(0, tracker.getView().sstables.size());
        Assert.assertEquals(0, tracker.getView().flushingMemtables.size());
        Assert.assertEquals(0, cfs.metric.liveDiskSpaceUsed.getCount());
        Assert.assertEquals(5, listener.received.size());
        Assert.assertEquals(prev1, ((MemtableSwitchedNotification) listener.received.get(0)).memtable);
        Assert.assertEquals(singleton(reader), ((SSTableAddedNotification) listener.received.get(1)).added);
        Assert.assertEquals(Optional.of(prev1), ((SSTableAddedNotification) listener.received.get(1)).memtable());
        Assert.assertEquals(prev1, ((MemtableDiscardedNotification) listener.received.get(2)).memtable);
        Assert.assertTrue(listener.received.get(3) instanceof SSTableDeletingNotification);
        Assert.assertEquals(1, ((SSTableListChangedNotification) listener.received.get(4)).removed.size());
        DatabaseDescriptor.setIncrementalBackupsEnabled(backups);
    }

    @Test
    public void testNotifications()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        SSTableReader r1 = MockSchema.sstable(0, cfs), r2 = MockSchema.sstable(1, cfs);
        Tracker tracker = Tracker.newDummyTracker();
        MockListener listener = new MockListener(false);
        tracker.subscribe(listener);
        tracker.notifyAdded(singleton(r1), false);
        Assert.assertEquals(singleton(r1), ((SSTableAddedNotification) listener.received.get(0)).added);
        listener.received.clear();
        tracker.notifyDeleting(r1);
        Assert.assertEquals(r1, ((SSTableDeletingNotification) listener.received.get(0)).deleting);
        listener.received.clear();
        Assert.assertNull(tracker.notifySSTablesChanged(singleton(r1), singleton(r2), OperationType.COMPACTION, null));
        Assert.assertEquals(singleton(r1), ((SSTableListChangedNotification) listener.received.get(0)).removed);
        Assert.assertEquals(singleton(r2), ((SSTableListChangedNotification) listener.received.get(0)).added);
        listener.received.clear();
        tracker.notifySSTableRepairedStatusChanged(singleton(r1));
        Assert.assertEquals(singleton(r1), ((SSTableRepairStatusChanged) listener.received.get(0)).sstables);
        listener.received.clear();
        Memtable memtable = MockSchema.memtable(cfs);
        tracker.notifyRenewed(memtable);
        Assert.assertEquals(memtable, ((MemtableRenewedNotification) listener.received.get(0)).renewed);
        listener.received.clear();
        tracker.notifySSTableMetadataChanged(r1, r1.getSSTableMetadata());
        Assert.assertEquals(((SSTableMetadataChanged)listener.received.get(0)).sstable, r1);
        Assert.assertEquals(r1.getSSTableMetadata(), ((SSTableMetadataChanged)listener.received.get(0)).oldMetadata);
        listener.received.clear();
        tracker.unsubscribe(listener);
        MockListener failListener = new MockListener(true);
        tracker.subscribe(failListener);
        tracker.subscribe(listener);
        Assert.assertNotNull(tracker.notifyAdded(singleton(r1), false, null, null));
        Assert.assertEquals(singleton(r1), ((SSTableAddedNotification) listener.received.get(0)).added);
        Assert.assertFalse(((SSTableAddedNotification) listener.received.get(0)).memtable().isPresent());
        listener.received.clear();
        Assert.assertNotNull(tracker.notifySSTablesChanged(singleton(r1), singleton(r2), OperationType.COMPACTION, null));
        Assert.assertEquals(singleton(r1), ((SSTableListChangedNotification) listener.received.get(0)).removed);
        Assert.assertEquals(singleton(r2), ((SSTableListChangedNotification) listener.received.get(0)).added);
        listener.received.clear();
    }

}
