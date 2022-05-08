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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.Assert;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction.ReaderState.Action;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction.ReaderState;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.AbstractTransactionalTest;
import org.apache.cassandra.utils.concurrent.Transactional.AbstractTransactional.State;

import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.size;
import static org.apache.cassandra.db.lifecycle.Helpers.idIn;
import static org.apache.cassandra.db.lifecycle.Helpers.orIn;
import static org.apache.cassandra.db.lifecycle.Helpers.select;

public class LifecycleTransactionTest extends AbstractTransactionalTest
{
    private boolean incrementalBackups;

    @BeforeClass
    public static void setUp()
    {
        MockSchema.cleanup();
    }

    @Before
    public void disableIncrementalBackup()
    {
        incrementalBackups = DatabaseDescriptor.isIncrementalBackupsEnabled();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
    }
    @After
    public void restoreIncrementalBackup()
    {
        DatabaseDescriptor.setIncrementalBackupsEnabled(incrementalBackups);
    }

    @Test
    public void testUpdates() // (including obsoletion)
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Tracker tracker = Tracker.newDummyTracker();
        SSTableReader[] readers = readersArray(0, 3, cfs);
        SSTableReader[] readers2 = readersArray(0, 4, cfs);
        SSTableReader[] readers3 = readersArray(0, 4, cfs);
        tracker.addInitialSSTables(copyOf(readers));
        LifecycleTransaction txn = tracker.tryModify(copyOf(readers), OperationType.UNKNOWN);

        txn.update(readers2[0], true);
        txn.obsolete(readers[1]);

        Assert.assertTrue(txn.isObsolete(readers[1]));
        Assert.assertFalse(txn.isObsolete(readers[0]));

        testBadUpdate(txn, readers2[0], true);  // same reader && instances
        testBadUpdate(txn, readers2[1], true);  // staged obsolete; cannot update
        testBadUpdate(txn, readers3[0], true);  // same reader, diff instances
        testBadUpdate(txn, readers2[2], false); // incorrectly declared original status
        testBadUpdate(txn, readers2[3], true); // incorrectly declared original status

        testBadObsolete(txn, readers[1]);  // staged obsolete; cannot obsolete again
        testBadObsolete(txn, readers2[0]);  // staged update; cannot obsolete

        txn.update(readers2[3], false);

        Assert.assertEquals(3, tracker.getView().compacting.size());
        txn.checkpoint();
        Assert.assertTrue(txn.isObsolete(readers[1]));
        Assert.assertFalse(txn.isObsolete(readers[0]));
        Assert.assertEquals(4, tracker.getView().compacting.size());
        Assert.assertEquals(3, tracker.getView().sstables.size());
        Assert.assertEquals(3, size(txn.current()));
        Assert.assertTrue(all(of(readers2[0], readers[2], readers2[3]), idIn(tracker.getView().sstablesMap)));
        Assert.assertTrue(all(txn.current(), idIn(tracker.getView().sstablesMap)));

        testBadObsolete(txn, readers[1]);  // logged obsolete; cannot obsolete again
        testBadObsolete(txn, readers2[2]);  // never seen instance; cannot obsolete
        testBadObsolete(txn, readers2[3]);  // non-original; cannot obsolete
        testBadUpdate(txn, readers3[1], true);  // logged obsolete; cannot update
        testBadUpdate(txn, readers2[0], true);  // same instance as logged update

        txn.update(readers3[0], true);  // same reader as logged update, different instance
        txn.checkpoint();

        Assert.assertEquals(4, tracker.getView().compacting.size());
        Assert.assertEquals(3, tracker.getView().sstables.size());
        Assert.assertEquals(3, size(txn.current()));
        Assert.assertTrue(all(of(readers3[0], readers[2], readers2[3]), idIn(tracker.getView().sstablesMap)));
        Assert.assertTrue(all(txn.current(), idIn(tracker.getView().sstablesMap)));

        testBadObsolete(txn, readers2[0]); // not current version of sstable

        txn.obsoleteOriginals();
        txn.checkpoint();
        Assert.assertEquals(1, tracker.getView().sstables.size());
        txn.obsoleteOriginals(); // should be no-op
        txn.checkpoint();
        Assert.assertEquals(1, tracker.getView().sstables.size());
        Assert.assertEquals(4, tracker.getView().compacting.size());
    }

    @Test
    public void testCancellation()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Tracker tracker = Tracker.newDummyTracker();
        List<SSTableReader> readers = readers(0, 3, cfs);
        tracker.addInitialSSTables(readers);
        LifecycleTransaction txn = tracker.tryModify(readers, OperationType.UNKNOWN);

        SSTableReader cancel = readers.get(0);
        SSTableReader update = readers(1, 2, cfs).get(0);
        SSTableReader fresh = readers(3, 4,cfs).get(0);
        SSTableReader notPresent = readers(4, 5, cfs).get(0);

        txn.cancel(cancel);
        txn.update(update, true);
        txn.update(fresh, false);

        testBadCancel(txn, cancel);
        testBadCancel(txn, update);
        testBadCancel(txn, fresh);
        testBadCancel(txn, notPresent);
        Assert.assertEquals(2, txn.originals().size());
        Assert.assertEquals(2, tracker.getView().compacting.size());
        Assert.assertTrue(all(readers.subList(1, 3), idIn(tracker.getView().compacting)));

        txn.checkpoint();

        testBadCancel(txn, cancel);
        testBadCancel(txn, update);
        testBadCancel(txn, fresh);
        testBadCancel(txn, notPresent);
        Assert.assertEquals(2, txn.originals().size());
        Assert.assertEquals(3, tracker.getView().compacting.size());
        Assert.assertEquals(3, size(txn.current()));
        Assert.assertTrue(all(concat(readers.subList(1, 3), of(fresh)), idIn(tracker.getView().compacting)));

        txn.cancel(readers.get(2));
        Assert.assertEquals(1, txn.originals().size());
        Assert.assertEquals(2, tracker.getView().compacting.size());
        Assert.assertEquals(2, size(txn.current()));
        Assert.assertTrue(all(of(readers.get(1), fresh), idIn(tracker.getView().compacting)));
    }

    @Test
    public void testSplit()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Tracker tracker = Tracker.newDummyTracker();
        List<SSTableReader> readers = readers(0, 4, cfs);
        tracker.addInitialSSTables(readers);
        LifecycleTransaction txn = tracker.tryModify(readers, OperationType.UNKNOWN);
        txn.cancel(readers.get(3));
        LifecycleTransaction txn2 = txn.split(readers.subList(0, 1));
        Assert.assertEquals(2, txn.originals().size());
        Assert.assertTrue(all(readers.subList(1, 3), in(txn.originals())));
        Assert.assertEquals(1, txn2.originals().size());
        Assert.assertTrue(all(readers.subList(0, 1), in(txn2.originals())));
        txn.update(readers(1, 2, cfs).get(0), true);
        boolean failed = false;
        try
        {
            txn.split(readers.subList(2, 3));
        }
        catch (Throwable t)
        {
            failed = true;
        }
        Assert.assertTrue(failed);
    }

    private static void testBadUpdate(LifecycleTransaction txn, SSTableReader update, boolean original)
    {
        boolean failed = false;
        try
        {
            txn.update(update, original);
        }
        catch (Throwable t)
        {
            failed = true;
        }
        Assert.assertTrue(failed);
    }

    private static void testBadObsolete(LifecycleTransaction txn, SSTableReader update)
    {
        boolean failed = false;
        try
        {
            txn.obsolete(update);
        }
        catch (Throwable t)
        {
            failed = true;
        }
        Assert.assertTrue(failed);
    }

    private static void testBadCancel(LifecycleTransaction txn, SSTableReader cancel)
    {
        boolean failed = false;
        try
        {
            txn.cancel(cancel);
        }
        catch (Throwable t)
        {
            failed = true;
        }
        Assert.assertTrue(failed);
    }

    protected TestableTransaction newTest()
    {
        LogTransaction.waitForDeletions();
        SSTableReader.resetTidying();
        return new TxnTest();
    }

    private static final class TxnTest extends TestableTransaction
    {
        final List<SSTableReader> originals;
        final List<SSTableReader> untouchedOriginals;
        final List<SSTableReader> loggedUpdate;
        final List<SSTableReader> loggedObsolete;
        final List<SSTableReader> stagedObsolete;
        final List<SSTableReader> loggedNew;
        final List<SSTableReader> stagedNew;
        final Tracker tracker;
        final LifecycleTransaction txn;

        private static Tracker tracker(ColumnFamilyStore cfs, List<SSTableReader> readers)
        {
            Tracker tracker = new Tracker(cfs, cfs.createMemtable(new AtomicReference<>(CommitLogPosition.NONE)), false);
            tracker.addInitialSSTables(readers);
            return tracker;
        }

        private TxnTest()
        {
            this(MockSchema.newCFS());
        }

        private TxnTest(ColumnFamilyStore cfs)
        {
            this(cfs, readers(0, 8, cfs));
        }

        private TxnTest(ColumnFamilyStore cfs, List<SSTableReader> readers)
        {
            this(tracker(cfs, readers), readers);
        }

        private TxnTest(Tracker tracker, List<SSTableReader> readers)
        {
            this(tracker, readers, tracker.tryModify(readers, OperationType.UNKNOWN));
        }

        private TxnTest(Tracker tracker, List<SSTableReader> readers, LifecycleTransaction txn)
        {
            super(txn);
            this.tracker = tracker;
            this.originals = readers;
            this.txn = txn;
            update(txn, loggedUpdate = readers(0, 2, tracker.cfstore), true);
            obsolete(txn, loggedObsolete = readers.subList(2, 4));
            update(txn, loggedNew = readers(8, 10, tracker.cfstore), false);
            txn.checkpoint();
            update(txn, stagedNew = readers(10, 12, tracker.cfstore), false);
            obsolete(txn, stagedObsolete = copyOf(concat(loggedUpdate, originals.subList(4, 6))));
            untouchedOriginals = originals.subList(6, 8);
        }

        private ReaderState state(SSTableReader reader, State state)
        {
            SSTableReader original = select(reader, originals);
            boolean isOriginal = original != null;

            switch (state)
            {
                case ABORTED:
                {
                    return new ReaderState(Action.NONE, Action.NONE, original, original, isOriginal);
                }

                case READY_TO_COMMIT:
                {
                    ReaderState prev = state(reader, State.IN_PROGRESS);
                    Action logged;
                    SSTableReader visible;
                    if (prev.staged == Action.NONE)
                    {
                        logged = prev.logged;
                        visible = prev.currentlyVisible;
                    }
                    else
                    {
                        logged = prev.staged;
                        visible = prev.nextVisible;
                    }
                    return new ReaderState(logged, Action.NONE, visible, visible, isOriginal);
                }

                case IN_PROGRESS:
                {
                    Action logged = Action.get(loggedUpdate.contains(reader) || loggedNew.contains(reader), loggedObsolete.contains(reader));
                    Action staged = Action.get(stagedNew.contains(reader), stagedObsolete.contains(reader));
                    SSTableReader currentlyVisible = ReaderState.visible(reader, in(loggedObsolete), loggedNew, loggedUpdate, originals);
                    SSTableReader nextVisible = ReaderState.visible(reader, orIn(stagedObsolete, loggedObsolete), stagedNew, loggedNew, loggedUpdate, originals);
                    return new ReaderState(logged, staged, currentlyVisible, nextVisible, isOriginal);
                }
            }
            throw new IllegalStateException();
        }

        private List<Pair<SSTableReader, ReaderState>> states(State state)
        {
            List<Pair<SSTableReader, ReaderState>> result = new ArrayList<>();
            for (SSTableReader reader : concat(originals, loggedNew, stagedNew))
                result.add(Pair.create(reader, state(reader, state)));
            return result;
        }

        protected void doAssert(State state)
        {
            for (Pair<SSTableReader, ReaderState> pair : states(state))
            {
                SSTableReader reader = pair.left;
                ReaderState readerState = pair.right;

                Assert.assertEquals(readerState, txn.state(reader));
                Assert.assertEquals(readerState.currentlyVisible, tracker.getView().sstablesMap.get(reader));
                if (readerState.currentlyVisible == null && readerState.nextVisible == null && !readerState.original)
                    Assert.assertTrue(reader.selfRef().globalCount() == 0);
            }
        }

        protected void assertInProgress() throws Exception
        {
            doAssert(State.IN_PROGRESS);
        }

        protected void assertPrepared() throws Exception
        {
            doAssert(State.READY_TO_COMMIT);
        }

        protected void assertAborted() throws Exception
        {
            doAssert(State.ABORTED);
            Assert.assertEquals(0, tracker.getView().compacting.size());
            Assert.assertEquals(8, tracker.getView().sstables.size());
            for (SSTableReader reader : concat(loggedNew, stagedNew))
                Assert.assertTrue(reader.selfRef().globalCount() == 0);
        }

        protected void assertCommitted() throws Exception
        {
            doAssert(State.READY_TO_COMMIT);
            Assert.assertEquals(0, tracker.getView().compacting.size());
            Assert.assertEquals(6, tracker.getView().sstables.size());
            for (SSTableReader reader : concat(loggedObsolete, stagedObsolete))
                Assert.assertTrue(reader.selfRef().globalCount() == 0);
        }

        @Override
        protected boolean commitCanThrow()
        {
            return true;
        }
    }

    private static SSTableReader[] readersArray(int lb, int ub, ColumnFamilyStore cfs)
    {
        return readers(lb, ub, cfs).toArray(new SSTableReader[0]);
    }

    private static List<SSTableReader> readers(int lb, int ub, ColumnFamilyStore cfs)
    {
        List<SSTableReader> readers = new ArrayList<>();
        for (int i = lb ; i < ub ; i++)
            readers.add(MockSchema.sstable(i, i, true, cfs));
        return copyOf(readers);
    }

    private static void update(LifecycleTransaction txn, Iterable<SSTableReader> readers, boolean originals)
    {
        for (SSTableReader reader : readers)
            txn.update(reader, originals);
    }

    private static void obsolete(LifecycleTransaction txn, Iterable<SSTableReader> readers)
    {
        for (SSTableReader reader : readers)
            txn.obsolete(reader);
    }
}
