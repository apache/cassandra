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

package org.apache.cassandra.db.compaction.differential;

import java.nio.ByteBuffer;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The cursor writer reuses one key for every partition it writes. A caller that holds the key or bounds
 * an sstable writer reports must not see them change when the next partition is written.
 */
public class CursorWriterLastKeyTest extends DifferentialCompactionTester
{
    @After
    public void tearDown()
    {
        restoreSelectedFormat();
    }

    @Test
    public void heldLastKeyAndBoundsAreStableBig() throws Throwable
    {
        assertHeldLastKeyAndBoundsAreStable("big");
    }

    @Test
    public void heldLastKeyAndBoundsAreStableBti() throws Throwable
    {
        assertHeldLastKeyAndBoundsAreStable("bti");
    }

    private static final class CurrentWriterExposed extends DefaultCompactionWriter
    {
        CurrentWriterExposed(ColumnFamilyStore cfs, Directories directories, ILifecycleTransaction txn, Set<SSTableReader> inputs)
        {
            super(cfs, directories, txn, inputs);
        }

        SSTableWriter current()
        {
            return sstableWriter.currentWriter();
        }
    }

    private void assertHeldLastKeyAndBoundsAreStable(String format) throws Throwable
    {
        selectSSTableFormat(format);
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < 20; pk++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, (long) round, "v" + round);
            flush();
        }
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull(txn);
        try (CompactionController controller = new CompactionController(cfs, inputs, gcBefore);
             AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(inputs);
             CurrentWriterExposed writer = new CurrentWriterExposed(cfs, cfs.getDirectories(), txn, inputs))
        {
            assertTrue(CursorCompactor.isSupported(scanners, controller));
            CursorCompactor compactor = new CursorCompactor(OperationType.COMPACTION, scanners.scanners, controller,
                                                            FBUtilities.nowInSeconds(), TimeUUID.Generator.nextTimeUUID());
            try
            {
                assertTrue(compactor.writeNextPartition(writer));
                DecoratedKey heldLast = writer.current().getLast();
                AbstractBounds<Token> heldBounds = writer.current().getBounds();
                ByteBuffer lastBytes = ByteBufferUtil.clone(heldLast.getKey());
                Token lastToken = cfs.getPartitioner().getToken(lastBytes);

                assertTrue(compactor.writeNextPartition(writer));

                assertEquals("the held last key changed when the next partition was written", lastBytes, heldLast.getKey());
                assertEquals("the held last key's token does not equal a token made from its bytes", lastToken, heldLast.getToken());
                assertEquals("the held bounds changed when the next partition was written", lastToken, heldBounds.right);
                while (compactor.writeNextPartition(writer)) { }
            }
            finally
            {
                compactor.close();
            }
        }
        finally
        {
            try
            {
                txn.abort();
            }
            catch (Throwable ignored)
            {
                // the writer's abort may already have aborted the transaction
            }
        }
    }
}
