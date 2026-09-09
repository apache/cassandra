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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The multi-data-directory boundary switch on the cursor path.
 *
 * {@code CompactionAwareWriter.maybeSwitchLocation} splits a compaction's output across data
 * directories at fixed token positions, finishing one sstable and starting the next in another
 * directory. {@code DefaultCompactionWriter.shouldSwitchWriterInCurrentLocation} always returns
 * false, so nothing else in this scenario can split the output: an output count above one is proof
 * the boundary path ran, and the count is exactly the number of directories the keys span.
 *
 * The switch decision itself is shared between the two compaction paths. What is not shared is when
 * it is consulted: the iterator path asks from {@code SSTableRewriter.append}, the cursor path from
 * {@code CursorCompactor.maybeSwitchWriter} at a partition boundary, handing it the cursor's own
 * reusable key. So this pins that the split lands on the same partitions on both paths, and that
 * every committed output holds only keys belonging to its directory.
 */
public class CursorDiskBoundaryDifferentialTest extends DifferentialCompactionTester
{
    private static final int DIRECTORIES = 3;
    private static final int PARTITIONS = 600;

    @Test
    public void diskBoundarySwitchMatchesIteratorOnBothPaths() throws Throwable
    {
        ColumnFamilyStore cfs = populated();
        ColumnFamilyStore bounded = withDirectories(cfs, boundaryPositions(cfs));

        assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), splitAcrossDirectories(bounded, true));
    }

    @Test
    public void committedOutputsRespectTheirDirectoryBoundaries() throws Throwable
    {
        ColumnFamilyStore cfs = populated();
        List<PartitionPosition> positions = boundaryPositions(cfs);
        ColumnFamilyStore bounded = withDirectories(cfs, positions);

        commitThroughFactory(cfs, true, splitAcrossDirectories(bounded, false));

        List<SSTableReader> outputs = new ArrayList<>(cfs.getLiveSSTables());
        assertEquals("one output per directory the keys span; DefaultCompactionWriter cannot split " +
                     "for any other reason, so a single output means the boundary path never ran",
                     DIRECTORIES, outputs.size());

        for (SSTableReader output : outputs)
        {
            int first = boundaryIndexOf(positions, output.getFirst());
            int last = boundaryIndexOf(positions, output.getLast());
            assertEquals("output " + output.descriptor.id + " spans a disk boundary: its first key " +
                         "belongs to directory " + first + " and its last to directory " + last,
                         first, last);

            // It must also physically sit in that directory, which is the half of the switch that
            // maybeSwitchLocation performs rather than decides.
            // Absolute on both sides: the configured data directory may be a relative path.
            String expected = new File(bounded.getDirectories().getLocationForDisk(
                bounded.getDiskBoundaries().directories.get(first)).path()).absolutePath();
            assertTrue("output " + output.descriptor.id + " belongs to directory " + first +
                       " but was written to " + output.descriptor.directory.absolutePath(),
                       output.descriptor.directory.absolutePath().startsWith(expected));
        }
    }

    /** The index of the first boundary at or above this key: the directory the key belongs to. */
    private static int boundaryIndexOf(List<PartitionPosition> positions, DecoratedKey key)
    {
        for (int i = 0; i < positions.size(); i++)
            if (key.compareTo(positions.get(i)) <= 0)
                return i;
        return positions.size();
    }

    /**
     * Two positions taken from the fixture's own keys, plus the partitioner's maximum, so the output
     * must split into exactly {@link #DIRECTORIES} pieces.
     */
    private static List<PartitionPosition> boundaryPositions(ColumnFamilyStore cfs)
    {
        List<DecoratedKey> keys = new ArrayList<>(PARTITIONS);
        for (int pk = 0; pk < PARTITIONS; pk++)
            keys.add(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(pk)));
        keys.sort(Comparator.naturalOrder());

        // maxKeyBound, as DiskBoundaryManager itself builds them, not the keys themselves.
        // CompactionAwareWriter.maybeSwitchLocation early-returns on `< 0` but advances on `> 0`, so a
        // key exactly equal to a boundary takes neither branch and switches to the directory it is
        // already in. A bound sits above every key of that token, so equality cannot arise.
        List<PartitionPosition> positions = new ArrayList<>(DIRECTORIES);
        positions.add(keys.get(PARTITIONS / 3).getToken().maxKeyBound());
        positions.add(keys.get(2 * PARTITIONS / 3).getToken().maxKeyBound());
        positions.add(cfs.getPartitioner().getMaximumTokenForSplitting().maxKeyBound());
        return positions;
    }

    /**
     * {@code Directories.dataDirectories} is a static final built at class load from the yaml, so a
     * test cannot give the real table more data directories after the fact. This builds a second view
     * of the same table with directories of its own and boundaries of its own.
     *
     * The boundaries are fabricated rather than derived from local ranges by
     * {@code DiskBoundaryManager}, so they fall on known keys of this fixture and the scenario
     * controls where the switch must happen instead of asserting against whatever ownership produced.
     */
    private static ColumnFamilyStore withDirectories(ColumnFamilyStore real, List<PartitionPosition> positions)
    {
        Directories.DataDirectory[] dirs = new Directories.DataDirectory[DIRECTORIES];
        for (int i = 0; i < DIRECTORIES; i++)
        {
            File dir = new File(DatabaseDescriptor.getAllDataFileLocations()[0],
                                "boundary-" + real.getTableName() + '-' + i);
            dir.tryCreateDirectories();
            dirs[i] = new Directories.DataDirectory(dir);
        }
        return new BoundedCFS(real, new Directories(real.metadata(), dirs), dirs, positions);
    }

    /** A view of one table carrying its own data directories and its own disk boundaries. */
    private static final class BoundedCFS extends ColumnFamilyStore
    {
        private final Directories.DataDirectory[] dirs;
        private final List<PartitionPosition> positions;

        BoundedCFS(ColumnFamilyStore real, Directories directories,
                   Directories.DataDirectory[] dirs, List<PartitionPosition> positions)
        {
            super(real.keyspace, real.getTableName(), Util.newSeqGen(), real.metadata.get(),
                  directories, false, false);
            this.dirs = dirs;
            this.positions = positions;
        }

        @Override
        public DiskBoundaries getDiskBoundaries()
        {
            // ColumnFamilyStore's constructor reaches this override before the fields below are
            // assigned, so the base answer stands until construction finishes.
            if (positions == null)
                return super.getDiskBoundaries();
            return new DiskBoundaries(this, dirs, positions, Epoch.EMPTY, 0);
        }
    }

    /**
     * The writer is built over the bounded view so it sees several directories; the transaction stays
     * on the real table, so the outputs are tracked and asserted there.
     * <p>
     * The parameter cannot be named keepOriginals: inside the subclass that name resolves to
     * CompactionTask's inherited field rather than to this parameter.
     */
    private static TaskFactory splitAcrossDirectories(ColumnFamilyStore bounded, boolean retainOriginals)
    {
        return (cfs, txn, gcBefore) -> new CompactionTask(cfs, txn, gcBefore, retainOriginals)
        {
            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore ignored,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                return new DefaultCompactionWriter(bounded, bounded.getDirectories(), transaction,
                                                   nonExpiredSSTables, retainOriginals, 0);
            }
        };
    }

    private ColumnFamilyStore populated() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "x".repeat(200);
        for (int round = 0; round < 2; round++)
        {
            for (int pk = 0; pk < PARTITIONS; pk++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, round, padding);
            flush();
        }
        assertTrue("the fixture needs inputs", cfs.getLiveSSTables().size() >= 2);
        return cfs;
    }
}
