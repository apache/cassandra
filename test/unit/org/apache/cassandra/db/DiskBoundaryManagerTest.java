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

package org.apache.cassandra.db;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class DiskBoundaryManagerTest extends CQLTester
{
    private DiskBoundaryManager dbm;
    private MockCFS mock;
    private Directories dirs;
    private List<Directories.DataDirectory> datadirs;
    private List<File> tableDirs;
    private Path tmpDir;

    @Before
    public void setup() throws IOException
    {
        DisallowedDirectories.clearUnwritableUnsafe();
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.updateNormalTokens(BootStrapper.getRandomTokens(metadata, 10), FBUtilities.getBroadcastAddressAndPort());
        createTable("create table %s (id int primary key, x text)");
        tmpDir = Files.createTempDirectory("DiskBoundaryManagerTest");
        datadirs = Lists.newArrayList(new Directories.DataDirectory(new File(tmpDir, "1")),
                                      new Directories.DataDirectory(new File(tmpDir, "2")),
                                      new Directories.DataDirectory(new File(tmpDir, "3")));
        dirs = new Directories(getCurrentColumnFamilyStore().metadata(), datadirs);
        mock = new MockCFS(getCurrentColumnFamilyStore(), dirs);
        dbm = mock.diskBoundaryManager;
        tableDirs = datadirs.stream().map(ddir -> mock.getDirectories().getLocationForDisk(ddir)).collect(Collectors.toList());
    }

    @Test
    public void getBoundariesTest()
    {
        DiskBoundaries dbv = dbm.getDiskBoundaries(mock);
        Assert.assertEquals(3, dbv.positions.size());
        assertEquals(dbv.directories, dirs.getWriteableLocations());
    }

    @Test
    public void disallowedDirectoriesTest()
    {
        DiskBoundaries dbv = dbm.getDiskBoundaries(mock);
        Assert.assertEquals(3, dbv.positions.size());
        assertEquals(dbv.directories, dirs.getWriteableLocations());
        DisallowedDirectories.maybeMarkUnwritable(new File(tmpDir, "3"));
        dbv = dbm.getDiskBoundaries(mock);
        Assert.assertEquals(2, dbv.positions.size());
        Assert.assertEquals(Lists.newArrayList(new Directories.DataDirectory(new File(tmpDir, "1")),
                                               new Directories.DataDirectory(new File(tmpDir, "2"))),
                                 dbv.directories);
    }

    @Test
    public void updateTokensTest() throws UnknownHostException
    {
        DiskBoundaries dbv1 = dbm.getDiskBoundaries(mock);
        StorageService.instance.getTokenMetadata().updateNormalTokens(BootStrapper.getRandomTokens(StorageService.instance.getTokenMetadata(), 10), InetAddressAndPort.getByName("127.0.0.10"));
        DiskBoundaries dbv2 = dbm.getDiskBoundaries(mock);
        assertFalse(dbv1.equals(dbv2));
    }

    @Test
    public void alterKeyspaceTest() throws Throwable
    {
        //do not use mock to since it will not be invalidated after alter keyspace
        DiskBoundaryManager dbm = getCurrentColumnFamilyStore().diskBoundaryManager;
        DiskBoundaries dbv1 = dbm.getDiskBoundaries(mock);
        execute("alter keyspace "+keyspace()+" with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
        DiskBoundaries dbv2 = dbm.getDiskBoundaries(mock);
        assertNotSame(dbv1, dbv2);
        DiskBoundaries dbv3 = dbm.getDiskBoundaries(mock);
        assertSame(dbv2, dbv3);

    }

    @Test
    public void testGetDisksInBounds()
    {
        List<PartitionPosition> pps = new ArrayList<>();

        pps.add(pp(100));
        pps.add(pp(200));
        pps.add(pp(Long.MAX_VALUE)); // last position is always the max token

        DiskBoundaries diskBoundaries = new DiskBoundaries(mock, dirs.getWriteableLocations(), pps, 0, 0);

        Assert.assertEquals(Lists.newArrayList(datadirs.get(0)),                  diskBoundaries.getDisksInBounds(dk(10),  dk(50)));
        Assert.assertEquals(Lists.newArrayList(datadirs.get(2)),                  diskBoundaries.getDisksInBounds(dk(250), dk(500)));
        Assert.assertEquals(Lists.newArrayList(datadirs),                         diskBoundaries.getDisksInBounds(dk(0),   dk(250)));
        Assert.assertEquals(Lists.newArrayList(datadirs),                         diskBoundaries.getDisksInBounds(dk(0),   dk(250)));
        Assert.assertEquals(Lists.newArrayList(datadirs.get(1), datadirs.get(2)), diskBoundaries.getDisksInBounds(dk(150), dk(250)));
        Assert.assertEquals(Lists.newArrayList(datadirs),                         diskBoundaries.getDisksInBounds(null,       dk(250)));

        Assert.assertEquals(Lists.newArrayList(datadirs.get(0)),                  diskBoundaries.getDisksInBounds(dk(0),   dk(99)));
        Assert.assertEquals(Lists.newArrayList(datadirs.get(0)),                  diskBoundaries.getDisksInBounds(dk(0),   dk(100))); // pp(100) is maxKeyBound, so dk(100) < pp(100)
        Assert.assertEquals(Lists.newArrayList(datadirs.get(0), datadirs.get(1)), diskBoundaries.getDisksInBounds(dk(100), dk(200)));
        Assert.assertEquals(Lists.newArrayList(datadirs.get(1)),                  diskBoundaries.getDisksInBounds(dk(101), dk(101)));

    }

    @Test
    public void testGetDataDirectoriesForFiles()
    {
        int gen = 1;
        List<Murmur3Partitioner.LongToken> tokens = mock.getDiskBoundaries().positions.stream().map(t -> (Murmur3Partitioner.LongToken)t.getToken()).collect(Collectors.toList());
        IPartitioner partitioner = Murmur3Partitioner.instance;

        Murmur3Partitioner.LongToken sstableFirstDisk1 = (Murmur3Partitioner.LongToken) partitioner.midpoint(partitioner.getMinimumToken(), tokens.get(0));
        Murmur3Partitioner.LongToken sstableEndDisk1   = (Murmur3Partitioner.LongToken) partitioner.midpoint(sstableFirstDisk1,             tokens.get(0));
        Murmur3Partitioner.LongToken sstableEndDisk2   = (Murmur3Partitioner.LongToken) partitioner.midpoint(tokens.get(0),                 tokens.get(1));
        Murmur3Partitioner.LongToken sstableFirstDisk2 = (Murmur3Partitioner.LongToken) partitioner.midpoint(tokens.get(0),                 sstableEndDisk2);

        SSTableReader containedDisk1 = MockSchema.sstable(gen++, (long)sstableFirstDisk1.getTokenValue(), (long)sstableEndDisk1.getTokenValue(), 0, mock);
        SSTableReader startDisk1EndDisk2 = MockSchema.sstable(gen++, (long)sstableFirstDisk1.getTokenValue(), (long)sstableEndDisk2.getTokenValue(), 0, mock);
        SSTableReader containedDisk2 = MockSchema.sstable(gen++, (long)sstableFirstDisk2.getTokenValue(), (long)sstableEndDisk2.getTokenValue(), 0, mock);

        SSTableReader disk1Boundary = MockSchema.sstable(gen++, (long)sstableFirstDisk1.getTokenValue(), (long)tokens.get(0).getTokenValue(), 0, mock);
        SSTableReader disk2Full = MockSchema.sstable(gen++, (long)tokens.get(0).nextValidToken().getTokenValue(), (long)tokens.get(1).getTokenValue(), 0, mock);
        SSTableReader disk3Full = MockSchema.sstable(gen++, (long)tokens.get(1).nextValidToken().getTokenValue(), (long)partitioner.getMaximumToken().getTokenValue(), 0, mock);

        Assert.assertEquals(tableDirs, mock.getDirectoriesForFiles(ImmutableSet.of()));
        Assert.assertEquals(Lists.newArrayList(tableDirs.get(0)), mock.getDirectoriesForFiles(ImmutableSet.of(containedDisk1)));
        Assert.assertEquals(Lists.newArrayList(tableDirs.get(0), tableDirs.get(1)), mock.getDirectoriesForFiles(ImmutableSet.of(containedDisk1, startDisk1EndDisk2)));
        Assert.assertEquals(Lists.newArrayList(tableDirs.get(1)), mock.getDirectoriesForFiles(ImmutableSet.of(containedDisk2)));
        Assert.assertEquals(Lists.newArrayList(tableDirs.get(0), tableDirs.get(1)), mock.getDirectoriesForFiles(ImmutableSet.of(containedDisk1, containedDisk2)));

        Assert.assertEquals(Lists.newArrayList(tableDirs.get(0)), mock.getDirectoriesForFiles(ImmutableSet.of(disk1Boundary)));
        Assert.assertEquals(Lists.newArrayList(tableDirs.get(1)), mock.getDirectoriesForFiles(ImmutableSet.of(disk2Full)));

        Assert.assertEquals(tableDirs, mock.getDirectoriesForFiles(ImmutableSet.of(containedDisk1, disk3Full)));
    }

    private PartitionPosition pp(long t)
    {
        return t(t).maxKeyBound();
    }
    private Token t(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }
    private DecoratedKey dk(long t)
    {
        return new BufferDecoratedKey(t(t), ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    private static void assertEquals(List<Directories.DataDirectory> dir1, Directories.DataDirectory[] dir2)
    {
        if (dir1.size() != dir2.length)
            fail();
        for (int i = 0; i < dir2.length; i++)
        {
            if (!dir1.get(i).equals(dir2[i]))
                fail();
        }
    }

    // just to be able to override the data directories
    private static class MockCFS extends ColumnFamilyStore
    {
        MockCFS(ColumnFamilyStore cfs, Directories dirs)
        {
            super(cfs.keyspace, cfs.getTableName(), Util.newSeqGen(), cfs.metadata, dirs, false, false, true);
        }
    }
}
