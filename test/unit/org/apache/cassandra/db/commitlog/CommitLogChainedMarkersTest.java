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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

/**
 * Tests the commitlog to make sure we can replay it - explicitly for the case where we update the chained markers
 * in the commit log segment but do not flush the file to disk.
 */
@RunWith(BMUnitRunner.class)
public class CommitLogChainedMarkersTest
{
    private static final String KEYSPACE1 = "CommitLogTest";
    private static final String STANDARD1 = "CommitLogChainedMarkersTest";

    private static volatile String skipCachePath;
    private static volatile ByteBuffer mappedBuffer;
    private static volatile boolean mappedBufferCleaned;
    private static volatile boolean skipCacheAfterClean;
    private static volatile int skipCacheCalls;
    private static volatile long skipCacheOffset;
    private static volatile long skipCacheLength;

    @Test
    @BMRule(name = "force all calls to sync() to not flush to disk",
    targetClass = "CommitLogSegment",
    targetMethod = "sync(boolean)",
    action = "$flush = false")
    public void replayCommitLogWithoutFlushing() throws IOException
    {
        // this method is blend of CommitLogSegmentBackpressureTest & CommitLogReaderTest methods
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCommitLogSegmentSize(5);
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.periodic);
        DatabaseDescriptor.setCommitLogSyncPeriod(10000 * 1000);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, 0, AsciiType.instance, BytesType.instance));

        CompactionManager.instance.disableAutoCompaction();

        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);

        byte[] entropy = new byte[1024];
        new Random().nextBytes(entropy);
        final Mutation m = new RowUpdateBuilder(cfs1.metadata.get(), 0, "k")
                           .clustering("bytes")
                           .add("val", ByteBuffer.wrap(entropy))
                           .build();

        int samples = 10000;
        for (int i = 0; i < samples; i++)
            CommitLog.instance.add(m);

        CommitLog.instance.sync(false);

        ArrayList<File> toCheck = CommitLogReaderTest.getCommitLogs();
        CommitLogReader reader = new CommitLogReader();
        CommitLogReaderTest.TestCLRHandler testHandler = new CommitLogReaderTest.TestCLRHandler(cfs1.metadata.get());
        for (File f : toCheck)
            reader.readCommitLogSegment(testHandler, f, CommitLogReader.ALL_MUTATIONS, false);

        Assert.assertEquals(samples, testHandler.seenMutationCount());
    }

    @Test
    @BMRules(rules = {
        @BMRule(name = "Capture mapped buffer cleanup",
                targetClass = "org.apache.cassandra.io.util.FileUtils",
                targetMethod = "clean(java.nio.ByteBuffer)",
                targetLocation = "AT EXIT",
                action = "org.apache.cassandra.db.commitlog.CommitLogChainedMarkersTest.captureClean($1)"),
        @BMRule(name = "Capture commit log skip cache range",
                targetClass = "org.apache.cassandra.utils.NativeLibrary",
                targetMethod = "trySkipCache(int, long, int, String)",
                targetLocation = "AT ENTRY",
                action = "org.apache.cassandra.db.commitlog.CommitLogChainedMarkersTest.captureSkipCache($2, $3, $4)")
    })
    public void skipCacheAfterUnmappingClosedSegment()
    {
        DatabaseDescriptor.daemonInitialization();
        ParameterizedClass previousCompression = DatabaseDescriptor.getCommitLogCompression();
        EncryptionContext previousEncryptionContext = DatabaseDescriptor.getEncryptionContext();
        CommitLogSegment segment;
        try
        {
            DatabaseDescriptor.setCommitLogCompression(null);
            DatabaseDescriptor.setEncryptionContext(new EncryptionContext());
            CommitLog.instance.resetConfiguration();
            segment = CommitLogSegment.createSegment(CommitLog.instance, CommitLog.instance.segmentManager);
        }
        finally
        {
            DatabaseDescriptor.setCommitLogCompression(previousCompression);
            DatabaseDescriptor.setEncryptionContext(previousEncryptionContext);
            CommitLog.instance.resetConfiguration();
        }

        Assert.assertTrue(segment instanceof MemoryMappedSegment);
        boolean discarded = false;
        try
        {
            Mutation mutation = new Mutation("ks",
                                             DatabaseDescriptor.getPartitioner()
                                                               .decorateKey(ByteBufferUtil.bytes("key")),
                                             ImmutableMap.of(),
                                             0);
            Allocation allocation = segment.allocate(mutation, 128);
            ByteBuffer buffer = allocation.getBuffer();
            while (buffer.hasRemaining())
                buffer.put((byte) 0);
            allocation.markWritten();

            skipCachePath = segment.logFile.absolutePath();
            mappedBuffer = segment.buffer;
            mappedBufferCleaned = false;
            skipCacheAfterClean = false;
            skipCacheCalls = 0;
            skipCacheOffset = -1;
            skipCacheLength = -1;

            segment.sync(false);
            segment.sync(true);
            Assert.assertEquals(0, skipCacheCalls);

            segment.discard(true);
            discarded = true;

            Assert.assertTrue(mappedBufferCleaned);
            Assert.assertTrue(skipCacheAfterClean);
            Assert.assertEquals(1, skipCacheCalls);
            Assert.assertEquals(0, skipCacheOffset);
            Assert.assertEquals(0, skipCacheLength);
        }
        finally
        {
            skipCachePath = null;
            mappedBuffer = null;
            if (!discarded)
                segment.discard(true);
        }
    }

    public static void captureClean(ByteBuffer buffer)
    {
        if (buffer == mappedBuffer)
            mappedBufferCleaned = true;
    }

    public static void captureSkipCache(long offset, long length, String path)
    {
        if (path.equals(skipCachePath))
        {
            skipCacheAfterClean = mappedBufferCleaned;
            skipCacheCalls++;
            skipCacheOffset = offset;
            skipCacheLength = length;
        }
    }
}
