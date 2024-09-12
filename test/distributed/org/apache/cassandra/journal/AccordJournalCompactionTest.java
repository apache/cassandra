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

package org.apache.cassandra.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.accord.AccordJournalTable;
import org.apache.cassandra.service.accord.AccordSegmentCompactor;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class AccordJournalCompactionTest
{
    private static final Set<Integer> SENTINEL_HOSTS = Collections.singleton(0);

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServer();
    }

    @Test
    public void segmentMergeTest() throws IOException
    {
        File directory = new File(Files.createTempDirectory(null));
        directory.deleteOnExit();

        Journal<TimeUUID, ByteBuffer> journal = journal(directory);
        AccordJournalTable<TimeUUID, ByteBuffer> journalTable = new AccordJournalTable<>(journal, journal.keySupport, journal.params.userVersion());
        journal.start();

        Map<TimeUUID, List<ByteBuffer >> uuids = new HashMap<>();

        int count = 0;
        for (int i = 0; i < 1024 * 5; i++)
        {
            TimeUUID uuid = nextTimeUUID();
            for (long j = 0; j < 5; j++)
            {
                ByteBuffer buf = ByteBuffer.allocate(1024);
                for (int k = 0; k < 1024; k++)
                    buf.put((byte) count);
                count++;
                buf.rewind();
                uuids.computeIfAbsent(uuid, (k) -> new ArrayList<>())
                     .add(buf);
                journal.asyncWrite(uuid, buf, SENTINEL_HOSTS);
            }
        }

        journal.closeCurrentSegmentForTesting();
        Runnable checkAll = () -> {
            for (Map.Entry<TimeUUID, List<ByteBuffer>> e : uuids.entrySet())
            {
                List<ByteBuffer> expected = e.getValue();

                List<ByteBuffer> actual = new ArrayList<>();
                journalTable.readAll(e.getKey(), (in, userVersion) -> actual.add(journal.valueSerializer.deserialize(e.getKey(), in, userVersion)));
                Assert.assertEquals(actual.size(), expected.size());
                for (int i = 0; i < actual.size(); i++)
                {
                    if (!actual.get(i).equals(expected.get(i)))
                    {
                        StringBuilder sb = new StringBuilder();
                        sb.append("Actual:\n");
                        for (ByteBuffer bb : actual)
                            sb.append(ByteBufferUtil.bytesToHex(bb)).append('\n');
                        sb.append("Expected:\n");
                        for (ByteBuffer bb : expected)
                            sb.append(ByteBufferUtil.bytesToHex(bb)).append('\n');
                        throw new AssertionError(sb.toString());
                    }
                }
            }
        };

        checkAll.run();
        journal.runCompactorForTesting();
        checkAll.run();
        journal.shutdown();
    }

    private static Journal<TimeUUID, ByteBuffer> journal(File directory)
    {
        return new Journal<>("TestJournal", directory,
                             new TestParams() {
                                 @Override
                                 public int segmentSize()
                                 {
                                     return 1024 * 1024;
                                 }

                                 @Override
                                 public boolean enableCompaction()
                                 {
                                     return false;
                                 }
                             },
                             TimeUUIDKeySupport.INSTANCE,
                             JournalTest.ByteBufferSerializer.INSTANCE,
                             new AccordSegmentCompactor<>());
    }
}
