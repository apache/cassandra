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
import java.nio.file.Files;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;

public class JournalTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServer();
    }

    @Test
    public void testSimpleReadWrite() throws IOException
    {
        File directory = new File(Files.createTempDirectory("JournalTest"));
        directory.deleteRecursiveOnExit();

        Journal<TimeUUID, Long> journal =
            new Journal<>("TestJournal", directory, TestParams.INSTANCE, TimeUUIDKeySupport.INSTANCE, LongSerializer.INSTANCE, SegmentCompactor.noop());

        journal.start();

        TimeUUID id1 = nextTimeUUID();
        TimeUUID id2 = nextTimeUUID();
        TimeUUID id3 = nextTimeUUID();
        TimeUUID id4 = nextTimeUUID();

        journal.blockingWrite(id1, 1L);
        journal.blockingWrite(id2, 2L);
        journal.blockingWrite(id3, 3L);
        journal.blockingWrite(id4, 4L);

        assertEquals(1L, (long) journal.readLast(id1));
        assertEquals(2L, (long) journal.readLast(id2));
        assertEquals(3L, (long) journal.readLast(id3));
        assertEquals(4L, (long) journal.readLast(id4));

        journal.shutdown();

        journal = new Journal<>("TestJournal", directory, TestParams.INSTANCE, TimeUUIDKeySupport.INSTANCE, LongSerializer.INSTANCE, SegmentCompactor.noop());
        journal.start();

        assertEquals(1L, (long) journal.readLast(id1));
        assertEquals(2L, (long) journal.readLast(id2));
        assertEquals(3L, (long) journal.readLast(id3));
        assertEquals(4L, (long) journal.readLast(id4));

        journal.shutdown();
    }

    static class LongSerializer implements ValueSerializer<TimeUUID, Long>
    {
        static final LongSerializer INSTANCE = new LongSerializer();

        public int serializedSize(TimeUUID key, Long value, int userVersion)
        {
            return Long.BYTES;
        }

        public void serialize(TimeUUID key, Long value, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeLong(value);
        }

        public Long deserialize(TimeUUID key, DataInputPlus in, int userVersion) throws IOException
        {
            return in.readLong();
        }
    }
}
