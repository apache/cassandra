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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import org.apache.cassandra.audit.BinAuditLogger;

public class AuditLogViewerTest
{
    private Path path;

    @Before
    public void setUp() throws IOException
    {
        path = Files.createTempDirectory("foo");
    }

    @After
    public void tearDown() throws IOException
    {
        if (path.toFile().exists() && path.toFile().isDirectory())
        {
            //Deletes directory and all of it's contents
            FileUtils.deleteDirectory(path.toFile());
        }
    }

    @Test
    public void testDisplayRecord()
    {
        List<String> records = new ArrayList<>();
        records.add("Test foo bar 1");
        records.add("Test foo bar 2");

        try (ChronicleQueue queue = ChronicleQueueBuilder.single(path.toFile()).rollCycle(RollCycles.TEST_SECONDLY).build())
        {
            ExcerptAppender appender = queue.acquireAppender();

            //Write bunch of records
            records.forEach(s -> appender.writeDocument(new BinAuditLogger.Message(s)));

            //Read those written records
            List<String> actualRecords = new ArrayList<>();
            AuditLogViewer.dump(ImmutableList.of(path.toString()), RollCycles.TEST_SECONDLY.toString(), false, actualRecords::add);

            for (int i = 0; i < records.size(); i++)
            {
                Assert.assertTrue(actualRecords.get(i).contains(records.get(i)));
            }
        }
    }
}