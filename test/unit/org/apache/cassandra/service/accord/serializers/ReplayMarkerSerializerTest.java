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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.utils.RandomSource;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.service.accord.api.AccordTimeService;
import org.apache.cassandra.service.accord.journal.ReplayMarkers;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static accord.utils.Property.qt;
import static org.apache.cassandra.service.accord.journal.ReplayMarkers.safeStopMarker;
import static org.apache.cassandra.service.accord.journal.ReplayMarkers.startMarker;
import static org.apache.cassandra.service.accord.journal.ReplayMarkers.writeMarker;

public class ReplayMarkerSerializerTest
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        CassandraRelevantProperties.TEST_STORAGE_COMPATIBILITY_MODE.setEnum(StorageCompatibilityMode.NONE);
        SchemaLoader.prepareServer();
    }

    @Test
    public void replayMarkerSerializerTest()
    {
        if (new File(DatabaseDescriptor.getAccordJournalDirectory()).exists())
            ServerTestUtils.cleanupDirectory(DatabaseDescriptor.getAccordJournalDirectory());

        qt().forAll(RandomSource::nextLong).check(timestamp -> {
            long lastUniqueTimeStamp = AccordTimeService.nowMicros();
            // Start marker
            writeMarker(startMarker(), timestamp, lastUniqueTimeStamp);
            ReplayMarkers.ReplayMarkerMetadata replayMarkerMetadata = ReplayMarkers.readStartMarker();
            Assert.assertEquals(timestamp, Long.valueOf(replayMarkerMetadata.getSegmentId()));
            Assert.assertEquals(lastUniqueTimeStamp, replayMarkerMetadata.getLastUniqueTimeStamp());

            // Stop marker
            writeMarker(safeStopMarker(), timestamp, lastUniqueTimeStamp);
            replayMarkerMetadata = ReplayMarkers.readStopMarker();
            Assert.assertEquals(timestamp, Long.valueOf(replayMarkerMetadata.getSegmentId()));
            Assert.assertEquals(lastUniqueTimeStamp, replayMarkerMetadata.getLastUniqueTimeStamp());
        });
    }

    @Test
    public void nonCrcReplayMarkerSerializerTest()
    {
        if (new File(DatabaseDescriptor.getAccordJournalDirectory()).exists())
            ServerTestUtils.cleanupDirectory(DatabaseDescriptor.getAccordJournalDirectory());

        qt().forAll(RandomSource::nextLong).check(timestamp -> {
            // Start marker
            File file = new File(DatabaseDescriptor.getAccordJournalDirectory(), "started");

            try (FileOutputStreamPlus out = new FileOutputStreamPlus(file))
            {
                out.writeBytes(Long.toString(timestamp));
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }

            Assert.assertEquals(timestamp, Long.valueOf(ReplayMarkers.readStartMarker().getSegmentId()));

            // Stop marker
            file = new File(DatabaseDescriptor.getAccordJournalDirectory(), "stopped");

            try (FileOutputStreamPlus out = new FileOutputStreamPlus(file))
            {
                out.writeBytes(Long.toString(timestamp));
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
            Assert.assertEquals(timestamp, Long.valueOf(ReplayMarkers.readStopMarker().getSegmentId()));
        });
    }

    @Test
    public void prioritizeCRCFile()
    {
        if (new File(DatabaseDescriptor.getAccordJournalDirectory()).exists())
            ServerTestUtils.cleanupDirectory(DatabaseDescriptor.getAccordJournalDirectory());

        // Start marker
        File file = new File(DatabaseDescriptor.getAccordJournalDirectory(), "started");

        try (FileOutputStreamPlus out = new FileOutputStreamPlus(file))
        {
            out.writeBytes(Long.toString(250L));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }

        long lastUniqueTimeStamp = AccordTimeService.nowMicros();
        writeMarker(startMarker(), 250L, lastUniqueTimeStamp);
        ReplayMarkers.ReplayMarkerMetadata replayMarkerMetadata = ReplayMarkers.readStartMarker();
        Assert.assertEquals(250L, replayMarkerMetadata.getSegmentId());
        Assert.assertEquals(lastUniqueTimeStamp, replayMarkerMetadata.getLastUniqueTimeStamp());

        // Stop marker
        file = new File(DatabaseDescriptor.getAccordJournalDirectory(), "stopped");

        try (FileOutputStreamPlus out = new FileOutputStreamPlus(file))
        {
            out.writeBytes(Long.toString(250L));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }

        writeMarker(safeStopMarker(), 250L, lastUniqueTimeStamp);
        replayMarkerMetadata = ReplayMarkers.readStartMarker();
        Assert.assertEquals(250L, replayMarkerMetadata.getSegmentId());
        Assert.assertEquals(lastUniqueTimeStamp, replayMarkerMetadata.getLastUniqueTimeStamp());
    }
}