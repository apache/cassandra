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

package org.apache.cassandra.utils.progress.jmx;

import java.util.Optional;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;

import static org.junit.Assert.*;


public class LegacyJMXProgressSupportTest
{

    @Test
    public void testSessionSuccess()
    {
        int cmd = 321;
        String message = String.format("Repair session %s for range %s finished", UUID.randomUUID(),
                                       new Range<Token>(new Murmur3Partitioner.LongToken(3), new Murmur3Partitioner.LongToken(4)));
        Optional<int[]> result = LegacyJMXProgressSupport.getLegacyUserdata(String.format("repair:%d", cmd),
                                                            new ProgressEvent(ProgressEventType.PROGRESS, 2, 10, message));
        assertTrue(result.isPresent());
        assertArrayEquals(new int[]{ cmd, ActiveRepairService.Status.SESSION_SUCCESS.ordinal() }, result.get());
    }

    @Test
    public void testSessionFailed()
    {
        int cmd = 321;
        String message = String.format("Repair session %s for range %s failed with error %s", UUID.randomUUID(),
                                       new Range<Token>(new Murmur3Partitioner.LongToken(3), new Murmur3Partitioner.LongToken(4)).toString(),
                                       new RuntimeException("error"));
        Optional<int[]> result = LegacyJMXProgressSupport.getLegacyUserdata(String.format("repair:%d", cmd),
                                                                            new ProgressEvent(ProgressEventType.PROGRESS, 2, 10, message));
        assertTrue(result.isPresent());
        assertArrayEquals(new int[]{ cmd, ActiveRepairService.Status.SESSION_FAILED.ordinal() }, result.get());
    }

    @Test
    public void testStarted()
    {
        int cmd = 321;
        Optional<int[]> result = LegacyJMXProgressSupport.getLegacyUserdata(String.format("repair:%d", cmd),
                                                                            new ProgressEvent(ProgressEventType.START,
                                                                                              0, 100, "bla"));
        assertTrue(result.isPresent());
        assertArrayEquals(new int[]{ cmd, ActiveRepairService.Status.STARTED.ordinal() }, result.get());
    }

    @Test
    public void testFinished()
    {
        int cmd = 321;
        Optional<int[]> result = LegacyJMXProgressSupport.getLegacyUserdata(String.format("repair:%d", cmd),
                                                                         new ProgressEvent(ProgressEventType.COMPLETE,
                                                                                           2, 10, "bla"));
        assertTrue(result.isPresent());
        assertArrayEquals(new int[]{ cmd, ActiveRepairService.Status.FINISHED.ordinal() }, result.get());
    }

    /*
    States not mapped to the legacy notification
     */
    @Test
    public void testNone()
    {
        int cmd = 33;
        Optional<int[]> result = LegacyJMXProgressSupport.getLegacyUserdata(String.format("repair:%d", cmd),
                                                                         new ProgressEvent(ProgressEventType.ERROR, 2, 10, "bla"));
        assertFalse(result.isPresent());

        cmd = 33;
        result = LegacyJMXProgressSupport.getLegacyUserdata(String.format("repair:%d", cmd),
                                                                         new ProgressEvent(ProgressEventType.SUCCESS, 2, 10, "bla"));
        assertFalse(result.isPresent());

        cmd = 43;
        result = LegacyJMXProgressSupport.getLegacyUserdata(String.format("repair:%d", cmd),
                                                            new ProgressEvent(ProgressEventType.PROGRESS, 2, 10, "bla"));
        assertFalse(result.isPresent());

        cmd = 1;
        result = LegacyJMXProgressSupport.getLegacyUserdata(String.format("repair:%d", cmd),
                                                            new ProgressEvent(ProgressEventType.ABORT, 2, 10, "bla"));
        assertFalse(result.isPresent());

        cmd = 9;
        result = LegacyJMXProgressSupport.getLegacyUserdata(String.format("repair:%d", cmd),
                                                            new ProgressEvent(ProgressEventType.NOTIFICATION, 2, 10, "bla"));
        assertFalse(result.isPresent());
    }

}
