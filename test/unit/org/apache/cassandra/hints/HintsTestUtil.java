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
package org.apache.cassandra.hints;

import java.util.UUID;

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.AbstractBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MockMessagingService;
import org.apache.cassandra.net.MockMessagingSpy;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Clock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.net.MockMessagingService.verb;
import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.apache.cassandra.net.Verb.HINT_RSP;

final class HintsTestUtil
{
    static void assertPartitionsEqual(AbstractBTreePartition expected, AbstractBTreePartition actual)
    {
        assertEquals(expected.partitionKey(), actual.partitionKey());
        assertEquals(expected.deletionInfo(), actual.deletionInfo());
        assertEquals(expected.columns(), actual.columns());
        assertTrue(Iterators.elementsEqual(expected.iterator(), actual.iterator()));
    }

    static void assertHintsEqual(Hint expected, Hint actual)
    {
        assertEquals(expected.mutation.getKeyspaceName(), actual.mutation.getKeyspaceName());
        assertEquals(expected.mutation.key(), actual.mutation.key());
        assertEquals(expected.mutation.getTableIds(), actual.mutation.getTableIds());
        for (PartitionUpdate partitionUpdate : expected.mutation.getPartitionUpdates())
            assertPartitionsEqual(partitionUpdate, actual.mutation.getPartitionUpdate(partitionUpdate.metadata()));
        assertEquals(expected.creationTime, actual.creationTime);
        assertEquals(expected.gcgs, actual.gcgs);
    }

    static MockMessagingSpy sendHintsAndResponses(TableMetadata metadata, int noOfHints, int noOfResponses)
    {
        // create spy for hint messages, but only create responses for noOfResponses hints
        Message<NoPayload> message = Message.internalResponse(HINT_RSP, NoPayload.noPayload);

        MockMessagingSpy spy;
        if (noOfResponses != -1)
        {
            spy = MockMessagingService.when(verb(HINT_REQ)).respondN(message, noOfResponses);
        }
        else
        {
            spy = MockMessagingService.when(verb(HINT_REQ)).respond(message);
        }

        // create and write noOfHints using service
        UUID hostId = StorageService.instance.getLocalHostUUID();
        for (int i = 0; i < noOfHints; i++)
        {
            long now = Clock.Global.currentTimeMillis();
            DecoratedKey dkey = dk(String.valueOf(i));
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, dkey).timestamp(now);
            builder.row("column0").add("val", "value0");
            Hint hint = Hint.create(builder.buildAsMutation(), now);
            HintsService.instance.write(hostId, hint);
        }
        return spy;
    }

    static class MockFailureDetector implements IFailureDetector
    {
        boolean isAlive = true;

        public boolean isAlive(InetAddressAndPort ep)
        {
            return isAlive;
        }

        public void interpret(InetAddressAndPort ep)
        {
            throw new UnsupportedOperationException();
        }

        public void report(InetAddressAndPort ep)
        {
            throw new UnsupportedOperationException();
        }

        public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener)
        {
            throw new UnsupportedOperationException();
        }

        public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener)
        {
            throw new UnsupportedOperationException();
        }

        public void remove(InetAddressAndPort ep)
        {
            throw new UnsupportedOperationException();
        }

        public void forceConviction(InetAddressAndPort ep)
        {
            throw new UnsupportedOperationException();
        }
    }
}
