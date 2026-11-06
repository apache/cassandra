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

package org.apache.cassandra.service.reads;

import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.reads.tracked.TrackedDataResponse;
import org.apache.cassandra.service.reads.tracked.TrackedRead;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

public class TrackedReadTest extends CQLTester
{
    private static final InetAddressAndPort REPLICA_1 = getAddress("127.0.0.1");
    private static final InetAddressAndPort REPLICA_2 = getAddress("127.0.0.2");

    private static class TestTrackedRead extends TrackedRead<EndpointsForToken, ReplicaPlan.ForTokenRead>
    {
        TestTrackedRead()
        {
            super(null, null, null, Dispatcher.RequestTime.forImmediateExecution());
        }

        @Override
        protected Verb verb()
        {
            return Verb.MT_PARTITION_READ_REQ;
        }
    }

    @Test
    public void testCoordinatorBehindIsIgnored()
    {
        TestTrackedRead read = new TestTrackedRead();
        read.onFailure(REPLICA_1, RequestFailure.forReason(RequestFailureReason.COORDINATOR_BEHIND));
        Assertions.assertFalse(read.future().isDone(), "Future must not be done after a COORDINATOR_BEHIND failure");
    }

    @Test
    public void testCoordinatorBehindDoesNotPreventOtherReplicaFailure()
    {
        TestTrackedRead read = new TestTrackedRead();
        read.onFailure(REPLICA_1, RequestFailure.forReason(RequestFailureReason.COORDINATOR_BEHIND));
        Assertions.assertFalse(read.future().isDone(), "Future must not be done after a COORDINATOR_BEHIND failure");
        read.onFailure(REPLICA_2, RequestFailure.forReason(RequestFailureReason.READ_TOO_MANY_TOMBSTONES));
        Assertions.assertTrue(read.future().isDone(), "Future must be done after a genuine failure");
        Throwable cause = read.future().cause();
        Assertions.assertTrue(cause instanceof TrackedRead.RequestFailure, "Cause should be a RequestFailure");
        Assertions.assertEquals(RequestFailureReason.READ_TOO_MANY_TOMBSTONES, ((TrackedRead.RequestFailure) cause).reasonByEndpoint().get(REPLICA_2));
        Assertions.assertFalse(((TrackedRead.RequestFailure) cause).reasonByEndpoint().containsKey(REPLICA_1));
    }

    @Test
    public void testIsCoordinatorBehind()
    {
        TrackedRead.RequestFailure failure = new TrackedRead.RequestFailure(REPLICA_1, RequestFailureReason.COORDINATOR_BEHIND);
        Assertions.assertTrue(failure.isCoordinatorBehind());
        TrackedRead.RequestFailure otherFailure = new TrackedRead.RequestFailure(REPLICA_1, RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
        Assertions.assertFalse(otherFailure.isCoordinatorBehind());
    }

    @Test
    public void testCoordinatorBehindFollowedBySuccessfulResponse()
    {
        TestTrackedRead read = new TestTrackedRead();
        read.onFailure(REPLICA_1, RequestFailure.forReason(RequestFailureReason.COORDINATOR_BEHIND));
        Assertions.assertFalse(read.future().isDone());
        ByteBuffer data = ByteBuffer.allocate(10);
        TrackedDataResponse response = new TrackedDataResponse(MessagingService.current_version, data);
        read.onResponse(Message.builder(Verb.MT_PARTITION_READ_RSP, response).from(REPLICA_2).build());
        Assertions.assertTrue(read.future().isSuccess());
        Assertions.assertSame(response, read.future().getNow());
    }

    @Test
    public void testUnexpectedFailureIsWrappedInRuntimeException() throws Exception
    {
        TestTrackedRead read = new TestTrackedRead();
        IllegalStateException original = new IllegalStateException("unexpected failure");
        Field futureField = TrackedRead.class.getDeclaredField("future");
        futureField.setAccessible(true);
        AsyncPromise<?> future = (AsyncPromise<?>) futureField.get(read);
        future.tryFailure(original);
        RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, read::awaitResults);
        Assertions.assertEquals("Unexpected error during tracked read", thrown.getMessage());
        Assertions.assertSame(original, thrown.getCause());
    }

    private static InetAddressAndPort getAddress(String host)
    {
        try
        {
            return InetAddressAndPort.getByName(host);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }
}

