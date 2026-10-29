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

package org.apache.cassandra.repair;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.ValidationOffsets;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;

public class MutationTrackingValidationCoordinatorTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static RepairJobDesc createDesc()
    {
        TimeUUID parentSession = nextTimeUUID();
        TimeUUID session = nextTimeUUID();
        return new RepairJobDesc(parentSession,
                                 session,
                                 "ks",
                                 "tbl",
                                 Collections.singletonList(new Range<>(new Murmur3Partitioner.LongToken(0),
                                                                       new Murmur3Partitioner.LongToken(100))));
    }

    @Test
    public void testEmptyParticipantsRejected()
    {
        RepairJobDesc desc = createDesc();
        assertThatThrownBy(() -> new MutationTrackingValidationCoordinator(SharedContext.Global.instance,
                                                                          desc,
                                                                          Collections.emptySet(),
                                                                          ValidationOffsets.EMPTY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Participant set cannot be empty");
    }

    @Test
    public void testNullParticipantsRejected()
    {
        RepairJobDesc desc = createDesc();
        assertThatThrownBy(() -> new MutationTrackingValidationCoordinator(SharedContext.Global.instance,
                                                                          desc,
                                                                          null,
                                                                          ValidationOffsets.EMPTY))
        .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testValidParticipantsAccepted() throws UnknownHostException
    {
        RepairJobDesc desc = createDesc();
        Set<InetAddressAndPort> participants = Collections.singleton(InetAddressAndPort.getByName("127.0.0.1"));
        MutationTrackingValidationCoordinator coordinator =
            new MutationTrackingValidationCoordinator(SharedContext.Global.instance,
                                                      desc,
                                                      participants,
                                                      ValidationOffsets.EMPTY);
        assertNotNull(coordinator);
    }
}
