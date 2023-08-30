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

package org.apache.cassandra.repair.consistent;

import java.net.UnknownHostException;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Ignore;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

@Ignore
public abstract class AbstractConsistentSessionTest
{
    protected static final InetAddressAndPort COORDINATOR;
    protected static final InetAddressAndPort PARTICIPANT1;
    protected static final InetAddressAndPort PARTICIPANT2;
    protected static final InetAddressAndPort PARTICIPANT3;

    static
    {
        try
        {
            COORDINATOR = InetAddressAndPort.getByName("10.0.0.1");
            PARTICIPANT1 = InetAddressAndPort.getByName("10.0.0.1");
            PARTICIPANT2 = InetAddressAndPort.getByName("10.0.0.2");
            PARTICIPANT3 = InetAddressAndPort.getByName("10.0.0.3");
        }
        catch (UnknownHostException e)
        {

            throw new AssertionError(e);
        }

        DatabaseDescriptor.daemonInitialization();
    }

    protected static final Set<InetAddressAndPort> PARTICIPANTS = ImmutableSet.of(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3);

    protected static Token t(int v)
    {
        return DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(v));
    }

    protected static final Range<Token> RANGE1 = new Range<>(t(1), t(2));
    protected static final Range<Token> RANGE2 = new Range<>(t(2), t(3));
    protected static final Range<Token> RANGE3 = new Range<>(t(4), t(5));


    protected static TimeUUID registerSession(ColumnFamilyStore cfs)
    {
        TimeUUID sessionId = nextTimeUUID();

        ActiveRepairService.instance().registerParentRepairSession(sessionId,
                                                                   COORDINATOR,
                                                                   Lists.newArrayList(cfs),
                                                                   Sets.newHashSet(RANGE1, RANGE2, RANGE3),
                                                                   true,
                                                                   System.currentTimeMillis(),
                                                                   true,
                                                                   PreviewKind.NONE);
        return sessionId;
    }
}
