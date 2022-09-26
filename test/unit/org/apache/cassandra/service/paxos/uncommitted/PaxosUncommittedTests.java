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

package org.apache.cassandra.service.paxos.uncommitted;

import java.util.*;

import com.google.common.collect.Lists;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.apache.cassandra.service.paxos.Ballot.Flag.GLOBAL;
import static org.apache.cassandra.service.paxos.BallotGenerator.Global.nextBallot;

class PaxosUncommittedTests
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    static final IPartitioner PARTITIONER = new ByteOrderedPartitioner();
    static final Token MIN_TOKEN = PARTITIONER.getMinimumToken();
    static final Range<Token> FULL_RANGE = new Range<>(MIN_TOKEN, MIN_TOKEN);
    static final Collection<Range<Token>> ALL_RANGES = Collections.singleton(FULL_RANGE);
    static final ColumnFamilyStore PAXOS_CFS = Keyspace.open(SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.PAXOS);
    static final TableMetadata PAXOS_CFM = PAXOS_CFS.metadata.get();
    static final ColumnFamilyStore PAXOS_REPAIR_CFS = Keyspace.open(SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.PAXOS_REPAIR_HISTORY);
    static final TableMetadata PAXOS_REPAIR_CFM = PAXOS_REPAIR_CFS.metadata();

    static Ballot[] createBallots(int num)
    {
        Ballot[] ballots = new Ballot[num];
        for (int i=0; i<num; i++)
            ballots[i] = nextBallot(0, GLOBAL);

        return ballots;
    }

    static DecoratedKey dk(int v)
    {
        return DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(v));
    }

    static List<PaxosKeyState> kl(Iterator<PaxosKeyState> iter)
    {
        return Lists.newArrayList(iter);
    }

    static List<PaxosKeyState> kl(PaxosKeyState... states)
    {
        return Lists.newArrayList(states);
    }

    static Token tk(int v)
    {
        return dk(v).getToken();
    }

    static Range<Token> r(Token start, Token stop)
    {
        return new Range<>(start != null ? start : MIN_TOKEN, stop != null ? stop : MIN_TOKEN);
    }

    static Range<Token> r(int start, int stop)
    {
        return r(PARTITIONER.getToken(ByteBufferUtil.bytes(start)), PARTITIONER.getToken(ByteBufferUtil.bytes(stop)));
    }

}
