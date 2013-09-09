package org.apache.cassandra.service.paxos;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.tracing.Tracing;

public class PaxosState
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosState.class);

    private static final Object[] locks;
    static
    {
        locks = new Object[1024];
        for (int i = 0; i < locks.length; i++)
            locks[i] = new Object();
    }
    private static Object lockFor(ByteBuffer key)
    {
        return locks[(0x7FFFFFFF & key.hashCode()) % locks.length];
    }

    private final Commit inProgressCommit;
    private final Commit mostRecentCommit;

    public PaxosState(ByteBuffer key, CFMetaData metadata)
    {
        this(Commit.emptyCommit(key, metadata), Commit.emptyCommit(key, metadata));
    }

    public PaxosState(Commit inProgressCommit, Commit mostRecentCommit)
    {
        assert inProgressCommit.key == mostRecentCommit.key;
        assert inProgressCommit.update.metadata() == inProgressCommit.update.metadata();

        this.inProgressCommit = inProgressCommit;
        this.mostRecentCommit = mostRecentCommit;
    }

    public static PrepareResponse prepare(Commit toPrepare)
    {
        synchronized (lockFor(toPrepare.key))
        {
            PaxosState state = SystemKeyspace.loadPaxosState(toPrepare.key, toPrepare.update.metadata());
            if (toPrepare.isAfter(state.inProgressCommit))
            {
                Tracing.trace("Promising ballot {}", toPrepare.ballot);
                SystemKeyspace.savePaxosPromise(toPrepare);
                // return the pre-promise ballot so coordinator can pick the most recent in-progress value to resume
                return new PrepareResponse(true, state.inProgressCommit, state.mostRecentCommit);
            }
            else
            {
                Tracing.trace("Promise rejected; {} is not sufficiently newer than {}", toPrepare, state.inProgressCommit);
                return new PrepareResponse(false, state.inProgressCommit, state.mostRecentCommit);
            }
        }
    }

    public static Boolean propose(Commit proposal)
    {
        synchronized (lockFor(proposal.key))
        {
            PaxosState state = SystemKeyspace.loadPaxosState(proposal.key, proposal.update.metadata());
            if (proposal.hasBallot(state.inProgressCommit.ballot) || proposal.isAfter(state.inProgressCommit))
            {
                Tracing.trace("Accepting proposal {}", proposal);
                SystemKeyspace.savePaxosProposal(proposal);
                return true;
            }
            else
            {
                Tracing.trace("Rejecting proposal for {} because inProgress is now {}", proposal, state.inProgressCommit);
                return false;
            }
        }
    }

    public static void commit(Commit proposal)
    {
        // There is no guarantee we will see commits in the right order, because messages
        // can get delayed, so a proposal can be older than our current most recent ballot/commit.
        // Committing it is however always safe due to column timestamps, so always do it. However,
        // if our current in-progress ballot is strictly greater than the proposal one, we shouldn't
        // erase the in-progress update.
        Tracing.trace("Committing proposal {}", proposal);
        RowMutation rm = proposal.makeMutation();
        Keyspace.open(rm.getKeyspaceName()).apply(rm, true);

        synchronized (lockFor(proposal.key))
        {
            PaxosState state = SystemKeyspace.loadPaxosState(proposal.key, proposal.update.metadata());
            SystemKeyspace.savePaxosCommit(proposal, state.inProgressCommit.ballot);
        }
    }
}
