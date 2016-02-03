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
package org.apache.cassandra.service.paxos;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Striped;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.UUIDGen;

public class PaxosState
{
    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentWriters() * 1024);

    private final Commit promised;
    private final Commit accepted;
    private final Commit mostRecentCommit;

    public PaxosState(DecoratedKey key, CFMetaData metadata)
    {
        this(Commit.emptyCommit(key, metadata), Commit.emptyCommit(key, metadata), Commit.emptyCommit(key, metadata));
    }

    public PaxosState(Commit promised, Commit accepted, Commit mostRecentCommit)
    {
        assert promised.update.partitionKey().equals(accepted.update.partitionKey()) && accepted.update.partitionKey().equals(mostRecentCommit.update.partitionKey());
        assert promised.update.metadata() == accepted.update.metadata() && accepted.update.metadata() == mostRecentCommit.update.metadata();

        this.promised = promised;
        this.accepted = accepted;
        this.mostRecentCommit = mostRecentCommit;
    }

    public static PrepareResponse prepare(Commit toPrepare)
    {
        long start = System.nanoTime();
        try
        {
            Lock lock = LOCKS.get(toPrepare.update.partitionKey());
            lock.lock();
            try
            {
                PaxosState state = SystemKeyspace.loadPaxosState(toPrepare.update.partitionKey(), toPrepare.update.metadata());
                if (toPrepare.isAfter(state.promised))
                {
                    Tracing.trace("Promising ballot {}", toPrepare.ballot);
                    SystemKeyspace.savePaxosPromise(toPrepare);
                    return new PrepareResponse(true, state.accepted, state.mostRecentCommit);
                }
                else
                {
                    Tracing.trace("Promise rejected; {} is not sufficiently newer than {}", toPrepare, state.promised);
                    // return the currently promised ballot (not the last accepted one) so the coordinator can make sure it uses newer ballot next time (#5667)
                    return new PrepareResponse(false, state.promised, state.mostRecentCommit);
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        finally
        {
            Keyspace.open(toPrepare.update.metadata().ksName).getColumnFamilyStore(toPrepare.update.metadata().cfId).metric.casPrepare.addNano(System.nanoTime() - start);
        }

    }

    public static Boolean propose(Commit proposal)
    {
        long start = System.nanoTime();
        try
        {
            Lock lock = LOCKS.get(proposal.update.partitionKey());
            lock.lock();
            try
            {
                PaxosState state = SystemKeyspace.loadPaxosState(proposal.update.partitionKey(), proposal.update.metadata());
                if (proposal.hasBallot(state.promised.ballot) || proposal.isAfter(state.promised))
                {
                    Tracing.trace("Accepting proposal {}", proposal);
                    SystemKeyspace.savePaxosProposal(proposal);
                    return true;
                }
                else
                {
                    Tracing.trace("Rejecting proposal for {} because inProgress is now {}", proposal, state.promised);
                    return false;
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        finally
        {
            Keyspace.open(proposal.update.metadata().ksName).getColumnFamilyStore(proposal.update.metadata().cfId).metric.casPropose.addNano(System.nanoTime() - start);
        }
    }

    public static void commit(Commit proposal)
    {
        long start = System.nanoTime();
        try
        {
            // There is no guarantee we will see commits in the right order, because messages
            // can get delayed, so a proposal can be older than our current most recent ballot/commit.
            // Committing it is however always safe due to column timestamps, so always do it. However,
            // if our current in-progress ballot is strictly greater than the proposal one, we shouldn't
            // erase the in-progress update.
            // The table may have been truncated since the proposal was initiated. In that case, we
            // don't want to perform the mutation and potentially resurrect truncated data
            if (UUIDGen.unixTimestamp(proposal.ballot) >= SystemKeyspace.getTruncatedAt(proposal.update.metadata().cfId))
            {
                Tracing.trace("Committing proposal {}", proposal);
                Mutation mutation = proposal.makeMutation();
                try
                {
                    Uninterruptibles.getUninterruptibly(Keyspace.open(mutation.getKeyspaceName()).apply(mutation, true));
                }
                catch (ExecutionException e)
                {
                    throw Throwables.propagate(e.getCause());
                }
            }
            else
            {
                Tracing.trace("Not committing proposal {} as ballot timestamp predates last truncation time", proposal);
            }
            // We don't need to lock, we're just blindly updating
            SystemKeyspace.savePaxosCommit(proposal);
        }
        finally
        {
            Keyspace.open(proposal.update.metadata().ksName).getColumnFamilyStore(proposal.update.metadata().cfId).metric.casCommit.addNano(System.nanoTime() - start);
        }
    }
}
