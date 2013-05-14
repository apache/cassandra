package org.apache.cassandra.service.paxos;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
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
            PaxosState state = SystemTable.loadPaxosState(toPrepare.key, toPrepare.update.metadata());
            if (toPrepare.isAfter(state.inProgressCommit))
            {
                Tracing.trace("promising ballot {}", toPrepare.ballot);
                SystemTable.savePaxosPromise(toPrepare);
                // return the pre-promise ballot so coordinator can pick the most recent in-progress value to resume
                return new PrepareResponse(true, state.inProgressCommit, state.mostRecentCommit);
            }
            else
            {
                Tracing.trace("promise rejected; {} is not sufficiently newer than {}", toPrepare, state.inProgressCommit);
                return new PrepareResponse(false, state.inProgressCommit, state.mostRecentCommit);
            }
        }
    }

    public static Boolean propose(Commit proposal)
    {
        synchronized (lockFor(proposal.key))
        {
            PaxosState state = SystemTable.loadPaxosState(proposal.key, proposal.update.metadata());
            if (proposal.hasBallot(state.inProgressCommit.ballot) || proposal.isAfter(state.inProgressCommit))
            {
                Tracing.trace("accepting proposal {}", proposal);
                SystemTable.savePaxosProposal(proposal);
                return true;
            }

            logger.debug("accept requested for {} but inProgress is now {}", proposal, state.inProgressCommit);
            return false;
        }
    }

    public static void commit(Commit proposal)
    {
        // There is no guarantee we will see commits in the right order, because messages
        // can get delayed, so a proposal can be older than our current most recent ballot/commit.
        // Committing it is however always safe due to column timestamps, so always do it. However,
        // if our current in-progress ballot is strictly greater than the proposal one, we shouldn't
        // erase the in-progress update.
        Tracing.trace("committing proposal {}", proposal);
        RowMutation rm = proposal.makeMutation();
        Table.open(rm.getTable()).apply(rm, true);

        synchronized (lockFor(proposal.key))
        {
            PaxosState state = SystemTable.loadPaxosState(proposal.key, proposal.update.metadata());
            SystemTable.savePaxosCommit(proposal, !state.inProgressCommit.isAfter(proposal));
        }
    }
}
