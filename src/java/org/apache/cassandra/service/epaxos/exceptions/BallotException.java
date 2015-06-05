package org.apache.cassandra.service.epaxos.exceptions;

import org.apache.cassandra.service.epaxos.Instance;

import java.util.UUID;

/**
 * Raised when a message is received for an instance with
 * a ballot value that is less than or equal to the local
 * instance's ballot.
 */
public class BallotException extends Exception
{
    public final int localBallot;
    public final int remoteBallot;
    public final UUID instanceId;

    public BallotException(Instance instance, int remoteBallot)
    {
        this(instance.getId(), instance.getBallot(), remoteBallot);
    }

    public BallotException(UUID instanceId, int localBallot, int remoteBallot)
    {
        super(String.format("Out of date ballot for %s. %s <= %s", instanceId, remoteBallot, localBallot));
        this.localBallot = localBallot;
        this.remoteBallot = remoteBallot;
        this.instanceId = instanceId;
    }
}
