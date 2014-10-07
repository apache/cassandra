package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.dht.Token;

public class EpochDecision
{
    public static enum Outcome { OK, LOCAL_FAILURE, REMOTE_FAILURE, RECOVERY }

    public final Outcome outcome;
    public final Token token;
    public final long localEpoch;
    public final long remoteEpoch;

    public EpochDecision(Outcome outcome, Token token, long localEpoch, long remoteEpoch)
    {
        this.outcome = outcome;
        this.token = token;
        this.localEpoch = localEpoch;
        this.remoteEpoch = remoteEpoch;
    }

    public static Outcome evaluate(long localEpoch, long remoteEpoch)
    {
        if (localEpoch > remoteEpoch + 1)
        {
            return Outcome.REMOTE_FAILURE;
        }
        else if (remoteEpoch > localEpoch + 1)
        {
            return Outcome.LOCAL_FAILURE;
        }
        else
        {
            return Outcome.OK;
        }
    }

    @Override
    public String toString()
    {
        return "EpochDecision{" +
                "outcome=" + outcome +
                ", token=" + token +
                ", localEpoch=" + localEpoch +
                ", remoteEpoch=" + remoteEpoch +
                '}';
    }
}
