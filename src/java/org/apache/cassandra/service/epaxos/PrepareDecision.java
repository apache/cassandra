package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class PrepareDecision
{
    public final Instance.State state;
    public final Set<UUID> deps;
    public final boolean vetoed;
    public final Range<Token> splitRange;
    public final int ballot;
    public final List<TryPreacceptAttempt> tryPreacceptAttempts;
    public final boolean commitNoop;

    public PrepareDecision(Instance.State state, Set<UUID> deps, boolean vetoed, Range<Token> splitRange, int ballot)
    {
        this(state, deps, vetoed, splitRange, ballot, null, false);
    }

    public PrepareDecision(Instance.State state, Set<UUID> deps, boolean vetoed, Range<Token> splitRange, int ballot, List<TryPreacceptAttempt> tryPreacceptAttempts, boolean commitNoop)
    {
        this.state = state;
        this.deps = deps != null ? ImmutableSet.copyOf(deps) : null;
        this.vetoed = vetoed;
        this.splitRange = splitRange;
        this.ballot = ballot;
        this.tryPreacceptAttempts = tryPreacceptAttempts;
        this.commitNoop = commitNoop;
    }

    @Override
    public String toString()
    {
        return "PrepareDecision{" +
                "state=" + state +
                ", dependencies=" + (deps != null ? deps.size() : null) +
                ", tryPreacceptAttempts=" + (tryPreacceptAttempts != null ? tryPreacceptAttempts.size() : 0) +
                ", commitNoop=" + commitNoop +
                ", vetoed=" + vetoed +
                ", splitRange=" + splitRange +
                '}';
    }
}
