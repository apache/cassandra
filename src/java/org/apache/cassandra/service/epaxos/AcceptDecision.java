package org.apache.cassandra.service.epaxos;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class AcceptDecision
{
    public final boolean acceptNeeded;
    public final Set<UUID> acceptDeps;
    public final boolean vetoed;
    public final Range<Token> splitRange;
    public final Map<InetAddress, Set<UUID>> missingInstances;

    public AcceptDecision(boolean acceptNeeded, Set<UUID> acceptDeps, boolean vetoed, Range<Token> splitRange, Map<InetAddress, Set<UUID>> missingInstances)
    {
        this.acceptNeeded = acceptNeeded;
        this.acceptDeps = acceptDeps;
        this.vetoed = vetoed;
        this.splitRange = splitRange;
        this.missingInstances = missingInstances;
    }

    @Override
    public String toString()
    {
        return "AcceptDecision{" +
               "acceptNeeded=" + acceptNeeded +
               ", acceptDeps=" + acceptDeps +
               ", vetoed=" + vetoed +
               ", splitRange=" + splitRange +
               ", missingInstances=" + missingInstances.size() +
               '}';
    }
}
