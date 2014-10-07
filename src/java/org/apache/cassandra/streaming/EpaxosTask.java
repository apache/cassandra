package org.apache.cassandra.streaming;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.epaxos.EpaxosService;
import org.apache.cassandra.service.epaxos.Scope;

import java.util.UUID;

public class EpaxosTask
{
    public final StreamSession session;
    public final UUID taskId;
    public final EpaxosService state;
    public final UUID cfId;
    public final Range<Token> range;
    public final Scope scope;

    public EpaxosTask(StreamSession session, UUID taskId, EpaxosService state, UUID cfId, Range<Token> range, Scope scope)
    {
        this.session = session;
        this.taskId = taskId;
        this.state = state;
        this.cfId = cfId;
        this.range = range;
        this.scope = scope;
    }
}
