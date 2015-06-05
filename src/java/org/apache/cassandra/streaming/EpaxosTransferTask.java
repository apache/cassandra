package org.apache.cassandra.streaming;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.epaxos.EpaxosService;
import org.apache.cassandra.service.epaxos.Scope;
import org.apache.cassandra.streaming.messages.EpaxosMessage;

import java.util.UUID;

public class EpaxosTransferTask extends EpaxosTask
{
    public EpaxosTransferTask(StreamSession session, UUID taskId, UUID cfId, Range<Token> range, Scope scope)
    {
        this(session, taskId, EpaxosService.getInstance(), cfId, range, scope);
    }

    public EpaxosTransferTask(StreamSession session, UUID taskId, EpaxosService state, UUID cfId, Range<Token> range, Scope scope)
    {
        super(session, taskId, state, cfId, range, scope);
    }

    public EpaxosSummary getSummary()
    {
        return new EpaxosSummary(taskId, cfId, range, scope);
    }

    public EpaxosMessage getMessage()
    {
        return new EpaxosMessage(taskId, cfId, range, scope);
    }
}
