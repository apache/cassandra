package org.apache.cassandra.service.paxos;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class PrepareVerbHandler implements IVerbHandler<Commit>
{
    public void doVerb(MessageIn<Commit> message, int id)
    {
        PrepareResponse response = PaxosState.prepare(message.payload);
        MessageOut<PrepareResponse> reply = new MessageOut<PrepareResponse>(MessagingService.Verb.REQUEST_RESPONSE, response, PrepareResponse.serializer);
        MessagingService.instance().sendReply(reply, id, message.from);
    }
}
