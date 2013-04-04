package org.apache.cassandra.service;

import org.apache.cassandra.gms.EchoMessage;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EchoVerbHandler implements IVerbHandler<EchoMessage>
{
    private static final Logger logger = LoggerFactory.getLogger(EchoVerbHandler.class);

    public void doVerb(MessageIn<EchoMessage> message, int id)
    {
        assert message.payload != null;
        MessageOut<EchoMessage> echoMessage = new MessageOut<EchoMessage>(MessagingService.Verb.REQUEST_RESPONSE, new EchoMessage(), EchoMessage.serializer);
        logger.trace("Sending a EchoMessage reply {}", message.from);
        MessagingService.instance().sendReply(echoMessage, id, message.from);
    }
}
