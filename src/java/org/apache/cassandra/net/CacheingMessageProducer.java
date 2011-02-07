package org.apache.cassandra.net;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CacheingMessageProducer implements MessageProducer
{
    private final MessageProducer prod;
    private final Map<Integer, Message> messages = new HashMap<Integer, Message>();
    private String messageId = null;
    
    public CacheingMessageProducer(MessageProducer prod)
    {
        this.prod = prod;    
    }

    public synchronized Message getMessage(int version) throws IOException
    {
        Message msg = messages.get(version);
        if (msg == null)
        {
            msg = prod.getMessage(version);
            if (messageId == null)
                messageId = msg.getMessageId();
            // it is important that both messages have the same id for callback processing.
            msg.setId(messageId);
            messages.put(version, msg);
        }
        return msg;
    }
}
