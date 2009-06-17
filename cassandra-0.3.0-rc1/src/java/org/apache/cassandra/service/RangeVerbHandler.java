package org.apache.cassandra.service;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.RangeCommand;
import org.apache.cassandra.db.RangeReply;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;

public class RangeVerbHandler implements IVerbHandler
{
    public void doVerb(Message message)
    {
        List<String> keys;
        try
        {
            RangeCommand command = RangeCommand.read(message);
            Table table = Table.open(command.table);
            keys = table.getKeyRange(command.startWith, command.stopAt, command.maxResults);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        Message response = new RangeReply(keys).getReply(message);
        MessagingService.getMessagingInstance().sendOneWay(response, message.getFrom());
    }
}
