package org.apache.cassandra.streaming;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.streaming.StreamOutManager;

public class StreamFinishedVerbHandler implements IVerbHandler
{
    private static Logger logger = Logger.getLogger(StreamFinishedVerbHandler.class);

    public void doVerb(Message message)
    {
        byte[] body = message.getMessageBody();
        ByteArrayInputStream bufIn = new ByteArrayInputStream(body);

        try
        {
            CompletedFileStatus streamStatus = CompletedFileStatus.serializer().deserialize(new DataInputStream(bufIn));

            switch (streamStatus.getAction())
            {
                case DELETE:
                    StreamOutManager.get(message.getFrom()).finishAndStartNext(streamStatus.getFile());
                    break;

                case STREAM:
                    if (logger.isDebugEnabled())
                        logger.debug("Need to re-stream file " + streamStatus.getFile());
                    StreamOutManager.get(message.getFrom()).startNext();
                    break;

                default:
                    break;
            }
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
    }
}
