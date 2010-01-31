package org.apache.cassandra.streaming;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.streaming.StreamContextManager;
import org.apache.cassandra.streaming.StreamManager;

public class StreamFinishedVerbHandler implements IVerbHandler
{
    private static Logger logger = Logger.getLogger(StreamFinishedVerbHandler.class);

    public void doVerb(Message message)
    {
        byte[] body = message.getMessageBody();
        ByteArrayInputStream bufIn = new ByteArrayInputStream(body);

        try
        {
            StreamContextManager.StreamStatusMessage streamStatusMessage = StreamContextManager.StreamStatusMessage.serializer().deserialize(new DataInputStream(bufIn));
            StreamContextManager.StreamStatus streamStatus = streamStatusMessage.getStreamStatus();

            switch (streamStatus.getAction())
            {
                case DELETE:
                    StreamManager.instance(message.getFrom()).finish(streamStatus.getFile());
                    break;

                case STREAM:
                    if (logger.isDebugEnabled())
                        logger.debug("Need to re-stream file " + streamStatus.getFile());
                    StreamManager.instance(message.getFrom()).repeat();
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
