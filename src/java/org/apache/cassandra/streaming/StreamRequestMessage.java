package org.apache.cassandra.streaming;

import java.io.*;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
* This class encapsulates the message that needs to be sent to nodes
* that handoff data. The message contains information about ranges
* that need to be transferred and the target node.
*/
class StreamRequestMessage
{
   private static ICompactSerializer<StreamRequestMessage> serializer_;
   static
   {
       serializer_ = new StreamRequestMessageSerializer();
   }

   protected static ICompactSerializer<StreamRequestMessage> serializer()
   {
       return serializer_;
   }

   protected static Message makeStreamRequestMessage(StreamRequestMessage streamRequestMessage)
   {
       ByteArrayOutputStream bos = new ByteArrayOutputStream();
       DataOutputStream dos = new DataOutputStream(bos);
       try
       {
           StreamRequestMessage.serializer().serialize(streamRequestMessage, dos);
       }
       catch (IOException e)
       {
           throw new IOError(e);
       }
       return new Message(FBUtilities.getLocalAddress(), StageManager.STREAM_STAGE, StorageService.Verb.STREAM_REQUEST, bos.toByteArray() );
   }

   protected StreamRequestMetadata[] streamRequestMetadata_ = new StreamRequestMetadata[0];

   // TODO only actually ever need one BM, not an array
   StreamRequestMessage(StreamRequestMetadata... streamRequestMetadata)
   {
       assert streamRequestMetadata != null;
       streamRequestMetadata_ = streamRequestMetadata;
   }

    private static class StreamRequestMessageSerializer implements ICompactSerializer<StreamRequestMessage>
    {
        public void serialize(StreamRequestMessage streamRequestMessage, DataOutputStream dos) throws IOException
        {
            StreamRequestMetadata[] streamRequestMetadata = streamRequestMessage.streamRequestMetadata_;
            dos.writeInt(streamRequestMetadata.length);
            for (StreamRequestMetadata bsmd : streamRequestMetadata)
            {
                StreamRequestMetadata.serializer().serialize(bsmd, dos);
            }
        }

        public StreamRequestMessage deserialize(DataInputStream dis) throws IOException
        {
            int size = dis.readInt();
            StreamRequestMetadata[] streamRequestMetadata = new StreamRequestMetadata[size];
            for (int i = 0; i < size; ++i)
            {
                streamRequestMetadata[i] = StreamRequestMetadata.serializer().deserialize(dis);
            }
            return new StreamRequestMessage(streamRequestMetadata);
        }
    }
}
