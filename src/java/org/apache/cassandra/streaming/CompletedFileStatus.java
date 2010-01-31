package org.apache.cassandra.streaming;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class CompletedFileStatus
{
    private static ICompactSerializer<CompletedFileStatus> serializer_;

    public static enum StreamCompletionAction
    {
        DELETE,
        STREAM
    }

    static
    {
        serializer_ = new CompletedFileStatusSerializer();
    }

    public static ICompactSerializer<CompletedFileStatus> serializer()
    {
        return serializer_;
    }

    private String file_;
    private long expectedBytes_;
    private StreamCompletionAction action_;

    public CompletedFileStatus(String file, long expectedBytes)
    {
        file_ = file;
        expectedBytes_ = expectedBytes;
        action_ = StreamCompletionAction.DELETE;
    }

    public String getFile()
    {
        return file_;
    }

    public long getExpectedBytes()
    {
        return expectedBytes_;
    }

    public void setAction(StreamCompletionAction action)
    {
        action_ = action;
    }

    public StreamCompletionAction getAction()
    {
        return action_;
    }

    public Message makeStreamStatusMessage() throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        CompletedFileStatus.serializer().serialize(this, dos);
        return new Message(FBUtilities.getLocalAddress(), "", StorageService.Verb.STREAM_FINISHED, bos.toByteArray());
    }

    private static class CompletedFileStatusSerializer implements ICompactSerializer<CompletedFileStatus>
    {
        public void serialize(CompletedFileStatus streamStatus, DataOutputStream dos) throws IOException
        {
            dos.writeUTF(streamStatus.getFile());
            dos.writeLong(streamStatus.getExpectedBytes());
            dos.writeInt(streamStatus.getAction().ordinal());
        }

        public CompletedFileStatus deserialize(DataInputStream dis) throws IOException
        {
            String targetFile = dis.readUTF();
            long expectedBytes = dis.readLong();
            CompletedFileStatus streamStatus = new CompletedFileStatus(targetFile, expectedBytes);

            int ordinal = dis.readInt();
            if ( ordinal == StreamCompletionAction.DELETE.ordinal() )
            {
                streamStatus.setAction(StreamCompletionAction.DELETE);
            }
            else if ( ordinal == StreamCompletionAction.STREAM.ordinal() )
            {
                streamStatus.setAction(StreamCompletionAction.STREAM);
            }

            return streamStatus;
        }
    }
}
