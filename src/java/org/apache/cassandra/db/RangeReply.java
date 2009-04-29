package org.apache.cassandra.db;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Arrays;
import java.io.IOException;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;

public class RangeReply
{
    public final List<String> keys;

    public RangeReply(List<String> keys)
    {
        this.keys = Collections.unmodifiableList(keys);
    }

    public Message getReply(Message originalMessage)
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        for (String key : keys)
        {
            try
            {
                dob.writeUTF(key);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        byte[] data = Arrays.copyOf(dob.getData(), dob.getLength());
        return originalMessage.getReply(StorageService.getLocalStorageEndPoint(), data);
    }

    public static RangeReply read(byte[] body) throws IOException
    {
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(body, body.length);

        List<String> keys = new ArrayList<String>();
        while (bufIn.getPosition() < body.length)
        {
            keys.add(bufIn.readUTF());
        }

        return new RangeReply(keys);
    }
}
