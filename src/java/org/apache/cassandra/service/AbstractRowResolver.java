package org.apache.cassandra.service;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

public abstract class AbstractRowResolver implements IResponseResolver<Row>
{
    protected static Logger logger = LoggerFactory.getLogger(AbstractRowResolver.class);

    private static final Message FAKE_MESSAGE = new Message(FBUtilities.getLocalAddress(), StorageService.Verb.INTERNAL_RESPONSE, ArrayUtils.EMPTY_BYTE_ARRAY);

    protected final String table;
    protected final ConcurrentMap<Message, ReadResponse> replies = new NonBlockingHashMap<Message, ReadResponse>();
    protected final DecoratedKey key;

    public AbstractRowResolver(ByteBuffer key, String table)
    {
        this.key = StorageService.getPartitioner().decorateKey(key);
        this.table = table;
    }

    public void preprocess(Message message)
    {
        byte[] body = message.getMessageBody();
        ByteArrayInputStream bufIn = new ByteArrayInputStream(body);
        try
        {
            ReadResponse result = ReadResponse.serializer().deserialize(new DataInputStream(bufIn));
            if (logger.isDebugEnabled())
                logger.debug("Preprocessed {} response", result.isDigestQuery() ? "digest" : "data");
            replies.put(message, result);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /** hack so local reads don't force de/serialization of an extra real Message */
    public void injectPreProcessed(ReadResponse result)
    {
        assert replies.get(FAKE_MESSAGE) == null; // should only be one local reply
        replies.put(FAKE_MESSAGE, result);
    }

    public Iterable<Message> getMessages()
    {
        return replies.keySet();
    }

    public int getMessageCount()
    {
        return replies.size();
    }
}
