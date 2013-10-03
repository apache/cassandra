package org.apache.cassandra.net;

import java.net.InetAddress;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.service.StorageProxy;

public class WriteCallbackInfo extends CallbackInfo
{
    public final MessageOut sentMessage;
    private final ConsistencyLevel consistencyLevel;

    public WriteCallbackInfo(InetAddress target, IAsyncCallback callback, MessageOut message, IVersionedSerializer<?> serializer, ConsistencyLevel consistencyLevel)
    {
        super(target, callback, serializer);
        assert message != null;
        this.sentMessage = message;
        this.consistencyLevel = consistencyLevel;
    }

    public boolean shouldHint()
    {
        return consistencyLevel != ConsistencyLevel.ANY && StorageProxy.shouldHint(target);
    }
}
