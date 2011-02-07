package org.apache.cassandra.net;

import java.io.IOException;

public interface MessageProducer
{
    public Message getMessage(int version) throws IOException;
}
