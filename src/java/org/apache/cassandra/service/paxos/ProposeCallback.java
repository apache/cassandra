package org.apache.cassandra.service.paxos;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.MessageIn;

public class ProposeCallback extends AbstractPaxosCallback<Boolean>
{
    private static final Logger logger = LoggerFactory.getLogger(ProposeCallback.class);

    private final AtomicInteger successful = new AtomicInteger(0);

    public ProposeCallback(int targets)
    {
        super(targets);
    }

    public void response(MessageIn<Boolean> msg)
    {
        logger.debug("Propose response {} from {}", msg.payload, msg.from);

        if (msg.payload)
            successful.incrementAndGet();
        latch.countDown();
    }

    public int getSuccessful()
    {
        return successful.get();
    }
}
