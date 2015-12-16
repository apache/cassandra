package org.apache.cassandra.net;

import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MessagingServiceTest
{
    private final MessagingService messagingService = MessagingService.test();

    @Test
    public void testDroppedMessages()
    {
        MessagingService.Verb verb = MessagingService.Verb.READ;

        for (int i = 1; i <= 5000; i++)
            messagingService.incrementDroppedMessages(verb, i, i % 2 == 0);

        List<String> logs = messagingService.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        assertEquals("READ messages were dropped in last 5000 ms: 2500 for internal timeout and 2500 for cross node timeout. Mean internal dropped latency: 2730 ms and Mean cross-node dropped latency: 2731 ms", logs.get(0));
        assertEquals(5000, (int)messagingService.getDroppedMessages().get(verb.toString()));

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.incrementDroppedMessages(verb, i, i % 2 == 0);

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals("READ messages were dropped in last 5000 ms: 1250 for internal timeout and 1250 for cross node timeout. Mean internal dropped latency: 2277 ms and Mean cross-node dropped latency: 2278 ms", logs.get(0));
        assertEquals(7500, (int)messagingService.getDroppedMessages().get(verb.toString()));
    }

}
