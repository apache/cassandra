package org.apache.cassandra.service.epaxos.integration;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.apache.cassandra.concurrent.TracingAwareExecutorService;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Messenger
{
    private static final Logger logger = LoggerFactory.getLogger(Messenger.class);

    private final int VERSION = 0;
    private final AtomicInteger nextMsgNumber = new AtomicInteger(0);

    private final Map<InetAddress, Node> nodes = Maps.newConcurrentMap();
    private final Map<InetAddress, Integer> missedMessage = Maps.newConcurrentMap();
    private final Map<InetAddress, Map<MessagingService.Verb, IVerbHandler>> verbHandlers = Maps.newConcurrentMap();
    private final Map<Integer, IAsyncCallback> callbackMap = Maps.newConcurrentMap();

    private final TracingAwareExecutorService executorService;

    public Messenger()
    {
        this(null);
    }

    public Messenger(TracingAwareExecutorService executorService)
    {
        this.executorService = executorService;
    }

    public void registerNode(Node node)
    {
        nodes.put(node.getEndpoint(), node);

        Map<MessagingService.Verb, IVerbHandler> handlers = Maps.newEnumMap(MessagingService.Verb.class);
        handlers.put(MessagingService.Verb.EPAXOS_PREACCEPT, node.getPreacceptVerbHandler());
        handlers.put(MessagingService.Verb.EPAXOS_ACCEPT, node.getAcceptVerbHandler());
        handlers.put(MessagingService.Verb.EPAXOS_COMMIT, node.getCommitVerbHandler());
        handlers.put(MessagingService.Verb.EPAXOS_PREPARE, node.getPrepareVerbHandler());
        handlers.put(MessagingService.Verb.EPAXOS_TRYPREACCEPT, node.getTryPreacceptVerbHandler());

        verbHandlers.put(node.getEndpoint(), handlers);
        missedMessage.put(node.getEndpoint(), 0);
    }

    private int number(InetAddress address)
    {
        return nodes.get(address).number;
    }

    public List<InetAddress> getEndpoints(final InetAddress forNode)
    {
        List<InetAddress> endpoints = Lists.newArrayList(nodes.keySet());
        // we want returned endpoints to be sorted in order of down to up,
        // which will ensure that noresponse nodes will get messages before
        // the callback receives enough messages to move on to the next step
        // the node asking for endpoints should always be the last one returned
        Collections.sort(endpoints, new Comparator<InetAddress>()
        {
            @Override
            public int compare(InetAddress o1, InetAddress o2)
            {
                if (o1.equals(forNode))
                    return 1;
                if (o2.equals(forNode))
                    return -1;

                Node n1 = nodes.get(o1);
                Node n2 = nodes.get(o2);
                return n2.getState().ordinal() - n1.getState().ordinal();
            }
        });
        return endpoints;
    }

    /**
     * create a new message from the sender's fake ip address
     */
    private <T> MessageOut<T> wrapMessage(MessageOut<T> msg, InetAddress from)
    {
        return new MessageOut<T>(from, msg.verb, msg.payload, msg.serializer, msg.parameters);
    }

    private void submit(Runnable r)
    {
        if (executorService != null)
        {
            executorService.submit(r);
        }
        else
        {
            r.run();
        }
    }

    private void maybeResetNodes()
    {
        for (Map.Entry<InetAddress, Node> entry: nodes.entrySet())
        {
            InetAddress address = entry.getKey();
            Node node = entry.getValue();
            if (node.getState() == Node.State.UP)
            {
                missedMessage.put(address, 0);
            }
        }
    }

    private void maybeRecordMissedMessage(InetAddress address)
    {
        if (nodes.get(address).getState() != Node.State.UP)
        {
            int missed = missedMessage.get(address);
            missed++;
            missedMessage.put(address, missed);
        }
        else
        {
            missedMessage.put(address, 0);
        }
    }

    public int getMissedMessages(InetAddress address)
    {
        return missedMessage.get(address);
    }

    public Node getNode(InetAddress endpoint)
    {
        return nodes.get(endpoint);
    }

    public <T> void sendReply(MessageOut<T> msg, final int id, InetAddress from, InetAddress to)
    {
        assert msg != null;
        if (!from.equals(to))
        {
            if (nodes.get(from).getNetworkZone() != nodes.get(to).getNetworkZone())
            {
                logger.info("Aborting partitioned sendRR {} {} from {} ({}). To {} ({})",
                            msg.verb, msg.payload,
                            number(from), nodes.get(from).getNetworkZone(),
                            number(to), nodes.get(to).getNetworkZone() );
                return;
            }
            if (nodes.get(from).getState() != Node.State.UP)
            {
                logger.info("Aborting sendReply {} {} from {} to {}. Node: {}",
                            msg.verb, msg.payload,
                            number(from), number(to),
                            nodes.get(from).getState());
                return;
            }
            else if (nodes.get(to).getState() == Node.State.DOWN)
            {
                logger.info("Aborting sendReply {} {} from {} to {}. Node: {}",
                            msg.verb, msg.payload,
                            number(from), number(to),
                            Node.State.DOWN);
                return;
            }
        }

        final MessageIn<T> messageIn;
        DataOutputBuffer out = new DataOutputBuffer();

        // make new message to capture fake endpoint
        MessageOut<T> messageOut = wrapMessage(msg, from);
        try
        {
            messageOut.serialize(out, VERSION);
            messageIn = MessageIn.read(ByteStreams.newDataInput(out.getData()), VERSION, id);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }

        maybeResetNodes();
        logger.info("enqueuing {} {} to {}.", msg.verb, msg.payload, to);

        Node.queuedExecutor.setNextNodeSubmit(number(to));
        submit(new Runnable()
        {
            @Override
            public void run()
            {

                @SuppressWarnings("unchecked")
                IAsyncCallback<T> cb = callbackMap.get(id);
                callbackMap.remove(id);

                cb.response(messageIn);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public <T> int sendRR(MessageOut<T> msg, InetAddress from, final InetAddress to, IAsyncCallback cb)
    {
        final int msgId = nextMsgNumber.getAndIncrement();
        if (!from.equals(to))
        {
            if (nodes.get(from).getNetworkZone() != nodes.get(to).getNetworkZone())
            {
                logger.info("Aborting partitioned sendRR {} {} from {} ({}). To {} ({})",
                            msg.verb, msg.payload,
                            number(from), nodes.get(from).getNetworkZone(),
                            number(to), nodes.get(to).getNetworkZone() );
                return msgId;
            }
            if (nodes.get(from).getState() == Node.State.DOWN)
            {
                logger.info("Aborting sendRR {} {} from {} to {}. Node: {}",
                            msg.verb, msg.payload,
                            number(from),
                            number(to),
                            Node.State.DOWN);
                return msgId;
            }
            else if (nodes.get(to).getState() == Node.State.DOWN)
            {
                logger.info("Aborting sendRR {} {} from {} to {}. Node: {}",
                            msg.verb, msg.payload,
                            number(from),
                            number(to),
                            Node.State.DOWN);
                return msgId;
            }
        }

        MessagingService.instance().setCallbackForTests(msgId, new CallbackInfo(to,
                                                                                cb,
                                                                                MessagingService.callbackDeserializers.get(msg.verb),
                                                                                false));
        if (cb != null)
            callbackMap.put(msgId, cb);

        final MessageIn<T> messageIn;
        DataOutputBuffer out = new DataOutputBuffer();
        MessageOut<T> messageOut = wrapMessage(msg, from);
        try
        {
            messageOut.serialize(out, VERSION);
            messageIn = MessageIn.read(ByteStreams.newDataInput(out.getData()), VERSION, msgId);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }

        maybeRecordMissedMessage(to);

        logger.info("enqueuing {} {} to {}.", msg.verb, msg.payload, to);
        Node.queuedExecutor.setNextNodeSubmit(number(to));
        submit(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    verbHandlers.get(to).get(messageIn.verb).doVerb(messageIn, msgId);
                }
                catch (IOException e)
                {
                    throw new AssertionError();
                }
            }
        });

        return msgId;
    }

    @SuppressWarnings("unchecked")
    public <T> void sendOneWay(MessageOut<T> msg, InetAddress from, final InetAddress to)
    {
        final int msgId = nextMsgNumber.getAndIncrement();
        if (!from.equals(to))
        {
            if (nodes.get(from).getNetworkZone() != nodes.get(to).getNetworkZone())
            {
                logger.info("Aborting partitioned sendOneWay {} {} from {} ({}). To {} ({})",
                            msg.verb, msg.payload,
                            number(from), nodes.get(from).getNetworkZone(),
                            number(to), nodes.get(to).getNetworkZone() );
                return;
            }
            if (nodes.get(from).getState() == Node.State.DOWN)
            {
                logger.info("Aborting sendOneWay {} {} from {} to {}. Node: {}",
                            msg.verb, msg.payload,
                            number(from),
                            number(to),
                            Node.State.DOWN);
                return;
            }
            else if (nodes.get(to).getState() == Node.State.DOWN)
            {
                logger.info("Aborting sendOneWay {} {} from {} to {}. Node: {}",
                            msg.verb, msg.payload,
                            number(from),
                            number(to),
                            Node.State.DOWN);
                return;
            }
        }

        final MessageIn<T> messageIn;
        DataOutputBuffer out = new DataOutputBuffer();
        MessageOut<T> messageOut = wrapMessage(msg, from);
        try
        {
            messageOut.serialize(out, VERSION);
            messageIn = MessageIn.read(ByteStreams.newDataInput(out.getData()), VERSION, msgId);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }

        maybeResetNodes();

        logger.info("enqueuing {} {} to {}.", msg.verb, msg.payload, number(to));
        Node.queuedExecutor.setNextNodeSubmit(number(to));
        submit(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    verbHandlers.get(to).get(messageIn.verb).doVerb(messageIn, msgId);
                }
                catch (IOException e)
                {
                    throw new AssertionError();
                }
            }
        });
    }
}
