package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class PrepareVerbHandler extends AbstractEpochVerbHandler<PrepareRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareVerbHandler.class);

    public PrepareVerbHandler(EpaxosService service)
    {
        super(service);
    }

    @Override
    public void doEpochVerb(MessageIn<PrepareRequest> message, int id)
    {
        logger.debug("Prepare request received from {} for {}", message.from, message.payload.iid);
        ReadWriteLock lock = service.getInstanceLock(message.payload.iid);
        lock.writeLock().lock();
        try
        {
            Instance instance = service.loadInstance(message.payload.iid);

            // generally, we don't want to send placeholder instances back from prepare requests, since
            // they complicate the prepare callback logic. If the ballot is 0, however, it means that the
            // requesting node doesn't have a copy of this instance, so we want to send it back
            if (instance != null && instance.isPlaceholder() && message.payload.ballot > 0)
            {
                instance = null;
            }

            if (instance != null)
            {
                try
                {
                    if (!instance.getState().atLeast(Instance.State.COMMITTED))
                    {
                        instance.checkBallot(message.payload.ballot);
                        service.saveInstance(instance);
                    }
                }
                catch (BallotException e)
                {
                    // don't die if the message has an old ballot value, just don't
                    // update the instance. This instance will still be useful to the requester
                    logger.debug("Prepare request from {} for {} ballot failure. {} >= {}",
                                 message.from,
                                 message.payload.iid,
                                 instance.getBallot(),
                                 message.payload.ballot);
                }
            }
            else
            {
                logger.debug("Skipping ballot check for unknown or placeholder instances");
            }

            Token token = instance != null ? instance.getToken() : message.payload.getToken();
            UUID cfId = instance != null ? instance.getCfId() : message.payload.getCfId();
            Scope scope = instance != null ? instance.getScope() : message.payload.getScope();
            MessageOut<MessageEnvelope<Instance>> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                           new MessageEnvelope<>(token,
                                                                                                 cfId,
                                                                                                 service.getCurrentEpoch(token, cfId, scope),
                                                                                                 scope,
                                                                                                 instance),
                                                                           Instance.envelopeSerializer);
            service.sendReply(reply, id, message.from);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
}
