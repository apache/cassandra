package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractEpochCallback<T extends IEpochMessage> implements IAsyncCallback<T>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptVerbHandler.class);

    protected final EpaxosService service;

    protected AbstractEpochCallback(EpaxosService service)
    {
        this.service = service;
    }

    @Override
    public final void response(MessageIn<T> message)
    {
        if (!service.tableExists(message.payload.getCfId()))
        {
            logger.debug("Ignoring message for unknown cfId {}", message.payload.getCfId());
            return;
        }

        Scope scope = message.payload.getScope();
        if (scope == Scope.LOCAL && !service.isInSameDC(message.from))
        {
            logger.warn("Received message with LOCAL scope from other dc");
            // ignore completely
            return;
        }
        TokenState tokenState = service.getTokenState(message.payload);
        tokenState.lock.readLock().lock();
        EpochDecision decision;
        logger.debug("Epoch response received from {} regarding {}", message.from, tokenState);
        try
        {
            TokenState.State s = tokenState.getState();
            if (s == TokenState.State.RECOVERY_REQUIRED)
            {
                service.startLocalFailureRecovery(tokenState.getToken(), tokenState.getCfId(), 0, scope);
                return;
            }
            else if (!s.isOkToParticipate())
            {
                logger.debug("TokenState {} cannot process {} message", tokenState, getClass().getSimpleName());
                return;
            }
            decision = tokenState.evaluateMessageEpoch(message.payload);
        }
        finally
        {
            tokenState.lock.readLock().unlock();
        }

        switch (decision.outcome)
        {
            case LOCAL_FAILURE:
                logger.debug("Unrecoverable local state", decision);
                service.startLocalFailureRecovery(decision.token, tokenState.getCfId(), decision.remoteEpoch, message.payload.getScope());
                break;
            case REMOTE_FAILURE:
                logger.debug("Unrecoverable remote state", decision);
                service.startRemoteFailureRecovery(message.from, decision.token, tokenState.getCfId(), decision.localEpoch, scope);
                break;
            case OK:
                epochResponse(message);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    public abstract void epochResponse(MessageIn<T> msg);

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
