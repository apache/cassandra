package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailureRecoveryVerbHandler implements IVerbHandler<FailureRecoveryRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(FailureRecoveryVerbHandler.class);

    private final EpaxosService service;

    public FailureRecoveryVerbHandler(EpaxosService service)
    {
        this.service = service;
    }

    @Override
    public void doVerb(MessageIn<FailureRecoveryRequest> message, int id)
    {
        logger.info("Received {} from {}", message.payload, message.from);
        FailureRecoveryRequest request = message.payload;
        service.startLocalFailureRecovery(request.token, request.cfId, request.epoch, request.scope);
    }
}
