package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Token;

import java.net.InetAddress;
import java.util.UUID;

/**
 * Base instance used for managing token states
 */
public abstract class AbstractTokenInstance extends Instance
{
    protected final UUID cfId;
    protected final Token token;
    protected final Scope scope;

    protected AbstractTokenInstance(InetAddress leader, UUID cfId, Token token, Scope scope)
    {
        super(leader);
        this.cfId = cfId;
        this.token = token;
        this.scope = scope;
    }

    protected AbstractTokenInstance(UUID id, InetAddress leader, UUID cfId, Token token, Scope scope)
    {
        super(id, leader);
        this.cfId = cfId;
        this.token = token;
        this.scope = scope;
    }

    public AbstractTokenInstance(AbstractTokenInstance i)
    {
        super(i);
        this.cfId = i.cfId;
        this.token = i.token;
        this.scope = i.scope;
    }

    @Override
    public Token getToken()
    {
        return token;
    }

    @Override
    public UUID getCfId()
    {
        return cfId;
    }

    @Override
    public Scope getScope()
    {
        return scope;
    }

    @Override
    public ConsistencyLevel getConsistencyLevel()
    {
        return scope.cl;
    }
}
