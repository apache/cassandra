package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.dht.Token;

import java.util.UUID;

public interface IEpochMessage
{
    public Token getToken();
    public UUID getCfId();
    public long getEpoch();
    public Scope getScope();
}
