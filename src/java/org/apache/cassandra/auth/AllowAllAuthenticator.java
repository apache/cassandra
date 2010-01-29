package org.apache.cassandra.auth;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;

public class AllowAllAuthenticator implements IAuthenticator
{
    @Override
    public void login(String keyspace, AuthenticationRequest authRequest) throws AuthenticationException, AuthorizationException
    {
        // do nothing, allow anything
    }
}
