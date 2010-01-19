package org.apache.cassandra.auth;

import org.apache.cassandra.service.AuthenticationException;
import org.apache.cassandra.service.AuthenticationRequest;
import org.apache.cassandra.service.AuthorizationException;

public class AllowAllAuthenticator implements IAuthenticator
{
    @Override
    public void login(String keyspace, AuthenticationRequest authRequest) throws AuthenticationException, AuthorizationException
    {
        // do nothing, allow anything
    }
}
