package org.apache.cassandra.auth;

import org.apache.cassandra.service.AuthenticationException;
import org.apache.cassandra.service.AuthenticationRequest;
import org.apache.cassandra.service.AuthorizationException;

public interface IAuthenticator
{
    public void login(String keyspace, AuthenticationRequest auth_request) throws AuthenticationException, AuthorizationException;
}
