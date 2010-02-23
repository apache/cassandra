package org.apache.cassandra.auth;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;

public interface IAuthenticator
{
    public void login(String keyspace, AuthenticationRequest auth_request) throws AuthenticationException, AuthorizationException;
}
