/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.auth;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.service.StorageService;

/**
 * LoginModule which authenticates a user towards the Cassandra database using
 * the internal authentication mechanism.
 */
public class CassandraLoginModule implements LoginModule
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraLoginModule.class);

    // initial state
    private Subject subject;
    private CallbackHandler callbackHandler;

    // the authentication status
    private boolean succeeded = false;
    private boolean commitSucceeded = false;

    // username and password
    private String username;
    private char[] password;

    private CassandraPrincipal principal;

    /**
     * Initialize this {@code}LoginModule{@code}.
     *
     * @param subject the {@code}Subject{@code} to be authenticated. <p>
     * @param callbackHandler a {@code}CallbackHandler{@code} for communicating
     *        with the end user (prompting for user names and passwords, for example)
     * @param sharedState shared {@code}LoginModule{@code} state. This param is unused.
     * @param options options specified in the login {@code}Configuration{@code} for this particular
     *        {@code}LoginModule{@code}. This param is unused
     */
    @Override
    public void initialize(Subject subject,
                           CallbackHandler callbackHandler,
                           Map<java.lang.String, ?> sharedState,
                           Map<java.lang.String, ?> options)
    {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
    }

    /**
     * Authenticate the user, obtaining credentials from the CallbackHandler
     * supplied in {@code}initialize{@code}. As long as the configured
     * {@code}IAuthenticator{@code} supports the optional
     * {@code}legacyAuthenticate{@code} method, it can be used here.
     *
     * @return true in all cases since this {@code}LoginModule{@code}
     *         should not be ignored.
     * @exception FailedLoginException if the authentication fails.
     * @exception LoginException if this {@code}LoginModule{@code} is unable to
     * perform the authentication.
     */
    @Override
    public boolean login() throws LoginException
    {
        // prompt for a user name and password
        if (callbackHandler == null)
        {
            logger.info("No CallbackHandler available for authentication");
            throw new LoginException("Authentication failed");
        }

        NameCallback nc = new NameCallback("username: ");
        PasswordCallback pc = new PasswordCallback("password: ", false);
        try
        {
            callbackHandler.handle(new Callback[]{nc, pc});
            username = nc.getName();
            char[] tmpPassword = pc.getPassword();
            if (tmpPassword == null)
                tmpPassword = new char[0];
            password = new char[tmpPassword.length];
            System.arraycopy(tmpPassword, 0, password, 0, tmpPassword.length);
            pc.clearPassword();
        }
        catch (IOException | UnsupportedCallbackException e)
        {
            logger.info("Unexpected exception processing authentication callbacks", e);
            throw new LoginException("Authentication failed");
        }

        // verify the credentials
        try
        {
            authenticate();
        }
        catch (AuthenticationException e)
        {
            // authentication failed -- clean up
            succeeded = false;
            cleanUpInternalState();
            throw new FailedLoginException(e.getMessage());
        }

        succeeded = true;
        return true;
    }

    private void authenticate()
    {
        if (!StorageService.instance.isAuthSetupComplete())
            throw new AuthenticationException("Cannot login as server authentication setup is not yet completed");

        IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
        Map<String, String> credentials = new HashMap<>();
        credentials.put(PasswordAuthenticator.USERNAME_KEY, username);
        credentials.put(PasswordAuthenticator.PASSWORD_KEY, String.valueOf(password));
        AuthenticatedUser user = authenticator.legacyAuthenticate(credentials);
        // Only actual users should be allowed to authenticate for JMX
        if (user.isAnonymous() || user.isSystem())
            throw new AuthenticationException(String.format("Invalid user %s", user.getName()));

        // The LOGIN privilege is required to authenticate - c.f. ClientState::login
        if (!DatabaseDescriptor.getRoleManager().canLogin(user.getPrimaryRole()))
            throw new AuthenticationException(user.getName() + " is not permitted to log in");
    }

    /**
     * This method is called if the LoginContext's overall authentication succeeded
     * (the relevant REQUIRED, REQUISITE, SUFFICIENT and OPTIONAL LoginModules
     * succeeded).
     *
     * If this LoginModule's own authentication attempt succeeded (checked by
     * retrieving the private state saved by the {@code}login{@code} method),
     * then this method associates a {@code}CassandraPrincipal{@code}
     * with the {@code}Subject{@code}.
     * If this LoginModule's own authentication attempted failed, then this
     * method removes any state that was originally saved.
     *
     * @return true if this LoginModule's own login and commit attempts succeeded, false otherwise.
     * @exception LoginException if the commit fails.
     */
    @Override
    public boolean commit() throws LoginException
    {
        if (!succeeded)
        {
            return false;
        }
        else
        {
            // add a Principal (authenticated identity)
            // to the Subject
            principal = new CassandraPrincipal(username);
            if (!subject.getPrincipals().contains(principal))
                subject.getPrincipals().add(principal);

            cleanUpInternalState();
            commitSucceeded = true;
            return true;
        }
    }

    /**
     * This method is called if the LoginContext's  overall authentication failed.
     * (the relevant REQUIRED, REQUISITE, SUFFICIENT and OPTIONAL LoginModules
     * did not succeed).
     *
     * If this LoginModule's own authentication attempt succeeded (checked by
     * retrieving the private state saved by the {@code}login{@code} and
     * {@code}commit{@code} methods), then this method cleans up any state that
     * was originally saved.
     *
     * @return false if this LoginModule's own login and/or commit attempts failed, true otherwise.
     * @throws LoginException if the abort fails.
     */
    @Override
    public boolean abort() throws LoginException
    {
        if (!succeeded)
        {
            return false;
        }
        else if (!commitSucceeded)
        {
            // login succeeded but overall authentication failed
            succeeded = false;
            cleanUpInternalState();
            principal = null;
        }
        else
        {
            // overall authentication succeeded and commit succeeded,
            // but someone else's commit failed
            logout();
        }
        return true;
    }

    /**
     * Logout the user.
     *
     * This method removes the principal that was added by the
     * {@code}commit{@code} method.
     *
     * @return true in all cases since this {@code}LoginModule{@code}
     *         should not be ignored.
     * @throws LoginException if the logout fails.
     */
    @Override
    public boolean logout() throws LoginException
    {
        subject.getPrincipals().remove(principal);
        succeeded = false;
        cleanUpInternalState();
        principal = null;
        return true;
    }

    private void cleanUpInternalState()
    {
        username = null;
        if (password != null)
        {
            for (int i = 0; i < password.length; i++)
                password[i] = ' ';
            password = null;
        }
    }
}
