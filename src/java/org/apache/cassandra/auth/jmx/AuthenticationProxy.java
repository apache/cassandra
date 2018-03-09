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

package org.apache.cassandra.auth.jmx;

import java.security.AccessController;
import java.security.PrivilegedAction;
import javax.management.remote.JMXAuthenticator;
import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * An alternative to the JAAS based implementation of JMXAuthenticator provided
 * by the JDK (JMXPluggableAuthenticator).
 *
 * Authentication is performed via delegation to a LoginModule. The JAAS login
 * config is specified by passing its identifier in a custom system property:
 *     cassandra.jmx.remote.login.config
 *
 * The location of the JAAS configuration file containing that config is
 * specified in the standard way, using the java.security.auth.login.config
 * system property.
 *
 * If authentication is successful then a Subject containing one or more
 * Principals is returned. This Subject may then be used during authorization
 * if a JMX authorization is enabled.
 */
public final class AuthenticationProxy implements JMXAuthenticator
{
    private static Logger logger = LoggerFactory.getLogger(AuthenticationProxy.class);

    // Identifier of JAAS configuration to be used for subject authentication
    private final String loginConfigName;

    /**
     * Creates an instance of <code>JMXPluggableAuthenticator</code>
     * and initializes it with a {@link LoginContext}.
     *
     * @param loginConfigName name of the specifig JAAS login configuration to
     *                        use when authenticating JMX connections
     * @throws SecurityException if the authentication mechanism cannot be
     *         initialized.
     */
    public AuthenticationProxy(String loginConfigName)
    {
        if (loginConfigName == null)
            throw new ConfigurationException("JAAS login configuration missing for JMX authenticator setup");

        this.loginConfigName = loginConfigName;
    }

    /**
     * Perform authentication of the client opening the {@code}MBeanServerConnection{@code}
     *
     * @param credentials optionally these credentials may be supplied by the JMX user.
     *                    Out of the box, the JDK's {@code}RMIServerImpl{@code} is capable
     *                    of supplying a two element String[], containing username and password.
     *                    If present, these credentials will be made available to configured
     *                    {@code}LoginModule{@code}s via {@code}JMXCallbackHandler{@code}.
     *
     * @return the authenticated subject containing any {@code}Principal{@code}s added by
     *the {@code}LoginModule{@code}s
     *
     * @throws SecurityException if the server cannot authenticate the user
     *         with the provided credentials.
     */
    public Subject authenticate(Object credentials)
    {
        // The credentials object is expected to be a string array holding the subject's
        // username & password. Those values are made accessible to LoginModules via the
        // JMXCallbackHandler.
        JMXCallbackHandler callbackHandler = new JMXCallbackHandler(credentials);
        try
        {
            LoginContext loginContext = new LoginContext(loginConfigName, callbackHandler);
            loginContext.login();
            final Subject subject = loginContext.getSubject();
            if (!subject.isReadOnly())
            {
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    subject.setReadOnly();
                    return null;
                });
            }

            return subject;
        }
        catch (LoginException e)
        {
            logger.trace("Authentication exception", e);
            throw new SecurityException("Authentication error", e);
        }
    }

    /**
     * This callback handler supplies the username and password (which was
     * optionally supplied by the JMX user) to the JAAS login module performing
     * the authentication, should it require them . No interactive user
     * prompting is necessary because the credentials are already available to
     * this class (via its enclosing class).
     */
    private static final class JMXCallbackHandler implements CallbackHandler
    {
        private char[] username;
        private char[] password;
        private JMXCallbackHandler(Object credentials)
        {
            // if username/password credentials were supplied, store them in
            // the relevant variables to make them accessible to LoginModules
            // via JMXCallbackHandler
            if (credentials instanceof String[])
            {
                String[] strings = (String[]) credentials;
                if (strings[0] != null)
                    username = strings[0].toCharArray();
                if (strings[1] != null)
                    password = strings[1].toCharArray();
            }
        }

        public void handle(Callback[] callbacks) throws UnsupportedCallbackException
        {
            for (int i = 0; i < callbacks.length; i++)
            {
                if (callbacks[i] instanceof NameCallback)
                    ((NameCallback)callbacks[i]).setName(username == null ? null : new String(username));
                else if (callbacks[i] instanceof PasswordCallback)
                    ((PasswordCallback)callbacks[i]).setPassword(password == null ? null : password);
                else
                    throw new UnsupportedCallbackException(callbacks[i], "Unrecognized Callback: " + callbacks[i].getClass().getName());
            }
        }
    }
}

