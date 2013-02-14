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

import java.util.Map;
import java.util.Set;

import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;

public interface IAuthenticator
{
    static final String USERNAME_KEY = "username";
    static final String PASSWORD_KEY = "password";

    /**
     * Supported CREATE USER/ALTER USER options.
     * Currently only PASSWORD is available.
     */
    enum Option
    {
        PASSWORD
    }

    /**
     * Whether or not the authenticator requires explicit login.
     * If false will instantiate user with AuthenticatedUser.ANONYMOUS_USER.
     */
    boolean requireAuthentication();

    /**
     * Set of options supported by CREATE USER and ALTER USER queries.
     * Should never return null - always return an empty set instead.
     */
    Set<Option> supportedOptions();

    /**
     * Subset of supportedOptions that users are allowed to alter when performing ALTER USER [themselves].
     * Should never return null - always return an empty set instead.
     */
    Set<Option> alterableOptions();

    /**
     * Authenticates a user given a Map<String, String> of credentials.
     * Should never return null - always throw AuthenticationException instead.
     * Returning AuthenticatedUser.ANONYMOUS_USER is an option as well if authentication is not required.
     *
     * @throws AuthenticationException if credentials don't match any known user.
     */
    AuthenticatedUser authenticate(Map<String, String> credentials) throws AuthenticationException;

    /**
     * Called during execution of CREATE USER query (also may be called on startup, see seedSuperuserOptions method).
     * If authenticator is static then the body of the method should be left blank, but don't throw an exception.
     * options are guaranteed to be a subset of supportedOptions().
     *
     * @param username Username of the user to create.
     * @param options Options the user will be created with.
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    void create(String username, Map<Option, Object> options) throws RequestValidationException, RequestExecutionException;

    /**
     * Called during execution of ALTER USER query.
     * options are always guaranteed to be a subset of supportedOptions(). Furthermore, if the user performing the query
     * is not a superuser and is altering himself, then options are guaranteed to be a subset of alterableOptions().
     * Keep the body of the method blank if your implementation doesn't support any options.
     *
     * @param username Username of the user that will be altered.
     * @param options Options to alter.
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    void alter(String username, Map<Option, Object> options) throws RequestValidationException, RequestExecutionException;


    /**
     * Called during execution of DROP USER query.
     *
     * @param username Username of the user that will be dropped.
     * @throws RequestValidationException
     * @throws RequestExecutionException
     */
    void drop(String username) throws RequestValidationException, RequestExecutionException;

     /**
     * Set of resources that should be made inaccessible to users and only accessible internally.
     *
     * @return Keyspaces, column families that will be unmodifiable by users; other resources.
     */
    Set<? extends IResource> protectedResources();

    /**
     * Validates configuration of IAuthenticator implementation (if configurable).
     *
     * @throws ConfigurationException when there is a configuration error.
     */
    void validateConfiguration() throws ConfigurationException;

    /**
     * Setup is called once upon system startup to initialize the IAuthenticator.
     *
     * For example, use this method to create any required keyspaces/column families.
     */
    void setup();
}
