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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;

/**
 * Provides a transitional IAuthenticator implementation for old-style (pre-1.2) authenticators.
 *
 * Comes with default implementation for the all of the new methods.
 * Subclass LegacyAuthenticator instead of implementing the old IAuthenticator and your old IAuthenticator
 * implementation should continue to work.
 */
public abstract class LegacyAuthenticator implements IAuthenticator
{
    /**
     * @return The user that a connection is initialized with, or 'null' if a user must call login().
     */
    public abstract AuthenticatedUser defaultUser();

    /**
     * @param credentials An implementation specific collection of identifying information.
     * @return A successfully authenticated user: should throw AuthenticationException rather than ever returning null.
     */
    public abstract AuthenticatedUser authenticate(Map<String, String> credentials) throws AuthenticationException;

    public abstract void validateConfiguration() throws ConfigurationException;

    @Override
    public boolean requireAuthentication()
    {
        return defaultUser() == null;
    }

    @Override
    public Set<Option> supportedOptions()
    {
        return Collections.emptySet();
    }

    @Override
    public Set<Option> alterableOptions()
    {
        return Collections.emptySet();
    }

    @Override
    public void create(String username, Map<Option, Object> options) throws RequestValidationException, RequestExecutionException
    {
    }

    @Override
    public void alter(String username, Map<Option, Object> options) throws RequestValidationException, RequestExecutionException
    {
    }

    @Override
    public void drop(String username) throws RequestValidationException, RequestExecutionException
    {
    }

    @Override
    public Set<IResource> protectedResources()
    {
        return Collections.emptySet();
    }

    @Override
    public void setup()
    {
    }
}
