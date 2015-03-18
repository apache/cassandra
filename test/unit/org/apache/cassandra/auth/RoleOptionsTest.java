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

import java.lang.reflect.Field;
import java.util.*;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RoleOptionsTest
{
    @Test
    public void validateValueTypes()
    {
        setupRoleManager(getRoleManager(IRoleManager.Option.values()));

        RoleOptions opts = new RoleOptions();
        opts.setOption(IRoleManager.Option.LOGIN, "test");
        assertInvalidOptions(opts, "Invalid value for property 'LOGIN'. It must be a boolean");

        opts = new RoleOptions();
        opts.setOption(IRoleManager.Option.PASSWORD, 99);
        assertInvalidOptions(opts, "Invalid value for property 'PASSWORD'. It must be a string");

        opts = new RoleOptions();
        opts.setOption(IRoleManager.Option.SUPERUSER, new HashSet<>());
        assertInvalidOptions(opts, "Invalid value for property 'SUPERUSER'. It must be a boolean");

        opts = new RoleOptions();
        opts.setOption(IRoleManager.Option.OPTIONS, false);
        assertInvalidOptions(opts, "Invalid value for property 'OPTIONS'. It must be a map");

        opts = new RoleOptions();
        opts.setOption(IRoleManager.Option.LOGIN, true);
        opts.setOption(IRoleManager.Option.SUPERUSER, false);
        opts.setOption(IRoleManager.Option.PASSWORD, "test");
        opts.setOption(IRoleManager.Option.OPTIONS, Collections.singletonMap("key", "value"));
        opts.validate();
    }

    @Test
    public void rejectUnsupportedOptions()
    {
        // Our hypothetical IRoleManager only supports the LOGIN option
        IRoleManager roleManager = getRoleManager(IRoleManager.Option.LOGIN);
        setupRoleManager(roleManager);
        RoleOptions opts = new RoleOptions();
        opts.setOption(IRoleManager.Option.PASSWORD, "test");
        assertInvalidOptions(opts, String.format("%s doesn't support PASSWORD", roleManager.getClass().getName()));
    }

    @Test
    public void rejectSettingSameOptionMultipleTimes()
    {
        RoleOptions opts = new RoleOptions();
        opts.setOption(IRoleManager.Option.LOGIN, true);
        try
        {
            opts.setOption(IRoleManager.Option.LOGIN, false);
        }
        catch (SyntaxException e)
        {
            assertEquals("Multiple definition for property 'LOGIN'", e.getMessage());
        }
    }

    @Test
    public void emptyByDefault()
    {
        RoleOptions opts = new RoleOptions();
        assertTrue(opts.isEmpty());
        assertFalse(opts.getLogin().isPresent());

        opts.setOption(IRoleManager.Option.LOGIN, true);
        assertFalse(opts.isEmpty());
        assertTrue(opts.getLogin().isPresent());
        assertTrue(opts.getLogin().get());
    }

    private void assertInvalidOptions(RoleOptions opts, String message)
    {
        try
        {
            opts.validate();
            fail("Expected error but didn't get one");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().equals(message));
        }
    }

    private void setupRoleManager(IRoleManager manager)
    {
        Field field = FBUtilities.getProtectedField(DatabaseDescriptor.class, "roleManager");
        try
        {
            field.set(null, manager);
        }
        catch (IllegalAccessException e)
        {
            fail("Error setting IRoleManager instance for test");
        }
    }

    private IRoleManager getRoleManager(final IRoleManager.Option...supportedOptions)
    {
        return new IRoleManager()
        {
            public Set<Option> supportedOptions()
            {
                return ImmutableSet.copyOf(supportedOptions);
            }

            public Set<Option> alterableOptions()
            {
                return null;
            }

            public void createRole(AuthenticatedUser performer,
                                   RoleResource role,
                                   RoleOptions options) throws RequestValidationException, RequestExecutionException
            {

            }

            public void dropRole(AuthenticatedUser performer,
                                 RoleResource role) throws RequestValidationException, RequestExecutionException
            {

            }

            public void alterRole(AuthenticatedUser performer,
                                  RoleResource role,
                                  RoleOptions options) throws RequestValidationException, RequestExecutionException
            {

            }

            public void grantRole(AuthenticatedUser performer,
                                  RoleResource role,
                                  RoleResource grantee) throws RequestValidationException, RequestExecutionException
            {

            }

            public void revokeRole(AuthenticatedUser performer,
                                   RoleResource role,
                                   RoleResource revokee) throws RequestValidationException, RequestExecutionException
            {

            }

            public Set<RoleResource> getRoles(RoleResource grantee,
                                              boolean includeInherited) throws RequestValidationException, RequestExecutionException
            {
                return null;
            }

            public Set<RoleResource> getAllRoles() throws RequestValidationException, RequestExecutionException
            {
                return null;
            }

            public boolean isSuper(RoleResource role)
            {
                return false;
            }

            public boolean canLogin(RoleResource role)
            {
                return false;
            }

            public Map<String, String> getCustomOptions(RoleResource role)
            {
                return Collections.EMPTY_MAP;
            }

            public boolean isExistingRole(RoleResource role)
            {
                return false;
            }

            public Set<? extends IResource> protectedResources()
            {
                return null;
            }

            public void validateConfiguration() throws ConfigurationException
            {

            }

            public void setup()
            {

            }
        };
    }
}
