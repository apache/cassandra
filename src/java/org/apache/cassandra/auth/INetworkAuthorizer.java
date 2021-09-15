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

import org.apache.cassandra.exceptions.ConfigurationException;

public interface INetworkAuthorizer extends AuthCache.BulkLoader<RoleResource, DCPermissions>
{
    /**
     * Whether or not the authorizer will attempt authorization.
     * If false the authorizer will not be called for authorization of resources.
     */
    default boolean requireAuthorization()
    {
        return true;
    }

    /**
     * Setup is called once upon system startup to initialize the INetworkAuthorizer.
     *
     * For example, use this method to create any required keyspaces/column families.
     */
    void setup();

    /**
     * Returns the dc permissions associated with the given role
     */
    DCPermissions authorize(RoleResource role);

    void setRoleDatacenters(RoleResource role, DCPermissions permissions);

    /**
     * Called when a role is deleted, so any corresponding network permissions
     * data can also be cleaned up
     */
    void drop(RoleResource role);

    /**
     * Validates configuration of IAuthorizer implementation (if configurable).
     *
     * @throws ConfigurationException when there is a configuration error.
     */
    void validateConfiguration() throws ConfigurationException;
}
