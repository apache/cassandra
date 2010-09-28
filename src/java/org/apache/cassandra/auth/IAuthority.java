/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

package org.apache.cassandra.auth;

import java.util.EnumSet;
import java.util.List;

import org.apache.cassandra.config.ConfigurationException;

/**
 * Cassandra's resource hierarchy looks something like:
 * {{/cassandra/keyspaces/$ks_name/...}}
 * 
 * In table form:
 *  /cassandra/
 *    - no checked permissions
 *    - String
 *    * Separates Cassandra-internal resources from resources that might be provided by plugins.
 *  keyspaces/
 *    - READ, WRITE
 *    - String
 *    * The list of keyspaces: READ/WRITE for this resource mean the ability to view/modify the list of keyspaces.
 *  $ks_name/
 *    - READ, WRITE
 *    - String
 *    * An individual keyspace: READ/WRITE permissions apply to the entire namespace and control the ability to both
 *      view and manipulate column families, and to read and write the data contained within.
 * 
 * Over time Cassandra _may_ add additional authorize calls for resources higher or lower in the hierarchy and
 * IAuthority implementations should be able to handle these calls (although many will choose to ignore them
 * completely).
 * 
 * NB: {{/cassandra/}} will not be checked for permissions via a call to IAuthority.authorize, so IAuthority
 * implementations can only deny access when a user attempts to access an ancestor resource.
 */
public interface IAuthority
{
    /**
     * @param user An authenticated user from a previous call to IAuthenticator.authenticate.
     * @param resource A List of Objects containing Strings and byte[]s: represents a resource in the hierarchy
     * described in the Javadocs.  
     * @return An AccessLevel representing the permissions for the user and resource: should never return null.
     */
    public EnumSet<Permission> authorize(AuthenticatedUser user, List<Object> resource);

    public void validateConfiguration() throws ConfigurationException;
}
