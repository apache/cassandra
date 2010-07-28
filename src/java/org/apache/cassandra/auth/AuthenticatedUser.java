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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.avro.AccessLevel;

/**
 * An authenticated user and her groups.
 */
public class AuthenticatedUser
{
    public final String username;
    public final Set<String> groups;
    public final boolean isSuper;

    public AuthenticatedUser(String username, boolean isSuper)
    {
        this.username = username;
        this.groups = Collections.emptySet();
        this.isSuper = isSuper;
    }

    public AuthenticatedUser(String username, Set<String> groups, boolean isSuper)
    {
        this.username = username;
        this.groups = Collections.unmodifiableSet(groups);
        this.isSuper = isSuper;
    }

    /**
     * @return The access level granted to the user by the given access maps.
     */
    public AccessLevel levelFor(Map<String,AccessLevel> usersAccess, Map<String,AccessLevel> groupsAccess)
    {
        // determine the maximum access level for this user and groups
        AccessLevel level = usersAccess.get(username);
        if (level == null)
            level = AccessLevel.NONE;
        for (String group : groups)
        {
            AccessLevel forGroup = groupsAccess.get(group);
            if (forGroup != null && forGroup.ordinal() > level.ordinal())
                level = forGroup;
        }
        return level;
    }

    @Override
    public String toString()
    {
        return String.format("#<User %s groups=%s>", username, groups);
    }
}
