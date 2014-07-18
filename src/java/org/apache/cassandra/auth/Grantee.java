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

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ComparisonChain;

public abstract class Grantee implements IGrantee
{
    protected final String name;

    public static IGrantee fromId(String id)
    {
        String[] pair = id.split(":", 2);
        if (Type.valueOf(pair[0]) == Type.User)
            return new User(pair[1]);
        else
            return new Role(pair[1]);
    }

    public static User asUser(String username)
    {
        return new User(username);
    }

    public static Role asRole(String rolename)
    {
        return new Role(rolename);
    }

    protected Grantee(String name)
    {
        this.name = name;
    }

    @Override
    public int compareTo(IGrantee other)
    {
        return ComparisonChain.start()
                              .compare(getType(), other.getType())
                              .compare(getName(), other.getName())
                              .result();
    }

    @Override
    public String getName()
    {
        return name;
    }

    private String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    @Override
    public String toString()
    {
        return String.format("#<%s %s>", getType(), name);
    }
}
