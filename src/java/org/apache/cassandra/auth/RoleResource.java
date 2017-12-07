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

import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * IResource implementation representing database roles.
 *
 * The root level "roles" resource represents the collection of all Roles.
 * Individual roles are represented as members of the collection:
 * "roles"                    - the root level collection resource
 * "roles/role1"              - a specific database role
 */
public class RoleResource implements IResource, Comparable<RoleResource>
{
    enum Level
    {
        ROOT, ROLE
    }

    // permissions which may be granted on the root level resource
    private static final Set<Permission> ROOT_LEVEL_PERMISSIONS = Sets.immutableEnumSet(Permission.CREATE,
                                                                                        Permission.ALTER,
                                                                                        Permission.DROP,
                                                                                        Permission.AUTHORIZE,
                                                                                        Permission.DESCRIBE);
    // permissions which may be granted on role level resources
    private static final Set<Permission> ROLE_LEVEL_PERMISSIONS = Sets.immutableEnumSet(Permission.ALTER,
                                                                                        Permission.DROP,
                                                                                        Permission.AUTHORIZE);

    private static final String ROOT_NAME = "roles";
    private static final RoleResource ROOT_RESOURCE = new RoleResource();

    private final Level level;
    private final String name;

    private RoleResource()
    {
        level = Level.ROOT;
        name = null;
    }

    private RoleResource(String name)
    {
        level = Level.ROLE;
        this.name = name;
    }

    /**
     * @return the root-level resource.
     */
    public static RoleResource root()
    {
        return ROOT_RESOURCE;
    }

    /**
     * Creates a RoleResource representing an individual Role.
     * @param name name of the Role.
     * @return RoleResource instance reresenting the Role.
     */
    public static RoleResource role(String name)
    {
        return new RoleResource(name);
    }

    /**
     * Parses a role resource name into a RoleResource instance.
     *
     * @param name Name of the data resource.
     * @return RoleResource instance matching the name.
     */
    public static RoleResource fromName(String name)
    {
        String[] parts = StringUtils.split(name, "/", 2);

        if (!parts[0].equals(ROOT_NAME))
            throw new IllegalArgumentException(String.format("%s is not a valid role resource name", name));

        if (parts.length == 1)
            return root();

        return role(parts[1]);
    }

    /**
     * @return Printable name of the resource.
     */
    public String getName()
    {
        return level == Level.ROOT ? ROOT_NAME : String.format("%s/%s", ROOT_NAME, name);
    }

    /**
     * @return short form name of a role level resource. i.e. not the full "root/name" version returned by getName().
     * Throws IllegalStateException if called on the root-level resource.
     */
    public String getRoleName()
    {
        if (level == Level.ROOT)
            throw new IllegalStateException(String.format("%s role resource has no role name", level));
        return name;
    }

    /**
     * @return Parent of the resource, if any. Throws IllegalStateException if it's the root-level resource.
     */
    public IResource getParent()
    {
        if (level == Level.ROLE)
            return root();

        throw new IllegalStateException("Root-level resource can't have a parent");
    }

    public boolean hasParent()
    {
        return level != Level.ROOT;
    }

    public boolean exists()
    {
        return level == Level.ROOT || DatabaseDescriptor.getRoleManager().isExistingRole(this);
    }

    public Set<Permission> applicablePermissions()
    {
        return level == Level.ROOT ? ROOT_LEVEL_PERMISSIONS : ROLE_LEVEL_PERMISSIONS;
    }

    public int compareTo(RoleResource o)
    {
        return this.name.compareTo(o.name);
    }

    @Override
    public String toString()
    {
        return level == Level.ROOT ? "<all roles>" : String.format("<role %s>", name);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof RoleResource))
            return false;

        RoleResource rs = (RoleResource) o;

        return Objects.equal(level, rs.level) && Objects.equal(name, rs.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(level, name);
    }
}
