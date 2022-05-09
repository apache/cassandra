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

import java.util.EnumSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * Container for granted permissions, restricted permissions and grantable permissions.
 */
public final class PermissionSets
{
    public static final PermissionSets EMPTY = builder().build();

    /**
     * Immutable set of granted permissions.
     */
    public final Set<Permission> granted;
    /**
     * Immutable set of restricted permissions.
     */
    public final Set<Permission> restricted;
    /**
     * Immutable set of permissions grantable to others.
     */
    public final Set<Permission> grantables;

    private PermissionSets(Set<Permission> granted, Set<Permission> restricted, Set<Permission> grantables)
    {
        this.granted = granted;
        this.restricted = restricted;
        this.grantables = grantables;
    }

    public Set<GrantMode> grantModesFor(Permission permission)
    {
        Set<GrantMode> modes = EnumSet.noneOf(GrantMode.class);
        if (granted.contains(permission))
            modes.add(GrantMode.GRANT);
        if (restricted.contains(permission))
            modes.add(GrantMode.RESTRICT);
        if (grantables.contains(permission))
            modes.add(GrantMode.GRANTABLE);
        return modes;
    }

    /**
     * Returns all permissions that are contained in {@link #granted}, {@link #restricted}
     * and {@link #grantables}.
     */
    public Set<Permission> allContainedPermissions()
    {
        Set<Permission> all = EnumSet.noneOf(Permission.class);
        all.addAll(granted);
        all.addAll(restricted);
        all.addAll(grantables);
        return all;
    }

    public Builder unbuild()
    {
        return new Builder().addGranted(granted)
                            .addRestricted(restricted)
                            .addGrantables(grantables);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private final Set<Permission> granted = EnumSet.noneOf(Permission.class);
        private final Set<Permission> restricted = EnumSet.noneOf(Permission.class);
        private final Set<Permission> grantables = EnumSet.noneOf(Permission.class);

        private Builder()
        {
        }

        public Builder accumulate(PermissionSets... permissionSets)
        {
            for (int i = 0; i < permissionSets.length; i++)
            {
                this.granted.addAll(permissionSets[i].granted);
                this.restricted.addAll(permissionSets[i].restricted);
                this.grantables.addAll(permissionSets[i].grantables);
            }

            return this;
        }

        public Builder addGranted(Set<Permission> granted)
        {
            this.granted.addAll(granted);
            return this;
        }

        public Builder addRestricted(Set<Permission> restricted)
        {
            this.restricted.addAll(restricted);
            return this;
        }

        public Builder addGrantables(Set<Permission> grantables)
        {
            this.grantables.addAll(grantables);
            return this;
        }

        public Builder addGranted(Permission granted)
        {
            this.granted.add(granted);
            return this;
        }

        public Builder addRestricted(Permission restricted)
        {
            this.restricted.add(restricted);
            return this;
        }

        public Builder addGrantable(Permission grantable)
        {
            this.grantables.add(grantable);
            return this;
        }

        public Builder removeGranted(Set<Permission> granted)
        {
            this.granted.removeAll(granted);
            return this;
        }

        public Builder removeRestricted(Set<Permission> restricted)
        {
            this.restricted.removeAll(restricted);
            return this;
        }

        public Builder removeGrantables(Set<Permission> grantables)
        {
            this.grantables.removeAll(grantables);
            return this;
        }

        public Builder removeGranted(Permission granted)
        {
            this.granted.remove(granted);
            return this;
        }

        public Builder removeRestricted(Permission restricted)
        {
            this.restricted.remove(restricted);
            return this;
        }

        public Builder removeGrantable(Permission grantable)
        {
            this.grantables.remove(grantable);
            return this;
        }

        public Builder purgeNonApplicabePermissions(Set<Permission> applicablePermissions)
        {
            this.granted.retainAll(applicablePermissions);
            this.grantables.retainAll(applicablePermissions);
            this.restricted.retainAll(applicablePermissions);
            return this;
        }

        public PermissionSets build()
        {
            return new PermissionSets(ImmutableSet.copyOf(granted),
                                      ImmutableSet.copyOf(restricted),
                                      ImmutableSet.copyOf(grantables));
        }
    }
}
