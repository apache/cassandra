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

import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Datacenters;
import org.apache.cassandra.exceptions.InvalidRequestException;

public abstract class DCPermissions
{
    /**
     * returns true if the user can access the given dc
     */
    public abstract boolean canAccess(String dc);

    /**
     * Indicates whether the permissions object explicitly allow access to
     * some dcs (true) or if it implicitly allows access to all dcs (false)
     */
    public abstract boolean restrictsAccess();
    public abstract Set<String> allowedDCs();
    public abstract void validate();

    private static class SubsetPermissions extends DCPermissions
    {
        private final Set<String> subset;

        public SubsetPermissions(Set<String> subset)
        {
            Preconditions.checkNotNull(subset);
            this.subset = subset;
        }

        public boolean canAccess(String dc)
        {
            return subset.contains(dc);
        }

        public boolean restrictsAccess()
        {
            return true;
        }

        public Set<String> allowedDCs()
        {
            return ImmutableSet.copyOf(subset);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SubsetPermissions that = (SubsetPermissions) o;

            return subset.equals(that.subset);
        }

        public int hashCode()
        {
            return subset.hashCode();
        }

        public String toString()
        {
            StringJoiner joiner = new StringJoiner(", ");
            subset.forEach(joiner::add);
            return joiner.toString();
        }

        public void validate()
        {
            Set<String> unknownDcs = Sets.difference(subset, Datacenters.getValidDatacenters());
            if (!unknownDcs.isEmpty())
            {
                throw new InvalidRequestException(String.format("Invalid value(s) for DATACENTERS '%s'," +
                                                                "All values must be valid datacenters", subset));
            }
        }
    }

    private static final DCPermissions ALL = new DCPermissions()
    {
        public boolean canAccess(String dc)
        {
            return true;
        }

        public boolean restrictsAccess()
        {
            return false;
        }

        public Set<String> allowedDCs()
        {
            throw new UnsupportedOperationException();
        }

        public String toString()
        {
            return "ALL";
        }

        public void validate()
        {

        }
    };

    private static final DCPermissions NONE = new DCPermissions()
    {
        public boolean canAccess(String dc)
        {
            return false;
        }

        public boolean restrictsAccess()
        {
            return true;
        }

        public Set<String> allowedDCs()
        {
            throw new UnsupportedOperationException();
        }

        public String toString()
        {
            return "n/a";
        }

        public void validate()
        {
            throw new UnsupportedOperationException();
        }
    };

    public static DCPermissions all()
    {
        return ALL;
    }

    public static DCPermissions none()
    {
        return NONE;
    }

    public static DCPermissions subset(Set<String> dcs)
    {
        return new SubsetPermissions(dcs);
    }

    public static DCPermissions subset(String... dcs)
    {
        return subset(Sets.newHashSet(dcs));
    }

    public static class Builder
    {
        private Set<String> dcs = new HashSet<>();
        private boolean isAll = false;
        private boolean modified = false;

        public void add(String dc)
        {
            Preconditions.checkArgument(!isAll, "All has been set");
            dcs.add(dc);
            modified = true;
        }

        public void all()
        {
            Preconditions.checkArgument(dcs.isEmpty(), "DCs have already been set");
            isAll = true;
            modified = true;
        }

        public boolean isModified()
        {
            return modified;
        }

        public DCPermissions build()
        {
            if (dcs.isEmpty())
            {
                return DCPermissions.all();
            }
            else
            {
                return subset(dcs);
            }
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }
}
