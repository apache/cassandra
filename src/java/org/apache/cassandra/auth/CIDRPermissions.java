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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Contains CIDR permissions of a role
 */
public abstract class CIDRPermissions
{
    /**
     * Determines whether this permissions object allows access from given cidr group(s)
     * @param cidrGroup set of CIDR groups
     * @return returns true if role has access, otherwise false
     */
    public abstract boolean canAccessFrom(Set<String> cidrGroup);

    /**
     * Determines whether this permissions object restricts access from some CIDR groups
     * or allows access from any CIDR group
     * @return true if restricts access, otherwise false
     */
    public abstract boolean restrictsAccess();

    /**
     * Returns CIDR permissions allowed by this object
     * @return returns set of CIDR groups
     */
    public abstract Set<String> allowedCIDRGroups();

    /**
     *  Validates does this object contains valid CIDR groups
     */
    public abstract void validate();

    // This class represents a subset of CIDR permissions, i.e, not ALL and not NONE.
    private static class SubsetPermissions extends CIDRPermissions
    {
        private final Set<String> subset;

        public SubsetPermissions(Set<String> subset)
        {
            Preconditions.checkNotNull(subset);
            this.subset = subset;
        }

        public boolean canAccessFrom(Set<String> cidrGroups)
        {
            return subset.stream().anyMatch(cidrGroups::contains);
        }

        public boolean restrictsAccess()
        {
            return true;
        }

        public Set<String> allowedCIDRGroups()
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
            Set<String> availableCidrGroups = DatabaseDescriptor.getCIDRAuthorizer()
                                                                .getCidrGroupsMappingManager()
                                                                .getAvailableCidrGroups();
            Set<String> unknownCidrGroups = Sets.difference(subset, availableCidrGroups);
            if (!unknownCidrGroups.isEmpty())
            {
                throw new InvalidRequestException("Invalid CIDR group(s): " + subset + ". Available CIDR Groups are: "
                                                  + availableCidrGroups);
            }
        }
    }

    private static final CIDRPermissions ALL = new CIDRPermissions()
    {
        public boolean canAccessFrom(Set<String> cidrGroup)
        {
            return true;
        }

        public boolean restrictsAccess()
        {
            return false;
        }

        public Set<String> allowedCIDRGroups()
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

    private static final CIDRPermissions NONE = new CIDRPermissions()
    {
        public boolean canAccessFrom(Set<String> cidrGroup)
        {
            return false;
        }

        public boolean restrictsAccess()
        {
            return true;
        }

        public Set<String> allowedCIDRGroups()
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

    /**
     * Generates CIDR permissions object which allows from all CIDR groups
     * @return returns CIDRPermissions object
     */
    public static CIDRPermissions all()
    {
        return ALL;
    }

    /**
     * Generates CIDR permissions object which doesn't allow from any CIDR group
     * @return returns CIDRPermissions object
     */
    public static CIDRPermissions none()
    {
        return NONE;
    }

    /**
     * Generates CIDR permissions object with given CIDR groups
     * @param cidrGroups set of CIDR groups
     * @return returns CIDRPermissions object
     */
    public static CIDRPermissions subset(Set<String> cidrGroups)
    {
        return new SubsetPermissions(cidrGroups);
    }

    /**
     * Builder to generate CIDR Permissions
     */
    public static class Builder
    {
        private final Set<String> cidrGroups = new HashSet<>();
        private boolean isAll = false;
        private boolean modified = false;

        public void add(String cidrGroup)
        {
            Preconditions.checkArgument(!isAll, "All has been set");
            cidrGroups.add(cidrGroup);
            modified = true;
        }

        public void all()
        {
            Preconditions.checkArgument(cidrGroups.isEmpty(), "CIDR Groups have already been set");
            isAll = true;
            modified = true;
        }

        public boolean isModified()
        {
            return modified;
        }

        public CIDRPermissions build()
        {
            if (cidrGroups.isEmpty())
            {
                return CIDRPermissions.all();
            }
            else
            {
                return subset(cidrGroups);
            }
        }
    }

    /**
     * Builder object to generate CIDR permissions
     * @return returns Builder object
     */
    public static Builder builder()
    {
        return new Builder();
    }
}
