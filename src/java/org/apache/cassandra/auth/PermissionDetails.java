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

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

/**
 *  Sets of instances of this class are returned by IAuthorizer.listPermissions() method for LIST PERMISSIONS query.
 *  None of the fields are nullable.
 */
public class PermissionDetails implements Comparable<PermissionDetails>
{
    public final String grantee;
    public final IResource resource;
    public final Permission permission;

    public PermissionDetails(String grantee, IResource resource, Permission permission)
    {
        this.grantee = grantee;
        this.resource = resource;
        this.permission = permission;
    }

    @Override
    public int compareTo(PermissionDetails other)
    {
        return ComparisonChain.start()
                              .compare(grantee, other.grantee)
                              .compare(resource.getName(), other.resource.getName())
                              .compare(permission, other.permission)
                              .result();
    }

    @Override
    public String toString()
    {
        return String.format("<PermissionDetails grantee:%s resource:%s permission:%s>",
                             grantee,
                             resource.getName(),
                             permission);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof PermissionDetails))
            return false;

        PermissionDetails pd = (PermissionDetails) o;
        return Objects.equal(grantee, pd.grantee)
            && Objects.equal(resource, pd.resource)
            && Objects.equal(permission, pd.permission);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(grantee, resource, permission);
    }
}
