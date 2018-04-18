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

package org.apache.cassandra.locator;

import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.cassandra.config.DatabaseDescriptor;

public class ReplicationFactor
{
    public static final ReplicationFactor ZERO = new ReplicationFactor(0);

    public final int trans;
    public final int replicas;
    public transient final int full;

    private ReplicationFactor(int replicas, int trans)
    {
        validate(replicas, trans);
        this.replicas = replicas;
        this.trans = trans;
        this.full = replicas - trans;
    }

    private ReplicationFactor(int replicas)
    {
        this(replicas, 0);
    }

    static void validate(int replicas, int trans)
    {
        Preconditions.checkArgument(trans == 0 || DatabaseDescriptor.isTransientReplicationEnabled(),
                                    "Transient replication is not enabled on this node");
        Preconditions.checkArgument(replicas >= 0,
                                    "Replication factor must be non-negative, found %s", replicas);
        Preconditions.checkArgument(trans == 0 || trans < replicas,
                                    "Transient replicas must be zero, or less than total replication factor. For %s/%s", replicas, trans);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicationFactor that = (ReplicationFactor) o;
        return full == that.full &&
               trans == that.trans &&
               replicas == that.replicas;
    }

    public int hashCode()
    {
        return Objects.hash(full, trans, replicas);
    }

    public static ReplicationFactor rf(int replicas)
    {
        return new ReplicationFactor(replicas);
    }

    public static ReplicationFactor rf(int replicas, int trans)
    {
        return new ReplicationFactor(replicas, trans);
    }

    public static ReplicationFactor fromString(String s)
    {
        if (s.contains("/"))
        {
            String[] parts = s.split("/");
            Preconditions.checkArgument(parts.length == 2,
                                        "Replication factor format is <replicas> or <replicas>/<transient>");
            return new ReplicationFactor(Integer.valueOf(parts[0]), Integer.valueOf(parts[1]));
        }
        else
        {
            return new ReplicationFactor(Integer.valueOf(s), 0);
        }
    }

    public String toString(boolean verbose)
    {
        StringBuilder sb = new StringBuilder();

        if (verbose)
            sb.append("rf(");

        sb.append(Integer.toString(replicas));

        if (trans > 0)
            sb.append('/').append(Integer.toString(trans));

        if (verbose)
            sb.append(')');

        return sb.toString();
    }

    @Override
    public String toString()
    {
        return toString(true);
    }
}
