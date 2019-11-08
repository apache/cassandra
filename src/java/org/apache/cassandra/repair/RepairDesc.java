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

package org.apache.cassandra.repair;

import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.repair.messages.RepairOption;

public class RepairDesc
{
    public final UUID parentSession;
    public final RepairOption options;
    public final String keyspace;

    public RepairDesc(UUID parentSession, RepairOption options, String keyspace)
    {
        this.parentSession = parentSession;
        this.options = options;
        this.keyspace = keyspace;
    }

    // NOTE (equals, hashCode) - options is intentially excluded; it doesn't implement eqauls and hashCode,
    // but also not needed since parentSession is expected to be unique.
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepairDesc that = (RepairDesc) o;
        return Objects.equals(parentSession, that.parentSession) &&
               Objects.equals(keyspace, that.keyspace);
    }

    public int hashCode()
    {
        return Objects.hash(parentSession, keyspace);
    }
}
