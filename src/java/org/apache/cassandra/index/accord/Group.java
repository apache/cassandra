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

package org.apache.cassandra.index.accord;

import java.util.Objects;

import org.apache.cassandra.schema.TableId;

public class Group implements Comparable<Group>
{
    public final int storeId;
    public final TableId tableId;

    public Group(int storeId, TableId tableId)
    {
        this.storeId = storeId;
        this.tableId = tableId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Group group = (Group) o;
        return storeId == group.storeId && Objects.equals(tableId, group.tableId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(storeId, tableId);
    }

    @Override
    public String toString()
    {
        return "Group{" +
               "storeId=" + storeId +
               ", tableId=" + tableId +
               '}';
    }

    @Override
    public int compareTo(Group o)
    {
        int rc = Integer.compare(storeId, o.storeId);
        if (rc == 0)
            rc = tableId.compareTo(o.tableId);
        return rc;
    }
}
