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
package org.apache.cassandra.cql3;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;

import java.util.Collection;
import java.util.Iterator;

public class ColumnSpecification
{
    public final String ksName;
    public final String cfName;
    public final ColumnIdentifier name;
    public final AbstractType<?> type;

    public ColumnSpecification(String ksName, String cfName, ColumnIdentifier name, AbstractType<?> type)
    {
        this.ksName = ksName;
        this.cfName = cfName;
        this.name = name;
        this.type = type;
    }

    /**
     * Returns a new <code>ColumnSpecification</code> for the same column but with the specified alias.
     *
     * @param alias the column alias
     * @return a new <code>ColumnSpecification</code> for the same column but with the specified alias.
     */
    public ColumnSpecification withAlias(ColumnIdentifier alias)
    {
        return new ColumnSpecification(ksName, cfName, alias, type);
    }

    public boolean isReversedType()
    {
        return type instanceof ReversedType;
    }

    /**
     * Returns true if all ColumnSpecifications are in the same table, false otherwise.
     */
    public static boolean allInSameTable(Collection<ColumnSpecification> names)
    {
        if (names == null || names.isEmpty())
            return false;

        Iterator<ColumnSpecification> iter = names.iterator();
        ColumnSpecification first = iter.next();
        while (iter.hasNext())
        {
            ColumnSpecification name = iter.next();
            if (!name.ksName.equals(first.ksName) || !name.cfName.equals(first.cfName))
                return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof ColumnSpecification))
            return false;

        ColumnSpecification that = (ColumnSpecification) other;
        return this.ksName.equals(that.ksName) &&
               this.cfName.equals(that.cfName) &&
               this.name.equals(that.name) &&
               this.type.equals(that.type);
    }

    public int hashCode()
    {
        return Objects.hashCode(ksName, cfName, name, type);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("name", name)
                          .add("type", type)
                          .toString();
    }
}
