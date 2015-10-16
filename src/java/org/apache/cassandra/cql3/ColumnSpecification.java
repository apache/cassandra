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

import com.google.common.base.Objects;

import org.apache.cassandra.db.marshal.AbstractType;

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

    public boolean equals(Object obj)
    {
        if (null == obj)
            return false;

        if(!(obj instanceof ColumnSpecification))
            return false;

        ColumnSpecification other = (ColumnSpecification)obj;
        return Objects.equal(ksName, other.ksName)
            && Objects.equal(cfName, other.cfName)
            && Objects.equal(name, other.name)
            && Objects.equal(type, other.type);
    }

    public int hashCode()
    {
        return Objects.hashCode(ksName, cfName, name, type);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add("name", name)
                      .add("type", type)
                      .toString();
    }

}
