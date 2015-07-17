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
package org.apache.cassandra.cql3.statements;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;

public class CFProperties
{
    public final TableAttributes properties = new TableAttributes();
    final Map<ColumnIdentifier, Boolean> definedOrdering = new LinkedHashMap<>(); // Insertion ordering is important
    boolean useCompactStorage = false;

    public void validate()
    {
        properties.validate();
    }

    public void setOrdering(ColumnIdentifier alias, boolean reversed)
    {
        definedOrdering.put(alias, reversed);
    }

    public void setCompactStorage()
    {
        useCompactStorage = true;
    }

    public AbstractType getReversableType(ColumnIdentifier targetIdentifier, AbstractType<?> type)
    {
        if (!definedOrdering.containsKey(targetIdentifier))
        {
            return type;
        }
        return definedOrdering.get(targetIdentifier) ? ReversedType.getInstance(type) : type;
    }
}
