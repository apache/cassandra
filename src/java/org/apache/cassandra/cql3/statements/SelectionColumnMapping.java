/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cql3.statements;

import java.util.*;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.marshal.LongType;

public class SelectionColumnMapping implements SelectionColumns
{
    private final ArrayList<ColumnSpecification> columnSpecifications;
    private final HashMultimap<ColumnSpecification, ColumnDefinition> columnMappings;

    private SelectionColumnMapping()
    {
        this.columnSpecifications = new ArrayList<>();
        this.columnMappings = HashMultimap.create();
    }

    protected static SelectionColumnMapping newMapping()
    {
        return new SelectionColumnMapping();
    }

    protected static SelectionColumnMapping countMapping(CFMetaData cfm, ColumnIdentifier countAlias)
    {
        ColumnSpecification spec = new ColumnSpecification(cfm.ksName,
                                                           cfm.cfName,
                                                           countAlias == null ? ResultSet.COUNT_COLUMN
                                                                              : countAlias,
                                                           LongType.instance);

        return new SelectionColumnMapping().addMapping(spec, Collections.<ColumnDefinition>emptyList());
    }

    protected static SelectionColumnMapping simpleMapping(Iterable<ColumnDefinition> columnDefinitions)
    {
        SelectionColumnMapping mapping = new SelectionColumnMapping();
        for (ColumnDefinition def: columnDefinitions)
            mapping.addMapping(def, def);
        return mapping;
    }

    protected SelectionColumnMapping addMapping(ColumnSpecification colSpec, ColumnDefinition column)
    {
        columnSpecifications.add(colSpec);
        // functions without arguments do not map to any column, so don't
        // record any mapping in that case
        if (column != null)
            columnMappings.put(colSpec, column);
        return this;
    }

    protected SelectionColumnMapping addMapping(ColumnSpecification colSpec, Iterable<ColumnDefinition> columns)
    {
        columnSpecifications.add(colSpec);
        columnMappings.putAll(colSpec, columns);
        return this;
    }

    public List<ColumnSpecification> getColumnSpecifications()
    {
        // return a mutable copy as we may add extra columns
        // for ordering (CASSANDRA-4911 & CASSANDRA-8286)
        return Lists.newArrayList(columnSpecifications);
    }

    public Multimap<ColumnSpecification, ColumnDefinition> getMappings()
    {
        return Multimaps.unmodifiableMultimap(columnMappings);
    }

    public boolean equals(Object obj)
    {
        if (obj == null)
            return false;

        if (!(obj instanceof SelectionColumnMapping))
            return false;

        SelectionColumns other = (SelectionColumns)obj;
        return Objects.equal(columnMappings, other.getMappings())
            && Objects.equal(columnSpecifications, other.getColumnSpecifications());
    }

    public int hashCode()
    {
        return Objects.hashCode(columnMappings);
    }

    public String toString()
    {
        final Function<ColumnDefinition, String> getDefName = new Function<ColumnDefinition, String>()
        {
            public String apply(ColumnDefinition columnDefinition)
            {
                return columnDefinition.name.toString();
            }
        };
        Function<Map.Entry<ColumnSpecification, Collection<ColumnDefinition>>, String> mappingEntryToString =
        new Function<Map.Entry<ColumnSpecification, Collection<ColumnDefinition>>, String>(){
            public String apply(Map.Entry<ColumnSpecification, Collection<ColumnDefinition>> entry)
            {
                StringBuilder builder = new StringBuilder();
                builder.append(entry.getKey().name.toString());
                builder.append(":[");
                builder.append(Joiner.on(',').join(Iterables.transform(entry.getValue(), getDefName)));
                builder.append("]");
                return builder.toString();
            }
        };

        Function<ColumnSpecification, String> colSpecToString = new Function<ColumnSpecification, String>()
        {
            public String apply(ColumnSpecification columnSpecification)
            {
                return columnSpecification.name.toString();
            }
        };

        StringBuilder builder = new StringBuilder();
        builder.append("{ Columns:[");
        builder.append(Joiner.on(",")
                             .join(Iterables.transform(columnSpecifications, colSpecToString )));
        builder.append("], Mappings:[");
        builder.append(Joiner.on(", ")
                             .join(Iterables.transform(columnMappings.asMap().entrySet(),
                                                       mappingEntryToString)));
        builder.append("] }");
        return builder.toString();
    }

}
