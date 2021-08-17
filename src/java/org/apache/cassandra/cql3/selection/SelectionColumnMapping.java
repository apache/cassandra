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
package org.apache.cassandra.cql3.selection;

import java.util.*;
import java.util.stream.Collectors;

import com.google.common.base.Objects;
import com.google.common.collect.*;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.ColumnSpecification;

/**
 * Separately maintains the ColumnSpecifications and their mappings to underlying
 * columns as we may receive null mappings. This occurs where a query result
 * includes a column specification which does not map to any particular real
 * column, e.g. COUNT queries or where no-arg functions like now() are used
 */
public class SelectionColumnMapping implements SelectionColumns
{
    private final List<ColumnSpecification> columnSpecifications;
    private final Multimap<ColumnSpecification, ColumnMetadata> columnMappings;

    private SelectionColumnMapping()
    {
        this.columnSpecifications = new ArrayList<>();
        this.columnMappings = HashMultimap.create();
    }

    protected static SelectionColumnMapping newMapping()
    {
        return new SelectionColumnMapping();
    }

    protected static SelectionColumnMapping simpleMapping(Iterable<ColumnMetadata> columnDefinitions)
    {
        SelectionColumnMapping mapping = new SelectionColumnMapping();
        for (ColumnMetadata def: columnDefinitions)
            mapping.addMapping(def, def);
        return mapping;
    }

    protected SelectionColumnMapping addMapping(ColumnSpecification colSpec, ColumnMetadata column)
    {
        columnSpecifications.add(colSpec);
        // functions without arguments do not map to any column, so don't
        // record any mapping in that case
        if (column != null)
            columnMappings.put(colSpec, column);
        return this;
    }

    protected SelectionColumnMapping addMapping(ColumnSpecification colSpec, Iterable<ColumnMetadata> columns)
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

    public Multimap<ColumnSpecification, ColumnMetadata> getMappings()
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
        return columnMappings.asMap()
                             .entrySet()
                             .stream()
                             .map(entry ->
                                  entry.getValue()
                                       .stream()
                                       .map(colDef -> colDef.name.toString())
                                       .collect(Collectors.joining(", ", entry.getKey().name.toString() + ":[", "]")))
                             .collect(Collectors.joining(", ",
                                                         columnSpecifications.stream()
                                                                             .map(colSpec -> colSpec.name.toString())
                                                                             .collect(Collectors.joining(", ",
                                                                                                         "{ Columns:[",
                                                                                                         "], Mappings:{")),
                                                         "} }"));
    }
}
