package org.apache.cassandra.cql3.selection;

import java.util.*;
import java.util.stream.Collectors;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnSpecification;

/**
 * Separately maintains the ColumnSpecifications and their mappings to underlying
 * columns as we may receive null mappings. This occurs where a query result
 * includes a column specification which does not map to any particular real
 * column, e.g. COUNT queries or where no-arg functions like now() are used
 */
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
