package org.apache.cassandra.cql3.statements;

import java.util.*;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnSpecification;

public class SelectionColumnMapping implements SelectionColumns
{
    // Uses LinkedHashMultimap because ordering of keys must be maintained
    private final LinkedHashMultimap<ColumnSpecification, ColumnDefinition> columnMappings;

    private SelectionColumnMapping()
    {
        this.columnMappings = LinkedHashMultimap.create();
    }

    protected static SelectionColumnMapping newMapping()
    {
        return new SelectionColumnMapping();
    }

    protected static SelectionColumnMapping simpleMapping(List<ColumnDefinition> columnDefinitions)
    {
        SelectionColumnMapping mapping = new SelectionColumnMapping();
        for (ColumnDefinition def: columnDefinitions)
            mapping.addMapping(def, def);
        return mapping;
    }

    protected SelectionColumnMapping addMapping(ColumnSpecification colSpec, ColumnDefinition column)
    {
        columnMappings.put(colSpec, column);
        return this;
    }

    protected SelectionColumnMapping addMapping(ColumnSpecification colSpec, Iterable<ColumnDefinition> columns)
    {
        columnMappings.putAll(colSpec, columns);
        return this;
    }

    public List<ColumnSpecification> getColumnSpecifications()
    {
        // return a mutable copy as we may add extra columns
        // for ordering (CASSANDRA-4911 & CASSANDRA-8286)
        return Lists.newArrayList(columnMappings.keySet());
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

        return Objects.equal(this.columnMappings, ((SelectionColumnMapping) obj).columnMappings);
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

        StringBuilder builder = new StringBuilder();
        builder.append("{ ");
        builder.append(Joiner.on(", ")
                             .join(Iterables.transform(columnMappings.asMap().entrySet(),
                                                       mappingEntryToString)));
        builder.append(" }");
        return builder.toString();
    }

}
