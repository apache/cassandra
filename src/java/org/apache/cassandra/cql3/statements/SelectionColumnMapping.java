package org.apache.cassandra.cql3.statements;

import java.util.*;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.*;

import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.ColumnSpecification;

public class SelectionColumnMapping implements SelectionColumns
{

    // Uses LinkedHashMultimap because ordering of keys must be maintained
    private final LinkedHashMultimap<ColumnSpecification, CFDefinition.Name> columnMappings;

    private SelectionColumnMapping()
    {
        this.columnMappings = LinkedHashMultimap.create();
    }

    protected static SelectionColumnMapping newMapping()
    {
        return new SelectionColumnMapping();
    }

    protected static SelectionColumnMapping simpleMapping(List<CFDefinition.Name> columnDefinitions)
    {
        SelectionColumnMapping mapping = new SelectionColumnMapping();
        for (CFDefinition.Name def: columnDefinitions)
            mapping.addMapping(def, def);
        return mapping;
    }

    protected SelectionColumnMapping addMapping(ColumnSpecification colSpec, CFDefinition.Name column)
    {
        columnMappings.put(colSpec, column);
        return this;
    }

    protected SelectionColumnMapping addMapping(ColumnSpecification colSpec, Iterable<CFDefinition.Name> columns)
    {
        columnMappings.putAll(colSpec, columns);
        return this;
    }

    public List<ColumnSpecification> getColumnSpecifications()
    {
        // return a mutable copy as we may add extra columns
        // for ordering (CASSANDRA-4911 & CASSANDRA-8286)
        return new ArrayList(columnMappings.keySet());
    }

    public Multimap<ColumnSpecification, CFDefinition.Name> getMappings()
    {
        return Multimaps.unmodifiableMultimap(columnMappings);
    }

    public boolean equals(Object obj)
    {
        if (obj == null)
            return false;

        if (!(obj instanceof SelectionColumns))
            return false;

        return Objects.equals(columnMappings, ((SelectionColumns) obj).getMappings());
    }

    public int hashCode()
    {
        return Objects.hashCode(columnMappings);
    }

    public String toString()
    {
        final Function<CFDefinition.Name, String> getDefName = new Function<CFDefinition.Name, String>()
        {
            public String apply(CFDefinition.Name name)
            {
                return name.toString();
            }
        };
        Function<Map.Entry<ColumnSpecification, Collection<CFDefinition.Name>>, String> mappingEntryToString =
        new Function<Map.Entry<ColumnSpecification, Collection<CFDefinition.Name>>, String>(){
            public String apply(Map.Entry<ColumnSpecification, Collection<CFDefinition.Name>> entry)
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
