package org.apache.cassandra.cql3.selection;

import java.util.LinkedHashSet;
import java.util.List;

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
    // Uses a LinkedHashSet as both order and uniqueness need to be preserved
    private final LinkedHashSet<ColumnSpecification> columnSpecifications;
    private final HashMultimap<ColumnSpecification, ColumnDefinition> columnMappings;

    private SelectionColumnMapping()
    {
        this.columnSpecifications = new LinkedHashSet<>();
        this.columnMappings = HashMultimap.create();
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
        columnSpecifications.add(colSpec);
        // some AbstractFunctionSelector impls do not map directly to an underlying column
        // so don't record a mapping in that case
        if (null != column)
            columnMappings.put(colSpec, column);
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

        return Objects.equal(this.columnMappings, ((SelectionColumnMapping)obj).columnMappings);
    }

    public int hashCode()
    {
        return Objects.hashCode(columnMappings);
    }

    public String toString()
    {
        final Function<ColumnDefinition, String> getDefName = new Function<ColumnDefinition, String>()
        {
            public String apply(ColumnDefinition def)
            {
                return def.name.toString();
            }
        };
        final Function<ColumnSpecification, String> colSpecToMappingString = new Function<ColumnSpecification, String>()
        {
            public String apply(ColumnSpecification colSpec)
            {
                StringBuilder builder = new StringBuilder();
                builder.append(colSpec.name.toString());
                if (columnMappings.containsKey(colSpec))
                {
                    builder.append(":[");
                    builder.append(Joiner.on(',').join(Iterables.transform(columnMappings.get(colSpec), getDefName)));
                    builder.append("]");
                }
                else
                {
                    builder.append(":[]");
                }
                return builder.toString();
            }
        };

        StringBuilder builder = new StringBuilder();
        builder.append("{ ");
        builder.append(Joiner.on(", ").join(Iterables.transform(columnSpecifications, colSpecToMappingString)));
        builder.append(" }");
        return builder.toString();
    }
}
