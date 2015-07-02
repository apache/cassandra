package org.apache.cassandra.cql3.statements;

import java.util.*;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.*;

import org.apache.cassandra.cql3.ColumnIdentifier;

import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.marshal.LongType;

import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.ColumnSpecification;

public class SelectionColumnMapping implements SelectionColumns
{
    private final ArrayList<ColumnSpecification> columnSpecifications;
    private final HashMultimap<ColumnSpecification, CFDefinition.Name> columnMappings;

    private SelectionColumnMapping()
    {
        this.columnSpecifications = new ArrayList<>();
        this.columnMappings = HashMultimap.create();
    }

    protected static SelectionColumnMapping newMapping()
    {
        return new SelectionColumnMapping();
    }

    protected static SelectionColumnMapping countMapping(CFDefinition cfDef, ColumnIdentifier countAlias)
    {
        ColumnSpecification spec = new ColumnSpecification(cfDef.cfm.ksName,
                                                           cfDef.cfm.cfName,
                                                           countAlias == null ? ResultSet.COUNT_COLUMN
                                                                              : countAlias,
                                                           LongType.instance);

        return new SelectionColumnMapping().addMapping(spec, Collections.<CFDefinition.Name>emptyList());
    }

    protected static SelectionColumnMapping simpleMapping(Iterable<CFDefinition.Name> columnDefinitions)
    {
        SelectionColumnMapping mapping = new SelectionColumnMapping();
        for (CFDefinition.Name def: columnDefinitions)
            mapping.addMapping(def, def);
        return mapping;
    }

    protected SelectionColumnMapping addMapping(ColumnSpecification colSpec, CFDefinition.Name column)
    {
        columnSpecifications.add(colSpec);
        // functions without arguments do not map to any column, so don't
        // record any mapping in that case
        if (column != null)
            columnMappings.put(colSpec, column);
        return this;
    }

    protected SelectionColumnMapping addMapping(ColumnSpecification colSpec, Iterable<CFDefinition.Name> columns)
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

        SelectionColumns other = (SelectionColumns)obj;
        return Objects.equals(columnMappings, other.getMappings())
            && Objects.equals(columnSpecifications, other.getColumnSpecifications());
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
