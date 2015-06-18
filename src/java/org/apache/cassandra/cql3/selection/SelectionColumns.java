package org.apache.cassandra.cql3.selection;

import java.util.List;

import com.google.common.collect.Multimap;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnSpecification;

/**
 * Represents a mapping between the actual columns used to satisfy a Selection
 * and the column definitions included in the resultset metadata for the query.
 */
public interface SelectionColumns
{
    List<ColumnSpecification> getColumnSpecifications();
    Multimap<ColumnSpecification, ColumnDefinition> getMappings();
}
