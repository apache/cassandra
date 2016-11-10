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
package org.apache.cassandra.schema;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.antlr.runtime.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.SyntaxException;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public final class ViewMetadata
{
    public final String keyspace;
    public final String name;
    public final TableId baseTableId;
    public final String baseTableName;
    public final boolean includeAllColumns;
    public final TableMetadata metadata;

    public final SelectStatement.RawStatement select;
    public final String whereClause;

    /**
     * @param name              Name of the view
     * @param baseTableId       Internal ID of the table which this view is based off of
     * @param includeAllColumns Whether to include all columns or not
     */
    public ViewMetadata(String keyspace,
                        String name,
                        TableId baseTableId,
                        String baseTableName,
                        boolean includeAllColumns,
                        SelectStatement.RawStatement select,
                        String whereClause,
                        TableMetadata metadata)
    {
        this.keyspace = keyspace;
        this.name = name;
        this.baseTableId = baseTableId;
        this.baseTableName = baseTableName;
        this.includeAllColumns = includeAllColumns;
        this.select = select;
        this.whereClause = whereClause;
        this.metadata = metadata;
    }

    /**
     * @return true if the view specified by this definition will include the column, false otherwise
     */
    public boolean includes(ColumnIdentifier column)
    {
        return metadata.getColumn(column) != null;
    }

    public ViewMetadata copy(TableMetadata newMetadata)
    {
        return new ViewMetadata(keyspace, name, baseTableId, baseTableName, includeAllColumns, select, whereClause, newMetadata);
    }

    public TableMetadata baseTableMetadata()
    {
        return Schema.instance.getTableMetadata(baseTableId);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof ViewMetadata))
            return false;

        ViewMetadata other = (ViewMetadata) o;
        return Objects.equals(keyspace, other.keyspace)
               && Objects.equals(name, other.name)
               && Objects.equals(baseTableId, other.baseTableId)
               && Objects.equals(includeAllColumns, other.includeAllColumns)
               && Objects.equals(whereClause, other.whereClause)
               && Objects.equals(metadata, other.metadata);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
               .append(keyspace)
               .append(name)
               .append(baseTableId)
               .append(includeAllColumns)
               .append(whereClause)
               .append(metadata)
               .toHashCode();
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
               .append("keyspace", keyspace)
               .append("name", name)
               .append("baseTableId", baseTableId)
               .append("baseTableName", baseTableName)
               .append("includeAllColumns", includeAllColumns)
               .append("whereClause", whereClause)
               .append("metadata", metadata)
               .toString();
    }

    /**
     * Replace the column 'from' with 'to' in this materialized view definition's partition,
     * clustering, or included columns.
     * @param from the existing column
     * @param to the new column
     */
    public ViewMetadata renamePrimaryKeyColumn(ColumnIdentifier from, ColumnIdentifier to)
    {
        // convert whereClause to Relations, rename ids in Relations, then convert back to whereClause
        List<Relation> relations = whereClauseToRelations(whereClause);
        ColumnMetadata.Raw fromRaw = ColumnMetadata.Raw.forQuoted(from.toString());
        ColumnMetadata.Raw toRaw = ColumnMetadata.Raw.forQuoted(to.toString());
        List<Relation> newRelations =
            relations.stream()
                     .map(r -> r.renameIdentifier(fromRaw, toRaw))
                     .collect(Collectors.toList());

        String rawSelect = View.buildSelectStatement(baseTableName, metadata.columns(), whereClause);

        return new ViewMetadata(keyspace,
                                name,
                                baseTableId,
                                baseTableName,
                                includeAllColumns,
                                (SelectStatement.RawStatement) QueryProcessor.parseStatement(rawSelect),
                                View.relationsToWhereClause(newRelations),
                                metadata.unbuild().renamePrimaryKeyColumn(from, to).build());
    }

    public ViewMetadata withAddedRegularColumn(ColumnMetadata column)
    {
        return new ViewMetadata(keyspace,
                                name,
                                baseTableId,
                                baseTableName,
                                includeAllColumns,
                                select,
                                whereClause,
                                metadata.unbuild().addColumn(column).build());
    }

    public ViewMetadata withAlteredColumnType(ColumnIdentifier name, AbstractType<?> type)
    {
        return new ViewMetadata(keyspace,
                                this.name,
                                baseTableId,
                                baseTableName,
                                includeAllColumns,
                                select,
                                whereClause,
                                metadata.unbuild().alterColumnType(name, type).build());
    }

    private static List<Relation> whereClauseToRelations(String whereClause)
    {
        try
        {
            return CQLFragmentParser.parseAnyUnhandled(CqlParser::whereClause, whereClause).build().relations;
        }
        catch (RecognitionException | SyntaxException exc)
        {
            throw new RuntimeException("Unexpected error parsing materialized view's where clause while handling column rename: ", exc);
        }
    }
}
