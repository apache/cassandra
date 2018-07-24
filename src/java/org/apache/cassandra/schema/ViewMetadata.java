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

import java.nio.ByteBuffer;
import java.util.Optional;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.UserType;

public final class ViewMetadata
{
    public final TableId baseTableId;
    public final String baseTableName;

    public final boolean includeAllColumns;
    public final TableMetadata metadata;

    public final WhereClause whereClause;

    /**
     * @param baseTableId       Internal ID of the table which this view is based off of
     * @param includeAllColumns Whether to include all columns or not
     */
    public ViewMetadata(TableId baseTableId,
                        String baseTableName,
                        boolean includeAllColumns,
                        WhereClause whereClause,
                        TableMetadata metadata)
    {
        this.baseTableId = baseTableId;
        this.baseTableName = baseTableName;
        this.includeAllColumns = includeAllColumns;
        this.whereClause = whereClause;
        this.metadata = metadata;
    }

    public String keyspace()
    {
        return metadata.keyspace;
    }

    public String name()
    {
        return metadata.name;
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
        return new ViewMetadata(baseTableId, baseTableName, includeAllColumns, whereClause, newMetadata);
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
        return baseTableId.equals(other.baseTableId)
            && includeAllColumns == other.includeAllColumns
            && whereClause.equals(other.whereClause)
            && metadata.equals(other.metadata);
    }

    Optional<Difference> compare(ViewMetadata other)
    {
        if (!baseTableId.equals(other.baseTableId) || includeAllColumns != other.includeAllColumns || !whereClause.equals(other.whereClause))
            return Optional.of(Difference.SHALLOW);

        return metadata.compare(other.metadata);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
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
               .append("baseTableId", baseTableId)
               .append("baseTableName", baseTableName)
               .append("includeAllColumns", includeAllColumns)
               .append("whereClause", whereClause)
               .append("metadata", metadata)
               .toString();
    }

    public boolean referencesUserType(ByteBuffer name)
    {
        return metadata.referencesUserType(name);
    }

    public ViewMetadata withUpdatedUserType(UserType udt)
    {
        return referencesUserType(udt.name)
             ? copy(metadata.withUpdatedUserType(udt))
             : this;
    }

    public ViewMetadata withRenamedPrimaryKeyColumn(ColumnIdentifier from, ColumnIdentifier to)
    {
        // convert whereClause to Relations, rename ids in Relations, then convert back to whereClause
        ColumnMetadata.Raw rawFrom = ColumnMetadata.Raw.forQuoted(from.toString());
        ColumnMetadata.Raw rawTo = ColumnMetadata.Raw.forQuoted(to.toString());

        return new ViewMetadata(baseTableId,
                                baseTableName,
                                includeAllColumns,
                                whereClause.renameIdentifier(rawFrom, rawTo),
                                metadata.unbuild().renamePrimaryKeyColumn(from, to).build());
    }

    public ViewMetadata withAddedRegularColumn(ColumnMetadata column)
    {
        return new ViewMetadata(baseTableId,
                                baseTableName,
                                includeAllColumns,
                                whereClause,
                                metadata.unbuild().addColumn(column).build());
    }
}
