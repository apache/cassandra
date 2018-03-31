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
package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Stores a column name and optionally type for an Alter Table statement definition.
 *
 * This is used by AlterTableStatement to store the added, altered or dropped columns.
 */
public class AlterTableStatementColumn
{
    private final CQL3Type.Raw dataType;
    private final ColumnMetadata.Raw colName;
    private final Boolean isStatic;

    public AlterTableStatementColumn(ColumnMetadata.Raw colName, CQL3Type.Raw dataType, boolean isStatic)
    {
        assert colName != null;
        this.dataType = dataType; // will be null when dropping columns, and never null otherwise (for ADD and ALTER).
        this.colName = colName;
        this.isStatic = isStatic;
    }

    public AlterTableStatementColumn(ColumnMetadata.Raw colName, CQL3Type.Raw dataType)
    {
        this(colName, dataType, false);
    }

    public AlterTableStatementColumn(ColumnMetadata.Raw colName)
    {
        this(colName, null, false);
    }

    public CQL3Type.Raw getColumnType()
    {
        return dataType;
    }

    public ColumnMetadata.Raw getColumnName()
    {
        return colName;
    }

    public Boolean getStaticType()
    {
        return isStatic;
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
