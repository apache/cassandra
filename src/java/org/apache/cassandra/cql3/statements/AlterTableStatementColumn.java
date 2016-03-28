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

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;

public class AlterTableStatementColumn {
    private final CQL3Type.Raw dataType;
    private final ColumnIdentifier.Raw colName;
    private final Boolean isStatic;

    public AlterTableStatementColumn(ColumnIdentifier.Raw colName, CQL3Type.Raw dataType, boolean isStatic) {
        this.dataType = dataType;
        this.colName = colName;
        this.isStatic = isStatic;
    }

    public AlterTableStatementColumn(ColumnIdentifier.Raw colName, CQL3Type.Raw dataType) {
        this(colName, dataType,false );
    }

    public AlterTableStatementColumn(ColumnIdentifier.Raw colName) {
        this(colName, null, false);
    }

    public CQL3Type.Raw getColumnType() {
        return dataType;
    }

    public ColumnIdentifier.Raw getColumnName() {
        return colName;
    }

    public Boolean getStaticType() {
        return isStatic;
    }
}
