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

package org.apache.cassandra.cql3.transactions;

import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

public class SelectReferenceSource implements RowDataReference.ReferenceSource
{
    public static final String COLUMN_NOT_IN_SELECT_MESSAGE = "%s refererences a column not included in the select";
    private final SelectStatement statement;
    private final Set<ColumnMetadata> selectedColumns;
    private final TableMetadata metadata;

    public SelectReferenceSource(SelectStatement statement)
    {
        this.statement = statement;
        this.metadata = statement.table;
        Selection selection = statement.getSelection();
        selectedColumns = new HashSet<>(selection.getColumns());
    }

    @Override
    public boolean isPointSelect()
    {
        return statement.getRestrictions().hasAllPKColumnsRestrictedByEqualities()
               || statement.getLimit(QueryOptions.DEFAULT) == 1;
    }

    @Override
    public ColumnMetadata getColumn(String name)
    {
        ColumnMetadata column = metadata.getColumn(new ColumnIdentifier(name, true));
        if (column != null)
            // TODO: test case around this
            checkTrue(selectedColumns.contains(column), COLUMN_NOT_IN_SELECT_MESSAGE, statement);
        return column;
    }
}