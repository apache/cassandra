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
package org.apache.cassandra.cql3.restrictions;

import java.util.Objects;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.TableMetadata;

public class CustomIndexExpression
{
    private final ColumnIdentifier valueColId = new ColumnIdentifier("custom index expression", false);

    public final QualifiedName targetIndex;
    public final Term.Raw valueRaw;

    private Term value;

    public CustomIndexExpression(QualifiedName targetIndex, Term.Raw value)
    {
        this.targetIndex = targetIndex;
        this.valueRaw = value;
    }

    public void prepareValue(TableMetadata table, AbstractType<?> expressionType, VariableSpecifications boundNames)
    {
        ColumnSpecification spec = new ColumnSpecification(table.keyspace, table.keyspace, valueColId, expressionType);
        value = valueRaw.prepare(table.keyspace, spec);
        value.collectMarkerSpecification(boundNames);
    }

    public void addToRowFilter(RowFilter filter, TableMetadata table, QueryOptions options)
    {
        filter.addCustomIndexExpression(table,
                                        table.indexes
                                             .get(targetIndex.getName())
                                             .orElseThrow(() -> IndexRestrictions.indexNotFound(targetIndex, table)),
                                        value.bindAndGet(options));
    }

    /**
     * Returns whether this expression would need filtering if the specified index group were used.
     *
     * @param indexGroup an index group
     * @return {@code true} if this would need filtering if {@code indexGroup} were used, {@code false} otherwise
     */
    public boolean needsFiltering(Index.Group indexGroup)
    {
        String indexName = targetIndex.getName();
        for (Index index : indexGroup.getIndexes())
        {
            if (index.getIndexMetadata().name.equals(indexName))
                return false;
        }
        return true;
    }

    public String toCQLString()
    {
        return String.format("expr(%s,%s)", targetIndex.toCQLString(), valueRaw.getText());
    }

    @Override
    public String toString()
    {
        return toCQLString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(targetIndex, valueRaw);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CustomIndexExpression))
            return false;

        CustomIndexExpression cie = (CustomIndexExpression) o;
        return targetIndex.equals(cie.targetIndex) && valueRaw.equals(cie.valueRaw);
    }
}
