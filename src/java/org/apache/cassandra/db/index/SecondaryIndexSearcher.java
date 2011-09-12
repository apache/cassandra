/**
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
package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;

public abstract class SecondaryIndexSearcher
{
    protected SecondaryIndexManager    indexManager;
    protected Set<ByteBuffer> columns;
    protected ColumnFamilyStore baseCfs;
    
    public SecondaryIndexSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
    {
        this.indexManager = indexManager;
        this.columns = columns;
        this.baseCfs = indexManager.baseCfs;
    }
    
    public static boolean satisfies(ColumnFamily data, IndexClause clause, IndexExpression first)
    {
        // We enforces even the primary clause because reads are not synchronized with writes and it is thus possible to have a race
        // where the index returned a row which doesn't have the primarycolumn when we actually read it
        for (IndexExpression expression : clause.expressions)
        {
            // check column data vs expression
            IColumn column = data.getColumn(expression.column_name);
            if (column == null)
                return false;
            int v = data.metadata().getValueValidator(expression.column_name).compare(column.value(), expression.value);
            if (!satisfies(v, expression.op))
                return false;
        }
        return true;
    }

    public static boolean satisfies(int comparison, IndexOperator op)
    {
        switch (op)
        {
            case EQ:
                return comparison == 0;
            case GTE:
                return comparison >= 0;
            case GT:
                return comparison > 0;
            case LTE:
                return comparison <= 0;
            case LT:
                return comparison < 0;
            default:
                throw new IllegalStateException();
        }
    }
    
    public NamesQueryFilter getExtraFilter(IndexClause clause)
    {
        SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(baseCfs.getComparator());
        for (IndexExpression expr : clause.expressions)
        {
            columns.add(expr.column_name);
        }
        return new NamesQueryFilter(columns);
    }
    
    
    public abstract List<Row> search(IndexClause clause, AbstractBounds range, IFilter dataFilter);
}
