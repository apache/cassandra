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

package org.apache.cassandra.cql3;

import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.restrictions.SingleRestriction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A single element of an ORDER BY clause.
 * <code>ORDER BY ordering1 [, ordering2 [, ...]] </code>
 * <p>
 * An ordering comprises an expression that produces the values to compare against each other
 * and a sorting direction (ASC, DESC).
 */
public class Ordering
{
    public final Expression expression;
    public final Direction direction;

    public Ordering(Expression expression, Direction direction)
    {
        this.expression = expression;
        this.direction = direction;
    }

    public static abstract class Expression
    {
        protected final ColumnMetadata columnMetadata;

        public Expression(ColumnMetadata columnMetadata)
        {
            this.columnMetadata = columnMetadata;
        }

        public boolean hasNonClusteredOrdering()
        {
            return false;
        }

        public SingleRestriction toRestriction()
        {
            throw new UnsupportedOperationException();
        }

        public ColumnMetadata getColumn()
        {
            return columnMetadata;
        }
    }

    /**
     * Represents a single column in <code>ORDER BY column</code>
     */
    public static class SingleColumn extends Expression
    {
        public SingleColumn(ColumnMetadata columnMetadata)
        {
            super(columnMetadata);
        }
    }

    /**
     * An expression used in Approximate Nearest Neighbor ordering. <code>ORDER BY column ANN OF value</code>
     */
    public static class Ann extends Expression
    {
        final Term vectorValue;

        public Ann(ColumnMetadata columnMetadata, Term vectorValue)
        {
            super(columnMetadata);
            this.vectorValue = vectorValue;
        }

        @Override
        public boolean hasNonClusteredOrdering()
        {
            return true;
        }

        @Override
        public SingleRestriction toRestriction()
        {
            return new SingleColumnRestriction.AnnRestriction(columnMetadata, vectorValue);
        }
    }

    public enum Direction
    {ASC, DESC}


    /**
     * Represents ANTLR's abstract syntax tree of a single element in the {@code ORDER BY} clause.
     * This comes directly out of CQL parser.
     */
    public static class Raw
    {

        final Expression expression;
        final Direction direction;

        public Raw(Expression expression, Direction direction)
        {
            this.expression = expression;
            this.direction = direction;
        }

        /**
         * Resolves column identifiers against the table schema.
         * Binds markers (?) to columns.
         */
        public Ordering bind(TableMetadata table, VariableSpecifications boundNames)
        {
            return new Ordering(expression.bind(table, boundNames), direction);
        }

        public interface Expression
        {
            Ordering.Expression bind(TableMetadata table, VariableSpecifications boundNames);
        }

        public static class SingleColumn implements Expression
        {
            final ColumnIdentifier column;

            SingleColumn(ColumnIdentifier column)
            {
                this.column = column;
            }

            @Override
            public Ordering.Expression bind(TableMetadata table, VariableSpecifications boundNames)
            {
                return new Ordering.SingleColumn(table.getExistingColumn(column));
            }
        }

        public static class Ann implements Expression
        {
            final ColumnIdentifier columnId;
            final Term.Raw vectorValue;

            Ann(ColumnIdentifier column, Term.Raw vectorValue)
            {
                this.columnId = column;
                this.vectorValue = vectorValue;
            }

            @Override
            public Ordering.Expression bind(TableMetadata table, VariableSpecifications boundNames)
            {
                ColumnMetadata column = table.getExistingColumn(columnId);
                Term value = vectorValue.prepare(table.keyspace, column);
                value.collectMarkerSpecification(boundNames);
                return new Ordering.Ann(column, value);
            }
        }
    }
}



