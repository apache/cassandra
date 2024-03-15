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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.RangeSet;

import org.apache.cassandra.cql3.ColumnsExpression;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.cql3.terms.Terms;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A simple predicate on a columns expression (e.g. columnA = X).
 */
public final class SimpleRestriction implements SingleRestriction
{
    /**
     * The columns expression to which the restriction applies.
     */
    private final ColumnsExpression columnsExpression;

    /**
     * The operator
     */
    private final Operator operator;

    /**
     * The values
     */
    private final Terms values;

    public SimpleRestriction(ColumnsExpression columnsExpression, Operator operator, Terms values)
    {
        this.columnsExpression = columnsExpression;
        this.operator = operator;
        this.values = values;
    }

    @Override
    public boolean isOnToken()
    {
        return columnsExpression.kind() == ColumnsExpression.Kind.TOKEN;
    }

    @Override
    public ColumnMetadata firstColumn()
    {
        return columnsExpression.firstColumn();
    }

    @Override
    public ColumnMetadata lastColumn()
    {
        return columnsExpression.lastColumn();
    }

    @Override
    public List<ColumnMetadata> columns()
    {
        return columnsExpression.columns();
    }

    @Override
    public boolean isMultiColumn()
    {
        return columnsExpression.kind() == ColumnsExpression.Kind.MULTI_COLUMN;
    }

    @Override
    public boolean isColumnLevel()
    {
        return columnsExpression.isColumnLevelExpression();
    }

    public Operator operator()
    {
        return operator;
    }

    @Override
    public boolean isANN()
    {
        return operator == Operator.ANN;
    }

    @Override
    public boolean isEQ()
    {
        return operator == Operator.EQ;
    }

    @Override
    public boolean isSlice()
    {
        return operator.isSlice();
    }

    @Override
    public boolean isIN()
    {
        return operator == Operator.IN;
    }

    /**
     * Checks if this restriction operator is a CONTAINS, CONTAINS_KEY or is an equality on a map element.
     * @return {@code true} if the restriction operator is one of the contains operations, {@code false} otherwise.
     */
    public boolean isContains()
    {
        return operator == Operator.CONTAINS
               || operator == Operator.CONTAINS_KEY
               || columnsExpression.kind() == ColumnsExpression.Kind.MAP_ELEMENT;
    }

    @Override
    public boolean needsFilteringOrIndexing()
    {
        // The need for filtering or indexing is a combination of columns expression type and operator
        // Therefore, we have to take both into account.
        return columnsExpression.kind() == ColumnsExpression.Kind.MAP_ELEMENT
               || operator.requiresFilteringOrIndexingFor(columnsExpression.columnsKind());
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        columnsExpression.addFunctionsTo(functions);
        values.addFunctionsTo(functions);
    }

    @Override
    public boolean needsFiltering(Index.Group indexGroup)
    {
        for (ColumnMetadata column : columns())
        {
            if (!isSupportedBy(indexGroup.getIndexes(), column))
                return true;
        }
        return false;
    }

    private boolean isSupportedBy(Iterable<Index> indexes, ColumnMetadata column)
    {
        if (isOnToken())
            return false;

        for (Index index : indexes)
        {
            if (index.supportsExpression(column, operator))
                return true;
        }
        return false;
    }

    @Override
    public Index findSupportingIndex(Iterable<Index> indexes)
    {
        if (isOnToken())
            return null;

        for (Index index : indexes)
            if (isSupportedBy(index))
                return index;
        return null;
    }

    @Override
    public boolean isSupportedBy(Index index)
    {
        if (isOnToken())
            return false;

        for (ColumnMetadata column : columns())
        {
            if (index.supportsExpression(column, operator))
                return true;
        }
        return false;
    }

    @Override
    public List<ClusteringElements> values(QueryOptions options)
    {
        assert operator == Operator.EQ || operator == Operator.IN || operator == Operator.ANN;
        return bindAndGetClusteringElements(options);
    }

    @Override
    public RangeSet<ClusteringElements> restrict(RangeSet<ClusteringElements> rangeSet, QueryOptions options)
    {
        assert operator.isSlice() || operator == Operator.EQ;
        return operator.restrict(rangeSet, bindAndGetClusteringElements(options));
    }

    private List<ClusteringElements> bindAndGetClusteringElements(QueryOptions options)
    {
        switch (columnsExpression.kind())
        {
            case SINGLE_COLUMN:
            case TOKEN:
                return bindAndGet(options).stream()
                                          .map(b ->  ClusteringElements.of(columnsExpression.columnSpecification(), b))
                                          .collect(Collectors.toList());
            case MULTI_COLUMN:
                return bindAndGetElements(options).stream()
                                                  .map(buffers -> ClusteringElements.of(columnsExpression.columns(), buffers))
                                                  .collect(Collectors.toList());
            default:
                throw new UnsupportedOperationException();
        }
    }

    private List<ByteBuffer> bindAndGet(QueryOptions options)
    {
        List<ByteBuffer> buffers = values.bindAndGet(options);
        validate(buffers);
        buffers.forEach(this::validate);
        return buffers;
    }

    private List<List<ByteBuffer>> bindAndGetElements(QueryOptions options)
    {
        List<List<ByteBuffer>> elementsList = values.bindAndGetElements(options);
        validate(elementsList);
        elementsList.forEach(this::validateElements);
        return elementsList;
    }

    private void validate(List<?> list)
    {
        if (list == null)
            throw invalidRequest("Invalid null value for %s", columnsExpression);
        if (list == Term.UNSET_LIST)
            throw invalidRequest("Invalid unset value for %s", columnsExpression);
    }

    private <T> void validate(ByteBuffer buffer)
    {
        if (buffer == null)
            throw invalidRequest("Invalid null value for %s", columnsExpression);
        if (buffer == ByteBufferUtil.UNSET_BYTE_BUFFER)
            throw invalidRequest("Invalid unset value for %s", columnsExpression);
    }

    private void validateElements(List<ByteBuffer> elements)
    {
        validate(elements);

        List<ColumnMetadata> columns = columns();
        for (int i = 0, m = columns.size(); i < m; i++)
        {
            ColumnMetadata column = columns.get(i);
            ByteBuffer element = elements.get(i);
            if (element == null)
                throw invalidRequest("Invalid null value for %s in %s",
                                     column.name.toCQLString(), columnsExpression);
            if (element == ByteBufferUtil.UNSET_BYTE_BUFFER)
                throw invalidRequest("Invalid unset value for %s in %s",
                                     column.name.toCQLString(), columnsExpression);
        }
    }

    @Override
    public void addToRowFilter(RowFilter filter, IndexRegistry indexRegistry, QueryOptions options)
    {
        if (isOnToken())
            throw new UnsupportedOperationException();

        switch (columnsExpression.kind())
        {
            case SINGLE_COLUMN:
                List<ByteBuffer> buffers = bindAndGet(options);

                ColumnMetadata column = firstColumn();
                if (operator == Operator.IN)
                {
                    filter.add(column, operator, inValues(column, buffers));
                }
                else if (operator == Operator.LIKE)
                {
                    LikePattern pattern = LikePattern.parse(buffers.get(0));
                    // there must be a suitable INDEX for LIKE_XXX expressions
                    RowFilter.SimpleExpression expression = filter.add(column, pattern.kind().operator(), pattern.value());
                    indexRegistry.getBestIndexFor(expression)
                                 .orElseThrow(() -> invalidRequest("%s is only supported on properly indexed columns",
                                                                   expression));
                }
                else
                {
                    filter.add(column, operator, buffers.get(0));
                }
                break;
            case MULTI_COLUMN:
                checkFalse(isSlice(), "Multi-column slice restrictions cannot be used for filtering.");

                if (isEQ())
                {
                    List<ByteBuffer> elements = bindAndGetElements(options).get(0);

                    for (int i = 0, m = columns().size(); i < m; i++)
                    {
                        ColumnMetadata columnDef = columns().get(i);
                        filter.add(columnDef, Operator.EQ, elements.get(i));
                    }
                }
                else if (isIN())
                {
                    // If the relation is of the type (c) IN ((x),(y),(z)) then it is equivalent to
                    // c IN (x, y, z) and we can perform filtering
                    if (columns().size() == 1)
                    {
                        List<ByteBuffer> values = bindAndGetElements(options).stream()
                                                                             .map(elements -> elements.get(0))
                                                                             .collect(Collectors.toList());

                        filter.add(firstColumn(), Operator.IN, inValues(firstColumn(), values));
                    }
                    else
                    {
                        throw invalidRequest("Multicolumn IN filters are not supported");
                    }
                }
                break;
            case MAP_ELEMENT:
                ByteBuffer key = columnsExpression.mapKey(options);
                List<ByteBuffer> values = bindAndGet(options);
                filter.addMapEquality(firstColumn(), key, Operator.EQ, values.get(0));
                break;
            default: throw new UnsupportedOperationException();
        }
    }

    private static ByteBuffer inValues(ColumnMetadata column, List<ByteBuffer> values)
    {
        return ListType.getInstance(column.type, false).pack(values);
    }
}
