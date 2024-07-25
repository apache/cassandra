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
import java.util.ArrayList;
import java.util.Collections;
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
     * Checks if this restriction operator is a CONTAINS, CONTAINS_KEY, NOT_CONTAINS, NOT_CONTAINS_KEY or is an equality on a map element.
     * @return {@code true} if the restriction operator is one of the contains operations, {@code false} otherwise.
     */
    public boolean isContains()
    {
        return operator == Operator.CONTAINS
                || operator == Operator.CONTAINS_KEY
                || operator == Operator.NOT_CONTAINS
                || operator == Operator.NOT_CONTAINS_KEY
                // TODO only map elements supported for now in restrictions
               || columnsExpression.isMapElementExpression();
    }

    @Override
    public boolean needsFilteringOrIndexing()
    {
        // The need for filtering or indexing is a combination of columns expression type and operator
        // Therefore, we have to take both into account.
        // TODO only map elements supported for now in restrictions
        return (columnsExpression.isMapElementExpression())
               || operator.requiresFilteringOrIndexingFor(columnsExpression.columnsKind());
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        if (columnsExpression.isMapElementExpression())
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
        assert operator == Operator.EQ || operator == Operator.IN || operator == Operator.ANN || operator == Operator.NEQ || operator == Operator.NOT_IN;
        return bindAndGetClusteringElements(options);
    }

    @Override
    public void restrict(RangeSet<ClusteringElements> rangeSet, QueryOptions options)
    {
        assert operator.isSlice() || operator == Operator.EQ;
        operator.restrict(rangeSet, bindAndGetClusteringElements(options));
    }

    private List<ClusteringElements> bindAndGetClusteringElements(QueryOptions options)
    {
        switch (columnsExpression.kind())
        {
            case SINGLE_COLUMN:
            case TOKEN:
                return bindAndGetSingleTermClusteringElements(options);
            case MULTI_COLUMN:
                return bindAndGetMultiTermClusteringElements(options);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private List<ClusteringElements> bindAndGetSingleTermClusteringElements(QueryOptions options)
    {
        List<ByteBuffer> values = bindAndGet(options);
        if (values.isEmpty())
            return Collections.emptyList();

        List<ClusteringElements> elements = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++)
            elements.add(ClusteringElements.of(columnsExpression.columnSpecification(), values.get(i)));
        return elements;
    }

    private List<ClusteringElements> bindAndGetMultiTermClusteringElements(QueryOptions options)
    {
        List<List<ByteBuffer>> values = bindAndGetElements(options);
        if (values.isEmpty())
            return Collections.emptyList();

        List<ClusteringElements> elements = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++)
            elements.add(ClusteringElements.of(columnsExpression.columns(), values.get(i)));
        return elements;
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

    private void validate(ByteBuffer buffer)
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

        ColumnMetadata column = firstColumn();
        switch (columnsExpression.kind())
        {
            case SINGLE_COLUMN:
                List<ByteBuffer> buffers = bindAndGet(options);
                if (operator == Operator.IN || operator == Operator.BETWEEN || operator == Operator.NOT_IN)
                {
                    filter.add(column, operator, multiInputOperatorValues(column, buffers));
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

                        filter.add(firstColumn(), Operator.IN, multiInputOperatorValues(firstColumn(), values));
                    }
                    else
                    {
                        throw invalidRequest("Multicolumn IN filters are not supported");
                    }
                }
                break;
            case ELEMENT:
                // TODO only map elements supported for now
                if (columnsExpression.isMapElementExpression())
                {
                    ByteBuffer key = columnsExpression.element(options);
                    if (key == null)
                        throw invalidRequest("Invalid null map key for column %s", firstColumn().name.toCQLString());
                    if (key == ByteBufferUtil.UNSET_BYTE_BUFFER)
                        throw invalidRequest("Invalid unset map key for column %s", firstColumn().name.toCQLString());
                    List<ByteBuffer> values = bindAndGet(options);
                    filter.addMapEquality(firstColumn(), key, operator, values.get(0));
                }
                break;
            default: throw new UnsupportedOperationException();
        }
    }

    private static ByteBuffer multiInputOperatorValues(ColumnMetadata column, List<ByteBuffer> values)
    {
        return ListType.getInstance(column.type, false).pack(values);
    }

    @Override
    public String toString()
    {
        return operator.buildCQLString(columnsExpression, values);
    }
}
