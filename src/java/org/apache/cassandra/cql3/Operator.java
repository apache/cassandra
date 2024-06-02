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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.RangeSet;

import org.apache.cassandra.cql3.restrictions.ClusteringElements;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public enum Operator
{
    EQ(0)
    {
        @Override
        public String toString()
        {
            return "=";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return type.compareForCQL(leftOperand, rightOperand) == 0;
        }

        @Override
        public boolean requiresFilteringOrIndexingFor(ColumnMetadata.Kind columnKind)
        {
            return !columnKind.isPrimaryKeyKind();
        }

        @Override
        public void restrict(RangeSet<ClusteringElements> rangeSet, List<ClusteringElements> args)
        {
            assert args.size() == 1 : this + " accept only one single value";
            ClusteringElements arg = args.get(0);
            rangeSet.removeAll(ClusteringElements.lessThan(arg));
            rangeSet.removeAll(ClusteringElements.greaterThan(arg));
        }

        @Override
        public Operator negate()
        {
            return NEQ;
        }

        @Override
        public boolean canBeUsedWith(ColumnsExpression.Kind kind)
        {
            return true;
        }
    },
    LT(4)
    {
        @Override
        public String toString()
        {
            return "<";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return type.compareForCQL(leftOperand, rightOperand) < 0;
        }

        @Override
        public boolean requiresFilteringOrIndexingFor(ColumnMetadata.Kind columnKind)
        {
            return columnKind != ColumnMetadata.Kind.CLUSTERING;
        }

        @Override
        public void restrict(RangeSet<ClusteringElements> rangeSet, List<ClusteringElements> args)
        {
            assert args.size() == 1 : this + " accept only one single value";
            rangeSet.removeAll(ClusteringElements.atLeast(args.get(0)));
        }

        @Override
        public Operator negate()
        {
            return GTE;
        }

        @Override
        public boolean isSlice()
        {
            return true;
        }

        @Override
        public boolean canBeUsedWith(ColumnsExpression.Kind kind)
        {
            return kind != ColumnsExpression.Kind.MAP_ELEMENT;
        }
    },
    LTE(3)
    {
        @Override
        public String toString()
        {
            return "<=";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return type.compareForCQL(leftOperand, rightOperand) <= 0;
        }

        @Override
        public boolean requiresFilteringOrIndexingFor(ColumnMetadata.Kind columnKind)
        {
            return columnKind != ColumnMetadata.Kind.CLUSTERING;
        }

        @Override
        public void restrict(RangeSet<ClusteringElements> rangeSet, List<ClusteringElements> args)
        {
            assert args.size() == 1 : this + " accept only one single value";
            rangeSet.removeAll(ClusteringElements.greaterThan(args.get(0)));
        }

        @Override
        public Operator negate()
        {
            return GT;
        }

        @Override
        public boolean isSlice()
        {
            return true;
        }

        @Override
        public boolean canBeUsedWith(ColumnsExpression.Kind kind)
        {
            return kind != ColumnsExpression.Kind.MAP_ELEMENT;
        }
    },
    GTE(1)
    {
        @Override
        public String toString()
        {
            return ">=";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return type.compareForCQL(leftOperand, rightOperand) >= 0;
        }

        @Override
        public boolean requiresFilteringOrIndexingFor(ColumnMetadata.Kind columnKind)
        {
            return columnKind != ColumnMetadata.Kind.CLUSTERING;
        }

        @Override
        public void restrict(RangeSet<ClusteringElements> rangeSet, List<ClusteringElements> args)
        {
            assert args.size() == 1 : this + " accept only one single value";
            rangeSet.removeAll(ClusteringElements.lessThan(args.get(0)));
        }

        @Override
        public Operator negate()
        {
            return LT;
        }

        @Override
        public boolean isSlice()
        {
            return true;
        }

        @Override
        public boolean canBeUsedWith(ColumnsExpression.Kind kind)
        {
            return kind != ColumnsExpression.Kind.MAP_ELEMENT;
        }
    },
    GT(2)
    {
        @Override
        public String toString()
        {
            return ">";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return type.compareForCQL(leftOperand, rightOperand) > 0;
        }

        @Override
        public boolean requiresFilteringOrIndexingFor(ColumnMetadata.Kind columnKind)
        {
            return columnKind != ColumnMetadata.Kind.CLUSTERING;
        }

        @Override
        public void restrict(RangeSet<ClusteringElements> rangeSet, List<ClusteringElements> args)
        {
            assert args.size() == 1 : this + " accept only one single value";
            rangeSet.removeAll(ClusteringElements.atMost(args.get(0)));
        }

        @Override
        public Operator negate()
        {
            return LTE;
        }

        @Override
        public boolean isSlice()
        {
            return true;
        }

        @Override
        public boolean canBeUsedWith(ColumnsExpression.Kind kind)
        {
            return kind != ColumnsExpression.Kind.MAP_ELEMENT;
        }
    },
    IN(7)
    {
        @Override
        public Kind kind() {
            return Kind.MULTI_VALUE;
        }

        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            ListSerializer<?> serializer = ListType.getInstance(type, false).getSerializer();
            return serializer.anyMatch(rightOperand, r -> type.compareForCQL(leftOperand, r) == 0);
        }

        @Override
        public boolean requiresFilteringOrIndexingFor(ColumnMetadata.Kind columnKind)
        {
            return !columnKind.isPrimaryKeyKind();
        }

        @Override
        public boolean canBeUsedWith(ColumnsExpression.Kind kind)
        {
            return kind == ColumnsExpression.Kind.SINGLE_COLUMN || kind == ColumnsExpression.Kind.MULTI_COLUMN;
        }
    },
    CONTAINS(5)
    {
        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            switch(((CollectionType<?>) type).kind)
            {
                case LIST:
                    ListType<?> listType = (ListType<?>) type;
                    return listType.compose(leftOperand).contains(listType.getElementsType().compose(rightOperand));
                case SET:
                    SetType<?> setType = (SetType<?>) type;
                    return setType.compose(leftOperand).contains(setType.getElementsType().compose(rightOperand));
                case MAP:
                    MapType<?, ?> mapType = (MapType<?, ?>) type;
                    return mapType.compose(leftOperand).containsValue(mapType.getValuesType().compose(rightOperand));
            }
            throw new AssertionError();
        }

        @Override
        public boolean isSatisfiedBy(CollectionType<?> type, ComplexColumnData leftOperand, ByteBuffer rightOperand)
        {
            for (Cell<?> cell : leftOperand)
            {
                if (type.kind == CollectionType.Kind.SET)
                {
                    if (type.nameComparator().compare(cell.path().get(0), rightOperand) == 0)
                        return true;
                }
                else
                {
                    if (type.valueComparator().compare(cell.buffer(), rightOperand) == 0)
                        return true;
                }
            }
            return false;
        }

        @Override
        public boolean appliesToColumnValues()
        {
            return false;
        }

        @Override
        public boolean appliesToCollectionElements()
        {
            return true;
        }
    },
    CONTAINS_KEY(6)
    {
        @Override
        public String toString()
        {
            return "CONTAINS KEY";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            MapType<?, ?> mapType = (MapType<?, ?>) type;
            return mapType.compose(leftOperand).containsKey(mapType.getKeysType().compose(rightOperand));
        }

        @Override
        public boolean isSatisfiedBy(CollectionType<?> type, ComplexColumnData leftOperand, ByteBuffer rightOperand)
        {
            return leftOperand.getCell(CellPath.create(rightOperand)) != null;
        }

        @Override
        public boolean appliesToColumnValues()
        {
            return false;
        }

        @Override
        public boolean appliesToMapKeys()
        {
            return true;
        }
    },
    NEQ(8)
    {
        @Override
        public String toString()
        {
            return "!=";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return type.compareForCQL(leftOperand, rightOperand) != 0;
        }

        @Override
        public boolean requiresFilteringOrIndexingFor(ColumnMetadata.Kind columnKind)
        {
            return !columnKind.isPrimaryKeyKind();
        }

        @Override
        public Operator negate()
        {
            return EQ;
        }

        @Override
        protected boolean isSupportedByReadPath()
        {
            return false;
        }
    },
    IS_NOT(9)
    {
        @Override
        public String toString()
        {
            return "IS NOT";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected boolean isSupportedByReadPath()
        {
            return false;
        }
    },
    LIKE_PREFIX(10)
    {
        @Override
        public String toString()
        {
            return "LIKE '<term>%'";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return ByteBufferUtil.startsWith(leftOperand, rightOperand);
        }
    },
    LIKE_SUFFIX(11)
    {
        @Override
        public String toString()
        {
            return "LIKE '%<term>'";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return ByteBufferUtil.endsWith(leftOperand, rightOperand);
        }
    },
    LIKE_CONTAINS(12)
    {
        @Override
        public String toString()
        {
            return "LIKE '%<term>%'";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return ByteBufferUtil.contains(leftOperand, rightOperand);
        }
    },
    LIKE_MATCHES(13)
    {
        @Override
        public String toString()
        {
            return "LIKE '<term>'";
        }

        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            return ByteBufferUtil.contains(leftOperand, rightOperand);
        }
    },
    LIKE(14)
    {
        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean requiresIndexing()
        {
            return true;
        }
    },
    ANN(15)
    {
        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            // The ANN operator is only supported by the vector index so, normally, should never be called directly.
            // In networked queries (non-local) the coordinator will end up calling the row filter directly. So, this
            // needs to return true so that the returned values are allowed through to the VectorTopKProcessor
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean requiresIndexing()
        {
            return true;
        }
    },
    BETWEEN(16)
    {
        final Comparator<ClusteringElements> comparator = new ClusteringElements.ClusteringElementsComparator(true);

        @Override
        public Kind kind() {
            return Kind.TERNARY;
        }

        @Override
        public String toString()
        {
            return "BETWEEN";
        }

        @Override
        public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand)
        {
            List<ByteBuffer> buffers = ListType.getInstance(type, false).unpack(rightOperand);
            buffers.sort(type);
            return type.compareForCQL(leftOperand, buffers.get(1)) <= 0 && type.compareForCQL(leftOperand, buffers.get(0)) >= 0;
        }

        @Override
        public boolean requiresFilteringOrIndexingFor(ColumnMetadata.Kind columnKind)
        {
            return !columnKind.isPrimaryKeyKind();
        }

        @Override
        public void restrict(RangeSet<ClusteringElements> rangeSet, List<ClusteringElements> args)
        {
            assert args.size() == 2 : this + " accepts exactly two values";
            args.sort(comparator);
            rangeSet.removeAll(ClusteringElements.lessThan(args.get(0)));
            rangeSet.removeAll(ClusteringElements.greaterThan(args.get(1)));
        }

        @Override
        public boolean isSlice()
        {
            return true;
        }

        @Override
        public boolean canBeUsedWith(ColumnsExpression.Kind kind)
        {
            return kind != ColumnsExpression.Kind.MAP_ELEMENT;
        }
    };

    /**
     * The different kinds of operators
     */
    public enum Kind
    {
        BINARY, TERNARY, MULTI_VALUE;
    };

    /**
     * The binary representation of this <code>Enum</code> value.
     */
    private final int b;

    /**
     * Creates a new {@code Operator} with the specified binary representation.
     * @param b the binary representation of this {@code Enum} value
     */
    Operator(int b)
    {
        this.b = b;
    }

    /**
     * Write the serialized version of this <code>Operator</code> to the specified output.
     *
     * @param output the output to write to
     * @throws IOException if an I/O problem occurs while writing to the specified output
     */
    public void writeTo(DataOutput output) throws IOException
    {
        output.writeInt(getValue());
    }

    public int getValue()
    {
        return b;
    }

    /**
     * Returns the kind of this operator.
     * @return the kind of this operator
     */
    public Kind kind()
    {
        return Kind.BINARY;
    }

    /**
     * Checks if this operator is a ternary operator.
     * @return {@code true} if this operator is a ternary operator, {@code false} otherwise.
     */
    public boolean isTernary()
    {
        return kind() == Kind.TERNARY;
    }

    /**
     * Deserializes a <code>Operator</code> instance from the specified input.
     *
     * @param input the input to read from
     * @return the <code>Operator</code> instance deserialized
     * @throws IOException if a problem occurs while deserializing the <code>Type</code> instance.
     */
    public static Operator readFrom(DataInput input) throws IOException
    {
          int b = input.readInt();
          for (Operator operator : values())
              if (operator.b == b)
                  return operator;

          throw new IOException(String.format("Cannot resolve Relation.Type from binary representation: %s", b));
    }


    /**
     * Whether 2 values satisfy this operator (given the type they should be compared with).
     */
    public abstract boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand);


    public boolean isSatisfiedBy(CollectionType<?> type, ComplexColumnData leftOperand, ByteBuffer rightOperand)
    {
        throw new UnsupportedOperationException();
    }

    public static int serializedSize()
    {
        return 4;
    }

    public void validateFor(ColumnsExpression expression)
    {
        if (!canBeUsedWith(expression.kind()))
            throw invalidRequest("%s cannot be used with %s relations", this, expression);

        switch (expression.kind())
        {
            case SINGLE_COLUMN:
                ColumnMetadata firstColumn = expression.firstColumn();
                AbstractType<?> columnType = firstColumn.type;
                if (isSlice())
                {
                    if (columnType.referencesDuration())
                    {
                        checkFalse(columnType.isCollection(), "Slice restrictions are not supported on collections containing durations");
                        checkFalse(columnType.isTuple(), "Slice restrictions are not supported on tuples containing durations");
                        checkFalse(columnType.isUDT(), "Slice restrictions are not supported on UDTs containing durations");
                        throw invalidRequest("Slice restrictions are not supported on duration columns");
                    }
                }
                else
                {
                    checkFalse(appliesToMapKeys() && !(columnType instanceof MapType), "Cannot use %s on non-map column %s", this, firstColumn.name);
                    checkFalse(appliesToCollectionElements() && !columnType.isCollection(), "Cannot use %s on non-collection column %s", this, firstColumn.name);
                }

            case MAP_ELEMENT:
                ColumnMetadata column = expression.firstColumn();
                AbstractType<?> type = column.type;
                if (type.isMultiCell())
                {
                    // Non-frozen UDTs don't support any operator
                    checkFalse(type.isUDT(),
                               "Non-frozen UDT column '%s' (%s) cannot be restricted by any relation",
                               column.name,
                               type.asCQL3Type());

                    // We don't support relations against entire collections (unless they're frozen), like "numbers = {1, 2, 3}"
                    checkFalse(type.isCollection()
                                    && !this.appliesToMapKeys()
                                    && !this.appliesToCollectionElements()
                                    && expression.kind() != ColumnsExpression.Kind.MAP_ELEMENT,
                               "Collection column '%s' (%s) cannot be restricted by a '%s' relation",
                               column.name,
                               type.asCQL3Type(),
                               this);
                }
                break;
        }
    }

    /**
     * Checks if the specified expression kind can be used with this operator.
     * @param kind the expression kind
     * @return {@code true} if the specified expression kind can be used with this operator, {@code false} otherwise.
     */
    public boolean canBeUsedWith(ColumnsExpression.Kind kind)
    {
        // All operators support single columns
        return kind == ColumnsExpression.Kind.SINGLE_COLUMN;
    }

    /**
     * Checks if this operator applies to non-multicell column values.
     * @return {@code true} if this operator applies to column values, {@code false} otherwise.
     */
    public boolean appliesToColumnValues()
    {
        return true;
    }

    /**
     * Checks if this operator applies to collection elements (from frozen and non-frozen collections).
     * @return {@code true} if this operator applies to collection elements, {@code false} otherwise.
     */
    public boolean appliesToCollectionElements()
    {
        return false;
    }

    /**
     * Checks if this operator applies to map keys.
     * @return {@code true} if this operator applies to map keys, {@code false} otherwise.
     */
    public boolean appliesToMapKeys()
    {
        return false;
    }

    /**
     * Restricts the specified range set based on the operator arguments (optional operation).
     * @param rangeSet the range set to restrict
     * @param args the operator arguments
     */
    public void restrict(RangeSet<ClusteringElements> rangeSet, List<ClusteringElements> args)
    {
        throw new UnsupportedOperationException(this + " is not a range operator");
    }

    /**
     * Checks if this operator <b>requires</b> either filtering or indexing for the specified columns kinds.
     * <p>An operator requires filtering or indexing only if it cannot be executed by other means.
     * An equal operator on a clustering column for example will return {@code false} even if filtering might be used
     * because the previous clustering column is not restricted.</p>
     *
     * @param columnKind the kind of column being restricted by the operator
     * @return {@code true} if this operator requires either filtering or indexing, {@code false} otherwise.
     */
    public boolean requiresFilteringOrIndexingFor(ColumnMetadata.Kind columnKind)
    {
        return true;
    }

    /**
     * Checks if this operator requires a secondary index.
     * @return {@code true} if this operator requires a secondary index, {@code false} otherwise.
     */
    public boolean requiresIndexing()
    {
        return false;
    }

    /**
     * Checks if this operator returning a slice of the data.
     * @return {@code true} if this operator is a slice operator, {@code false} otherwise.
     */
    public boolean isSlice()
    {
        return false;
    }

    @Override
    public String toString()
    {
         return this.name();
    }

    /**
     * Checks if this operator is an IN operator.
     * @return {@code true} if this operator is an IN operator, {@code false} otherwise.
     */
    public boolean isIN()
    {
        return this == IN;
    }

    /**
     * Reverse this operator.
     * @return the reverse operator from this operator.
     */
    public Operator negate()
    {
        throw new UnsupportedOperationException(this + " does not support negation");
    }

    /**
     * Some operators are not supported by the read path because we never fully implemented support for them.
     * It is the case for {@code IS_NOT} and {@code !=}
     * @return {@code true} for the operators supported by the read path, {@code false} otherwise.
     */
    protected boolean isSupportedByReadPath()
    {
        return true;
    }

    /**
     * The "LIKE_" operators are not real CQL operators and are simply an internal hack that should be removed at some point.
     * Therefore, we want to ignore them in the error messages returned to the users.
     * @return {@code true} for the "LIKE_" operators
     */
    private boolean isLikeVariant()
    {
        return this == LIKE_CONTAINS || this == LIKE_PREFIX || this == LIKE_MATCHES || this == LIKE_SUFFIX;
    }

    /**
     * Returns the operators that require an index or filtering for the specified column kind
     * @param columnKind the column kind
     * @return the operators that require an index or filtering for the specified column kind
     */
    public static List<Operator> operatorsRequiringFilteringOrIndexingFor(ColumnMetadata.Kind columnKind)
    {
        return Arrays.stream(values())
                     .filter(o -> o.isSupportedByReadPath() && !o.isLikeVariant() && o.requiresFilteringOrIndexingFor(columnKind))
                     .collect(Collectors.toList());
    }
}
