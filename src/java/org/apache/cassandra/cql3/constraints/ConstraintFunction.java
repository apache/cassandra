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

package org.apache.cassandra.cql3.constraints;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.apache.cassandra.cql3.Operator.EQ;
import static org.apache.cassandra.cql3.Operator.GT;
import static org.apache.cassandra.cql3.Operator.GTE;
import static org.apache.cassandra.cql3.Operator.LT;
import static org.apache.cassandra.cql3.Operator.LTE;
import static org.apache.cassandra.cql3.Operator.NEQ;

/**
 * Interface to be implemented by functions that are executed as part of CQL constraints.
 */
public abstract class ConstraintFunction
{
    public static final List<Operator> DEFAULT_FUNCTION_OPERATORS = List.of(EQ, NEQ, GTE, GT, LTE, LT);

    protected final ColumnIdentifier columnName;
    protected final String name;

    public ConstraintFunction(ColumnIdentifier columnName, String name)
    {
        this.columnName = columnName;
        this.name = name;
    }

    /**
     * Method that performs the actual condition test, executed during the write path.
     * It the test is not successful, it throws a {@link ConstraintViolationException}.
     */
    public void evaluate(AbstractType<?> valueType, Operator relationType, String term, ByteBuffer columnValue) throws ConstraintViolationException
    {
        if (columnValue.capacity() == 0)
            throw new ConstraintViolationException("Column value does not satisfy value constraint for column '" + columnName + "' as it is null.");

        internalEvaluate(valueType, relationType, term, columnValue);
    }

    /**
     * Internal evaluation method, by default called from {@link ConstraintFunction#evaluate(AbstractType, Operator, String, ByteBuffer)}.
     * {@code columnValue} is by default guaranteed to not represent CQL value of 'null'.
     */
    protected abstract void internalEvaluate(AbstractType<?> valueType, Operator relationType, String term, ByteBuffer columnValue);

    /**
     * Used mostly for unary functions which do not expect any relation type nor term.
     */
    public void evaluate(AbstractType<?> valueType, ByteBuffer columnValue) throws ConstraintViolationException
    {
        evaluate(valueType, null, null, columnValue);
    }

    /**
     * Method that validates that a condition is valid. This method is called when the CQL constraint is created to determine
     * if the CQL statement is valid or needs to be rejected as invalid throwing a {@link InvalidConstraintDefinitionException}
     */
    public abstract void validate(ColumnMetadata columnMetadata) throws InvalidConstraintDefinitionException;

    /**
     * Return operators this function supports. By default, it returns an empty list, modelling unary function.
     *
     * @return list of operators this function is allowed to have.
     */
    public List<Operator> getSupportedOperators()
    {
        return List.of();
    }

    /**
     * Tells what types of columns are supported by this constraint.
     * Returning null or empty list means that all types are supported.
     *
     * @return supported types for given constraint
     */
    public abstract List<AbstractType<?>> getSupportedTypes();
}
