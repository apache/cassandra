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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.functions.types.ParseUtils;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.lang.String.format;
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

    protected ColumnIdentifier columnName;
    protected final String name;
    protected final List<String> args;
    // args as propagated from cql
    protected final List<String> rawArgs;

    public ConstraintFunction(String name, List<String> args)
    {
        this.name = name;
        this.rawArgs = args;
        this.args = unquote(args);
    }

    public List<String> arguments()
    {
        return args;
    }

    /**
     * Method that performs the actual condition test, executed during the write path.
     * It the test is not successful, it throws a {@link ConstraintViolationException}.
     */
    public void evaluate(AbstractType<?> valueType, Operator relationType, String term, ByteBuffer columnValue) throws ConstraintViolationException
    {
        if (columnValue == ByteBufferUtil.EMPTY_BYTE_BUFFER)
            throw new ConstraintViolationException("Column value does not satisfy value constraint for column '" + columnName + "' as it is null.");
        else if (valueType.isEmptyValueMeaningless() && columnValue.capacity() == 0)
            throw new ConstraintViolationException("Column value does not satisfy value constraint for column '" + columnName + "' as it is empty.");

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
    public void validate(ColumnMetadata columnMetadata, String term) throws InvalidConstraintDefinitionException
    {
        maybeThrowOnNonEmptyArguments(name);
    }

    /**
     * Return operators this function supports. By default, it returns an empty list, modelling unary function.
     *
     * @return list of operators this function is allowed to have.
     */
    public abstract List<Operator> getSupportedOperators();

    /**
     * Tells what types of columns are supported by this constraint.
     * Returning null or empty list means that all types are supported.
     *
     * @return supported types for given constraint
     */
    public abstract List<AbstractType<?>> getSupportedTypes();

    /**
     * Tells whether implementation supports specifying arguments on its function.
     * <br>
     * In this case, this function will return "true"
     * <pre>
     *     val int check length() < 1024
     * </pre>
     *
     * In this case, this function will return "false"
     * <pre>
     *     val int check someconstraint('abc', 'def')
     * </pre>
     * @return true if this constraint does not accept any parameters, false otherwise.
     */
    public boolean isParameterless() { return true; }

    @Override
    public String toString()
    {
        return name;
    }

    protected void maybeThrowOnNonEmptyArguments(String constraintName)
    {
        if (!isParameterless())
            return;

        if (args != null && !args.isEmpty())
            throw new InvalidConstraintDefinitionException(format("Constraint %s does not accept any arguments.", constraintName));
    }

    private List<String> unquote(List<String> quotedArgs)
    {
        List<String> unquotedArgs = new ArrayList<>();
        for (String quotedArg : quotedArgs)
            unquotedArgs.add(ParseUtils.unquote(quotedArg));

        return unquotedArgs;
    }
}
