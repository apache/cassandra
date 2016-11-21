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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.OperationExecutionException;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Operation functions (Mathematics).
 *
 */
public final class OperationFcts
{
    private static enum OPERATION
    {
        ADDITION('+', "_add")
        {
            protected ByteBuffer execute(NumberType<?> resultType,
                                         NumberType<?> leftType,
                                         ByteBuffer left,
                                         NumberType<?> rightType,
                                         ByteBuffer right)
            {
                return resultType.add(leftType, left, rightType, right);
            }
        },
        SUBSTRACTION('-', "_substract")
        {
            protected ByteBuffer execute(NumberType<?> resultType,
                                         NumberType<?> leftType,
                                         ByteBuffer left,
                                         NumberType<?> rightType,
                                         ByteBuffer right)
            {
                return resultType.substract(leftType, left, rightType, right);
            }
        },
        MULTIPLICATION('*', "_multiply")
        {
            protected ByteBuffer execute(NumberType<?> resultType,
                                         NumberType<?> leftType,
                                         ByteBuffer left,
                                         NumberType<?> rightType,
                                         ByteBuffer right)
            {
                return resultType.multiply(leftType, left, rightType, right);
            }
        },
        DIVISION('/', "_divide")
        {
            protected ByteBuffer execute(NumberType<?> resultType,
                                         NumberType<?> leftType,
                                         ByteBuffer left,
                                         NumberType<?> rightType,
                                         ByteBuffer right)
            {
                return resultType.divide(leftType, left, rightType, right);
            }
        },
        MODULO('%', "_modulo")
        {
            protected ByteBuffer execute(NumberType<?> resultType,
                                         NumberType<?> leftType,
                                         ByteBuffer left,
                                         NumberType<?> rightType,
                                         ByteBuffer right)
            {
                return resultType.mod(leftType, left, rightType, right);
            }
        };

        /**
         * The operator symbol.
         */
        private final char symbol;

        /**
         * The name of the function associated to this operation
         */
        private final String functionName;

        private OPERATION(char symbol, String functionName)
        {
            this.symbol = symbol;
            this.functionName = functionName;
        }

        /**
         * Executes the operation between the specified operand.
         *
         * @param resultType the result ype of the operation
         * @param leftType the type of the left operand
         * @param left the left operand
         * @param rightType the type of the right operand
         * @param right the right operand
         * @return the operation result
         */
        protected abstract ByteBuffer execute(NumberType<?> resultType,
                                              NumberType<?> leftType,
                                              ByteBuffer left,
                                              NumberType<?> rightType,
                                              ByteBuffer right);

        /**
         * Returns the {@code OPERATOR} associated to the specified function.
         * @param functionName the function name
         * @return the {@code OPERATOR} associated to the specified function
         */
        public static OPERATION fromFunctionName(String functionName)
        {
            for (OPERATION operator : values())
            {
                if (operator.functionName.equals(functionName))
                    return operator;
            }
            return null;
        }

        /**
         * Returns the {@code OPERATOR} with the specified symbol.
         * @param functionName the function name
         * @return the {@code OPERATOR} with the specified symbol
         */
        public static OPERATION fromSymbol(char symbol)
        {
            for (OPERATION operator : values())
            {
                if (operator.symbol == symbol)
                    return operator;
            }
            return null;
        }
    }

    /**
     * The name of the function used to perform negations
     */
    public static final String NEGATION_FUNCTION_NAME = "_negate";

    public static Collection<Function> all()
    {
        List<Function> functions = new ArrayList<>();

        final NumberType<?>[] numericTypes = new NumberType[] { ByteType.instance,
                                                                ShortType.instance,
                                                                Int32Type.instance,
                                                                LongType.instance,
                                                                FloatType.instance,
                                                                DoubleType.instance,
                                                                DecimalType.instance,
                                                                IntegerType.instance,
                                                                CounterColumnType.instance};

        for (NumberType<?> left : numericTypes)
        {
            for (NumberType<?> right : numericTypes)
            {
                NumberType<?> returnType = returnType(left, right);
                functions.add(new OperationFunction(returnType, left, OPERATION.ADDITION, right));
                functions.add(new OperationFunction(returnType, left, OPERATION.SUBSTRACTION, right));
                functions.add(new OperationFunction(returnType, left, OPERATION.MULTIPLICATION, right));
                functions.add(new OperationFunction(returnType, left, OPERATION.DIVISION, right));
                functions.add(new OperationFunction(returnType, left, OPERATION.MODULO, right));
            }
            functions.add(new NegationFunction(left));
        }
        return functions;
    }

    /**
     * Checks if the function with the specified name is an operation.
     *
     * @param function the function name
     * @return {@code true} if the function is an operation, {@code false} otherwise.
     */
    public static boolean isOperation(FunctionName function)
    {
        return SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(function.keyspace)
                && OPERATION.fromFunctionName(function.name) != null;
    }

    /**
     * Checks if the function with the specified name is a negation.
     *
     * @param function the function name
     * @return {@code true} if the function is an negation, {@code false} otherwise.
     */
    public static boolean isNegation(FunctionName function)
    {
        return SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(function.keyspace)&& NEGATION_FUNCTION_NAME.equals(function.name);
    }

    /**
     * Returns the operator associated to the specified function.
     *
     * @return the operator associated to the specified function.
     */
    public static char getOperator(FunctionName function)
    {
        assert SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(function.keyspace);
        return OPERATION.fromFunctionName(function.name).symbol;
    }

    /**
     * Returns the name of the function associated to the specified operator.
     *
     * @param operator the operator
     * @return the name of the function associated to the specified operator
     */
    public static FunctionName getFunctionNameFromOperator(char operator)
    {
        return FunctionName.nativeFunction(OPERATION.fromSymbol(operator).functionName);
    }

    /**
     * Determine the return type for an operation between the specified types.
     *
     * @param left the type of the left operand
     * @param right the type of the right operand
     * @return the return type for an operation between the specified types
     */
    private static NumberType<?> returnType(NumberType<?> left, NumberType<?> right)
    {
        boolean isFloatingPoint = left.isFloatingPoint() || right.isFloatingPoint();
        int size = Math.max(size(left), size(right));
        return isFloatingPoint
             ? floatPointType(size)
             : integerType(size);
    }

    /**
     * Returns the number of bytes used to represent a value of this type.
     * @return the number of bytes used to represent a value of this type or {@code Integer.MAX} if the number of bytes
     * is not limited.
     */
    private static int size(NumberType<?> type)
    {
        int size = type.valueLengthIfFixed();

        if (size > 0)
            return size;

        if (type.isCounter())
            return LongType.instance.valueLengthIfFixed();

        return Integer.MAX_VALUE;
    }

    private static NumberType<?> floatPointType(int size)
    {
        switch (size)
        {
            case 4: return FloatType.instance;
            case 8: return DoubleType.instance;
            default: return DecimalType.instance;
        }
    }

    private static NumberType<?> integerType(int size)
    {
        switch (size)
        {
            case 1: return ByteType.instance;
            case 2: return ShortType.instance;
            case 4: return Int32Type.instance;
            case 8: return LongType.instance;
            default: return IntegerType.instance;
        }
    }

    /**
     * The class must not be instantiated.
     */
    private OperationFcts()
    {
    }

    /**
     * Function that execute operations.
     */
    private static class OperationFunction extends NativeScalarFunction
    {
        private final OPERATION operation;

        public OperationFunction(NumberType<?> returnType,
                                 NumberType<?> left,
                                 OPERATION operation,
                                 NumberType<?> right)
        {
            super(operation.functionName, returnType, left, right);
            this.operation = operation;
        }

        @Override
        public final String columnName(List<String> columnNames)
        {
            return String.format("%s %s %s", columnNames.get(0), getOperator(), columnNames.get(1));
        }

        public final ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            ByteBuffer left = parameters.get(0);
            ByteBuffer right = parameters.get(1);
            if (left == null || !left.hasRemaining() || right == null || !right.hasRemaining())
                return null;

            NumberType<?> leftType = (NumberType<?>) argTypes().get(0);
            NumberType<?> rightType = (NumberType<?>) argTypes().get(1);
            NumberType<?> resultType = (NumberType<?>) returnType();

            try
            {
                return operation.execute(resultType, leftType, left, rightType, right);
            }
            catch (Exception e)
            {
                throw OperationExecutionException.create(getOperator(), argTypes, e);
            }
        }

        /**
         * Returns the operator symbol.
         * @return the operator symbol
         */
        private char getOperator()
        {
            return operation.symbol;
        }
    }

    /**
     * Function that negate a number.
     */
    private static class NegationFunction extends NativeScalarFunction
    {
        public NegationFunction(NumberType<?> inputType)
        {
            super(NEGATION_FUNCTION_NAME, inputType, inputType);
        }

        @Override
        public final String columnName(List<String> columnNames)
        {
            return String.format("-%s", columnNames.get(0));
        }

        public final ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            ByteBuffer input = parameters.get(0);
            if (input == null)
                return null;

            NumberType<?> inputType = (NumberType<?>) argTypes().get(0);

            return inputType.negate(input);
        }
    }
}
