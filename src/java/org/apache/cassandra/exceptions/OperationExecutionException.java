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
package org.apache.cassandra.exceptions;

import java.util.List;

import org.apache.cassandra.cql3.functions.OperationFcts;
import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Thrown when an operation problem has occured (e.g. division by zero with integer).
 */
public final class OperationExecutionException extends FunctionExecutionException
{

    /**
     * Creates a new <code>OperationExecutionException</code> for the specified operation.
     *
     * @param operator the operator
     * @param argTypes the argument types
     * @param e the original Exception
     * @return a new <code>OperationExecutionException</code> for the specified operation
     */
    public static OperationExecutionException create(char operator, List<AbstractType<?>> argTypes, Exception e)
    {
        List<String> cqlTypes = AbstractType.asCQLTypeStringList(argTypes);
        String msg = String.format("the operation '%s %s %s' failed: %s", cqlTypes.get(0), operator, cqlTypes.get(1), e.getMessage());
        return new OperationExecutionException(operator, cqlTypes, msg);
    }

    /**
     * Creates an <code>OperationExecutionException</code> with the specified message.
     * @param operator the operator
     * @param argTypes the argument types
     * @param msg the error message
     */
    public OperationExecutionException(char operator, List<String> argTypes, String msg)
    {
        super(OperationFcts.getFunctionNameFromOperator(operator), argTypes, msg);
    }

}
