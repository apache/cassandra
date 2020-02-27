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

import org.apache.cassandra.cql3.functions.types.TupleValue;
import org.apache.cassandra.cql3.functions.types.UDTValue;

/**
 * Provides context information for a particular user defined function.
 * Java UDFs can access implementations of this interface using the
 * {@code udfContext} field, scripted UDFs can get it using the {@code udfContext}
 * binding.
 */
public interface UDFContext
{
    /**
     * Creates a new {@code UDTValue} instance for an argument.
     *
     * @param argName name of the argument as declared in the {@code CREATE FUNCTION} statement
     * @return a new {@code UDTValue} instance
     * @throws IllegalArgumentException if no argument for the given name exists
     * @throws IllegalStateException    if the argument is not a UDT
     */
    UDTValue newArgUDTValue(String argName);

    /**
     * Creates a new {@code UDTValue} instance for an argument.
     *
     * @param argNum zero-based index of the argument as declared in the {@code CREATE FUNCTION} statement
     * @return a new {@code UDTValue} instance
     * @throws ArrayIndexOutOfBoundsException if no argument for the given index exists
     * @throws IllegalStateException          if the argument is not a UDT
     */
    UDTValue newArgUDTValue(int argNum);

    /**
     * Creates a new {@code UDTValue} instance for the return value.
     *
     * @return a new {@code UDTValue} instance
     * @throws IllegalStateException          if the return type is not a UDT
     */
    UDTValue newReturnUDTValue();

    /**
     * Creates a new {@code UDTValue} instance by name in the same keyspace.
     *
     * @param udtName name of the user defined type in the same keyspace as the function
     * @return a new {@code UDTValue} instance
     * @throws IllegalArgumentException if no UDT for the given name exists
     */
    UDTValue newUDTValue(String udtName);

    /**
     * Creates a new {@code TupleValue} instance for an argument.
     *
     * @param argName name of the argument as declared in the {@code CREATE FUNCTION} statement
     * @return a new {@code TupleValue} instance
     * @throws IllegalArgumentException if no argument for the given name exists
     * @throws IllegalStateException    if the argument is not a tuple
     */
    TupleValue newArgTupleValue(String argName);

    /**
     * Creates a new {@code TupleValue} instance for an argument.
     *
     * @param argNum zero-based index of the argument as declared in the {@code CREATE FUNCTION} statement
     * @return a new {@code TupleValue} instance
     * @throws ArrayIndexOutOfBoundsException if no argument for the given index exists
     * @throws IllegalStateException          if the argument is not a tuple
     */
    TupleValue newArgTupleValue(int argNum);

    /**
     * Creates a new {@code TupleValue} instance for the return value.
     *
     * @return a new {@code TupleValue} instance
     * @throws IllegalStateException          if the return type is not a tuple
     */
    TupleValue newReturnTupleValue();

    /**
     * Creates a new {@code TupleValue} instance for the CQL type definition.
     *
     * @param cqlDefinition CQL tuple type definition like {@code tuple<int, text, bigint>}
     * @return a new {@code TupleValue} instance
     * @throws IllegalStateException          if cqlDefinition type is not a tuple or an invalid type
     */
    TupleValue newTupleValue(String cqlDefinition);
}
