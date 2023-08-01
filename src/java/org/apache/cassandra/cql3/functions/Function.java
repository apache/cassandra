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
import java.util.List;
import java.util.Optional;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.Difference;
import org.apache.cassandra.transport.ProtocolVersion;
import org.github.jamm.Unmetered;

@Unmetered
public interface Function extends AssignmentTestable
{
    /**
     * A marker buffer used to represent function parameters that cannot be resolved at some stage of CQL processing.
     * This is used for partial function application in particular.
     */
    public static final ByteBuffer UNRESOLVED = ByteBuffer.allocate(0);

    public FunctionName name();
    public List<AbstractType<?>> argTypes();
    public AbstractType<?> returnType();

    /**
     * Checks whether the function is a native/hard coded one or not.
     *
     * @return {@code true} if the function is a native/hard coded one, {@code false} otherwise.
     */
    public boolean isNative();

    /**
     * Checks whether the function is a pure function (as in doesn't depend on, nor produces side effects) or not.
     *
     * @return {@code true} if the function is a pure function, {@code false} otherwise.
     */
    public boolean isPure();

    /**
     * Checks whether the function is an aggregate function or not.
     *
     * @return {@code true} if the function is an aggregate function, {@code false} otherwise.
     */
    public boolean isAggregate();

    public void addFunctionsTo(List<Function> functions);

    public boolean referencesUserType(ByteBuffer name);

    /**
     * Returns the name of the function to use within a ResultSet.
     *
     * @param columnNames the names of the columns used to call the function
     * @return the name of the function to use within a ResultSet
     */
    public String columnName(List<String> columnNames);

    /**
     * Creates some new input arguments for this function.
     *
     * @param version the protocol version
     * @return some new input arguments for this function
     */
    Arguments newArguments(ProtocolVersion version);

    public default Optional<Difference> compare(Function other)
    {
        throw new UnsupportedOperationException();
    }
}
