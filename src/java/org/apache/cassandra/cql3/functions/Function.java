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

import java.util.List;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.github.jamm.Unmetered;

@Unmetered
public interface Function extends AssignmentTestable
{
    public FunctionName name();
    public List<AbstractType<?>> argTypes();
    public AbstractType<?> returnType();

    /**
     * Checks whether the function is a native/hard coded one or not.
     *
     * @return <code>true</code> if the function is a native/hard coded one, <code>false</code> otherwise.
     */
    public boolean isNative();

    /**
     * Checks whether the function is an aggregate function or not.
     *
     * @return <code>true</code> if the function is an aggregate function, <code>false</code> otherwise.
     */
    public boolean isAggregate();

    public void addFunctionsTo(List<Function> functions);

    public boolean hasReferenceTo(Function function);

    /**
     * Returns the name of the function to use within a ResultSet.
     *
     * @param columnNames the names of the columns used to call the function
     * @return the name of the function to use within a ResultSet
     */
    public String columnName(List<String> columnNames);
}
