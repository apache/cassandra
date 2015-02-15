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

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.marshal.AbstractType;

public class FunctionExecutionException extends CassandraException
{
    public final FunctionName functionName;
    public final List<String> argTypes;
    public final String detail;

    public static FunctionExecutionException create(Function function, Throwable cause)
    {
        List<String> cqlTypes = AbstractType.asCQLTypeStringList(function.argTypes());
        FunctionExecutionException fee = new FunctionExecutionException(function.name(), cqlTypes, cause.toString());
        fee.initCause(cause);
        return fee;
    }

    public FunctionExecutionException(FunctionName functionName, List<String> argTypes, String detail)
    {
        super(ExceptionCode.FUNCTION_FAILURE, "execution of '" + functionName + argTypes + "' failed: " + detail);
        this.functionName = functionName;
        this.argTypes = argTypes;
        this.detail = detail;
    }
}
