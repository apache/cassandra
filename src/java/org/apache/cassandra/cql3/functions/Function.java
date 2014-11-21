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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.github.jamm.Unmetered;

@Unmetered
public interface Function
{
    public String name();
    public List<AbstractType<?>> argsType();
    public AbstractType<?> returnType();

    public ByteBuffer execute(List<ByteBuffer> parameters) throws InvalidRequestException;

    // Whether the function is a pure function (as in doesn't depend on, nor produce side effects).
    public boolean isPure();

    public interface Factory
    {
        // We allow the function to be parametered by the keyspace it is part of because the
        // "token" function needs it (the argument depends on the keyValidator). However,
        // for most function, the factory will just always the same function object (see
        // AbstractFunction).
        public Function create(String ksName, String cfName);
    }
}
