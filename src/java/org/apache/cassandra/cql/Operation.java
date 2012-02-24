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
package org.apache.cassandra.cql;

public class Operation
{
    public static enum OperationType
    { PLUS, MINUS }

    public final OperationType type;
    public final Term a, b;

    // unary operation
    public Operation(Term a)
    {
        this.a = a;
        type = null;
        b = null;
    }

    // binary operation
    public Operation(Term a, OperationType type, Term b)
    {
        this.a = a;
        this.type = type;
        this.b = b;
    }

    public boolean isUnary()
    {
        return type == null && b == null;
    }

    public String toString()
    {
        return (isUnary())
                ? String.format("UnaryOperation(%s)", a)
                : String.format("BinaryOperation(%s, %s, %s)", a, type, b);
    }
}
