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
package org.apache.cassandra.cql3;

public class Operation
{
    public static enum Type { PLUS, MINUS }

    public final Type type;
    public final ColumnIdentifier ident;
    public final Term value;

    // unary operation
    public Operation(Term a)
    {
        this(null, null, a);
    }

    // binary operation
    public Operation(ColumnIdentifier a, Type type, Term b)
    {
        this.ident = a;
        this.type = type;
        this.value = b;
    }

    public boolean isUnary()
    {
        return type == null && ident == null;
    }

    public String toString()
    {
        return (isUnary())
                ? String.format("UnaryOperation(%s)", value)
                : String.format("BinaryOperation(%s, %s, %s)", ident, type, value);
    }
}
