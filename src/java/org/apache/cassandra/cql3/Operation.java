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

import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.thrift.InvalidRequestException;

public abstract class Operation
{
    public static enum Type { SET, COUNTER, FUNCTION }

    public final Type type;

    protected Operation(Type type)
    {
        this.type = type;
    }

    public abstract Iterable<Term> allTerms();

    public static class Set extends Operation
    {
        public final Value value;

        public Set(Value value)
        {
            super(Type.SET);
            this.value = value;
        }

        @Override
        public String toString()
        {
            return " = " + value;
        }

        public List<Term> allTerms()
        {
            return value.asList();
        }
    }

    public static class Counter extends Operation
    {
        public final Term value;
        public final boolean isSubstraction;

        public Counter(Term value, boolean isSubstraction)
        {
            super(Type.COUNTER);
            this.value = value;
            this.isSubstraction = isSubstraction;
        }

        @Override
        public String toString()
        {
            return (isSubstraction ? "-" : "+") + "= " + value;
        }

        public Iterable<Term> allTerms()
        {
            return Collections.singletonList(value);
        }
    }

    public static class Function extends Operation
    {
        public final CollectionType.Function fct;
        public final List<Term> arguments;

        public Function(CollectionType.Function fct, List<Term> arguments)
        {
            super(Type.FUNCTION);
            this.fct = fct;
            this.arguments = arguments;
        }

        @Override
        public String toString()
        {
            return "." + fct + arguments;
        }

        public Iterable<Term> allTerms()
        {
            return arguments;
        }
    }
}
