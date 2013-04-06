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
import java.util.List;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class FunctionCall extends Term.NonTerminal
{
    private final Function fun;
    private final List<Term> terms;

    private FunctionCall(Function fun, List<Term> terms)
    {
        this.fun = fun;
        this.terms = terms;
    }

    public void collectMarkerSpecification(ColumnSpecification[] boundNames)
    {
        for (Term t : terms)
            t.collectMarkerSpecification(boundNames);
    }

    public Term.Terminal bind(List<ByteBuffer> values) throws InvalidRequestException
    {
        return makeTerminal(fun, bindAndGet(values));
    }

    public ByteBuffer bindAndGet(List<ByteBuffer> values) throws InvalidRequestException
    {
        List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(terms.size());
        for (Term t : terms)
        {
            // For now, we don't allow nulls as argument as no existing function needs it and it
            // simplify things.
            ByteBuffer val = t.bindAndGet(values);
            if (val == null)
                throw new InvalidRequestException(String.format("Invalid null value for argument to %s", fun));
            buffers.add(val);
        }

        return fun.execute(buffers);
    }

    private static Term.Terminal makeTerminal(Function fun, ByteBuffer result) throws InvalidRequestException
    {
        if (!(fun.returnType() instanceof CollectionType))
            return new Constants.Value(result);

        switch (((CollectionType)fun.returnType()).kind)
        {
            case LIST: return Lists.Value.fromSerialized(result, (ListType)fun.returnType());
            case SET:  return Sets.Value.fromSerialized(result, (SetType)fun.returnType());
            case MAP:  return Maps.Value.fromSerialized(result, (MapType)fun.returnType());
        }
        throw new AssertionError();
    }

    public static class Raw implements Term.Raw
    {
        private final String functionName;
        private final List<Term.Raw> terms;

        public Raw(String functionName, List<Term.Raw> terms)
        {
            this.functionName = functionName;
            this.terms = terms;
        }

        public Term prepare(ColumnSpecification receiver) throws InvalidRequestException
        {
            Function fun = Functions.get(functionName, terms, receiver);

            List<Term> parameters = new ArrayList<Term>(terms.size());
            boolean allTerminal = true;
            for (int i = 0; i < terms.size(); i++)
            {
                Term t = terms.get(i).prepare(Functions.makeArgSpec(receiver, fun, i));
                if (t instanceof NonTerminal)
                    allTerminal = false;
                parameters.add(t);
            }

            return allTerminal
                ? makeTerminal(fun, execute(fun, parameters))
                : new FunctionCall(fun, parameters);
        }

        // All parameters must be terminal
        private static ByteBuffer execute(Function fun, List<Term> parameters) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(parameters.size());
            for (Term t : parameters)
            {
                assert t instanceof Term.Terminal;
                buffers.add(((Term.Terminal)t).get());
            }
            return fun.execute(buffers);
        }

        public boolean isAssignableTo(ColumnSpecification receiver)
        {
            AbstractType<?> returnType = Functions.getReturnType(functionName, receiver.ksName, receiver.cfName);
            // Note: if returnType == null, it means the function doesn't exist. We may get this if an undefined function
            // is used as argument of another, existing, function. In that case, we return true here because we'll catch
            // the fact that the method is undefined latter anyway and with a more helpful error message that if we were
            // to return false here.
            return returnType == null || receiver.type.asCQL3Type().equals(returnType.asCQL3Type());
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(functionName).append("(");
            for (int i = 0; i < terms.size(); i++)
            {
                if (i > 0) sb.append(", ");
                sb.append(terms.get(i));
            }
            return sb.append(")").toString();
        }
    }
}
