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

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.serializers.MarshalException;

public class FunctionCall extends Term.NonTerminal
{
    private final Function fun;
    private final List<Term> terms;

    private FunctionCall(Function fun, List<Term> terms)
    {
        this.fun = fun;
        this.terms = terms;
    }

    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        for (Term t : terms)
            t.collectMarkerSpecification(boundNames);
    }

    public Term.Terminal bind(QueryOptions options) throws InvalidRequestException
    {
        return makeTerminal(fun, bindAndGet(options), options.getProtocolVersion());
    }

    public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
    {
        List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(terms.size());
        for (Term t : terms)
        {
            // For now, we don't allow nulls as argument as no existing function needs it and it
            // simplify things.
            ByteBuffer val = t.bindAndGet(options);
            if (val == null)
                throw new InvalidRequestException(String.format("Invalid null value for argument to %s", fun));
            buffers.add(val);
        }
        return executeInternal(fun, buffers);
    }

    private static ByteBuffer executeInternal(Function fun, List<ByteBuffer> params) throws InvalidRequestException
    {
        ByteBuffer result = fun.execute(params);
        try
        {
            // Check the method didn't lied on it's declared return type
            if (result != null)
                fun.returnType().validate(result);
            return result;
        }
        catch (MarshalException e)
        {
            throw new RuntimeException(String.format("Return of function %s (%s) is not a valid value for its declared return type %s", 
                                                     fun, ByteBufferUtil.bytesToHex(result), fun.returnType().asCQL3Type()));
        }
    }

    public boolean containsBindMarker()
    {
        for (Term t : terms)
        {
            if (t.containsBindMarker())
                return true;
        }
        return false;
    }

    private static Term.Terminal makeTerminal(Function fun, ByteBuffer result, int version) throws InvalidRequestException
    {
        if (!(fun.returnType() instanceof CollectionType))
            return new Constants.Value(result);

        switch (((CollectionType)fun.returnType()).kind)
        {
            case LIST: return Lists.Value.fromSerialized(result, (ListType)fun.returnType(), version);
            case SET:  return Sets.Value.fromSerialized(result, (SetType)fun.returnType(), version);
            case MAP:  return Maps.Value.fromSerialized(result, (MapType)fun.returnType(), version);
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

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            Function fun = Functions.get(keyspace, functionName, terms, receiver);

            List<Term> parameters = new ArrayList<Term>(terms.size());
            boolean allTerminal = true;
            for (int i = 0; i < terms.size(); i++)
            {
                Term t = terms.get(i).prepare(keyspace, Functions.makeArgSpec(receiver, fun, i));
                if (t instanceof NonTerminal)
                    allTerminal = false;
                parameters.add(t);
            }

            // If all parameters are terminal and the function is pure, we can
            // evaluate it now, otherwise we'd have to wait execution time
            return allTerminal && fun.isPure()
                ? makeTerminal(fun, execute(fun, parameters), QueryOptions.DEFAULT.getProtocolVersion())
                : new FunctionCall(fun, parameters);
        }

        // All parameters must be terminal
        private static ByteBuffer execute(Function fun, List<Term> parameters) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(parameters.size());
            for (Term t : parameters)
            {
                assert t instanceof Term.Terminal;
                buffers.add(((Term.Terminal)t).get(QueryOptions.DEFAULT));
            }

            return executeInternal(fun, buffers);
        }

        public boolean isAssignableTo(String keyspace, ColumnSpecification receiver)
        {
            AbstractType<?> returnType = Functions.getReturnType(functionName, receiver.ksName, receiver.cfName);
            // Note: if returnType == null, it means the function doesn't exist. We may get this if an undefined function
            // is used as argument of another, existing, function. In that case, we return true here because we'll catch
            // the fact that the method is undefined latter anyway and with a more helpful error message that if we were
            // to return false here.
            return returnType == null || receiver.type.isValueCompatibleWith(returnType);
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
