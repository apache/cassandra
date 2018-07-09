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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public class FunctionCall extends Term.NonTerminal
{
    private final ScalarFunction fun;
    private final List<Term> terms;

    private FunctionCall(ScalarFunction fun, List<Term> terms)
    {
        this.fun = fun;
        this.terms = terms;
    }

    public void addFunctionsTo(List<Function> functions)
    {
        Terms.addFunctions(terms, functions);
        fun.addFunctionsTo(functions);
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
        List<ByteBuffer> buffers = new ArrayList<>(terms.size());
        for (Term t : terms)
        {
            ByteBuffer functionArg = t.bindAndGet(options);
            RequestValidations.checkBindValueSet(functionArg, "Invalid unset value for argument in call to function %s", fun.name().name);
            buffers.add(functionArg);
        }
        return executeInternal(options.getProtocolVersion(), fun, buffers);
    }

    private static ByteBuffer executeInternal(ProtocolVersion protocolVersion, ScalarFunction fun, List<ByteBuffer> params) throws InvalidRequestException
    {
        ByteBuffer result = fun.execute(protocolVersion, params);
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
                                                     fun, ByteBufferUtil.bytesToHex(result), fun.returnType().asCQL3Type()), e);
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

    private static Term.Terminal makeTerminal(Function fun, ByteBuffer result, ProtocolVersion version) throws InvalidRequestException
    {
        if (result == null)
            return null;
        if (fun.returnType().isCollection())
        {
            switch (((CollectionType) fun.returnType()).kind)
            {
                case LIST:
                    return Lists.Value.fromSerialized(result, (ListType) fun.returnType(), version);
                case SET:
                    return Sets.Value.fromSerialized(result, (SetType) fun.returnType(), version);
                case MAP:
                    return Maps.Value.fromSerialized(result, (MapType) fun.returnType(), version);
            }
        }
        else if (fun.returnType().isUDT())
        {
            return UserTypes.Value.fromSerialized(result, (UserType) fun.returnType());
        }

        return new Constants.Value(result);
    }

    public static class Raw extends Term.Raw
    {
        private FunctionName name;
        private final List<Term.Raw> terms;

        public Raw(FunctionName name, List<Term.Raw> terms)
        {
            this.name = name;
            this.terms = terms;
        }

        public static Raw newOperation(char operator, Term.Raw left, Term.Raw right)
        {
            FunctionName name = OperationFcts.getFunctionNameFromOperator(operator);
            return new Raw(name, Arrays.asList(left, right));
        }

        public static Raw newNegation(Term.Raw raw)
        {
            FunctionName name = FunctionName.nativeFunction(OperationFcts.NEGATION_FUNCTION_NAME);
            return new Raw(name, Collections.singletonList(raw));
        }

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            Function fun = FunctionResolver.get(keyspace, name, terms, receiver.ksName, receiver.cfName, receiver.type);
            if (fun == null)
                throw invalidRequest("Unknown function %s called", name);
            if (fun.isAggregate())
                throw invalidRequest("Aggregation function are not supported in the where clause");

            ScalarFunction scalarFun = (ScalarFunction) fun;

            // Functions.get() will complain if no function "name" type check with the provided arguments.
            // We still have to validate that the return type matches however
            if (!scalarFun.testAssignment(keyspace, receiver).isAssignable())
            {
                if (OperationFcts.isOperation(name))
                    throw invalidRequest("Type error: cannot assign result of operation %s (type %s) to %s (type %s)",
                                         OperationFcts.getOperator(scalarFun.name()), scalarFun.returnType().asCQL3Type(),
                                         receiver.name, receiver.type.asCQL3Type());

                throw invalidRequest("Type error: cannot assign result of function %s (type %s) to %s (type %s)",
                                     scalarFun.name(), scalarFun.returnType().asCQL3Type(),
                                     receiver.name, receiver.type.asCQL3Type());
            }

            if (fun.argTypes().size() != terms.size())
                throw invalidRequest("Incorrect number of arguments specified for function %s (expected %d, found %d)",
                                     fun, fun.argTypes().size(), terms.size());

            List<Term> parameters = new ArrayList<>(terms.size());
            for (int i = 0; i < terms.size(); i++)
            {
                Term t = terms.get(i).prepare(keyspace, FunctionResolver.makeArgSpec(receiver.ksName, receiver.cfName, scalarFun, i));
                parameters.add(t);
            }

            return new FunctionCall(scalarFun, parameters);
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            // Note: Functions.get() will return null if the function doesn't exist, or throw is no function matching
            // the arguments can be found. We may get one of those if an undefined/wrong function is used as argument
            // of another, existing, function. In that case, we return true here because we'll throw a proper exception
            // later with a more helpful error message that if we were to return false here.
            try
            {
                Function fun = FunctionResolver.get(keyspace, name, terms, receiver.ksName, receiver.cfName, receiver.type);

                // Because fromJson() can return whatever type the receiver is, we'll always get EXACT_MATCH.  To
                // handle potentially ambiguous function calls with fromJson() as an argument, always return
                // WEAKLY_ASSIGNABLE to force the user to typecast if necessary
                if (fun != null && fun.name().equals(FromJsonFct.NAME))
                    return TestResult.WEAKLY_ASSIGNABLE;

                if (fun != null && receiver.type.equals(fun.returnType()))
                    return AssignmentTestable.TestResult.EXACT_MATCH;
                else if (fun == null || receiver.type.isValueCompatibleWith(fun.returnType()))
                    return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                else
                    return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            }
            catch (InvalidRequestException e)
            {
                return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
            }
        }

        public AbstractType<?> getExactTypeIfKnown(String keyspace)
        {
            // We could implement this, but the method is only used in selection clause, where FunctionCall is not used 
            // we use a Selectable.WithFunction instead). And if that method is later used in other places, better to
            // let that future patch make sure this can be implemented properly (note in particular we don't have access
            // to the receiver type, which FunctionResolver.get() takes) rather than provide an implementation that may
            // not work in all cases.
            throw new UnsupportedOperationException();
        }

        public String getText()
        {
            return name + terms.stream().map(Term.Raw::getText).collect(Collectors.joining(", ", "(", ")"));
        }
    }
}
