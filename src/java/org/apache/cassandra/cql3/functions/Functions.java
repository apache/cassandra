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

import com.google.common.collect.ArrayListMultimap;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.AssignementTestable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public abstract class Functions
{
    private Functions() {}

    // If we ever allow this to be populated at runtime, this will need to be thread safe.
    private static final ArrayListMultimap<String, Function.Factory> declared = ArrayListMultimap.create();
    static
    {
        // All method sharing the same name must have the same returnType. We could find a way to make that clear.
        declared.put("token", TokenFct.factory);

        declared.put("now", AbstractFunction.factory(TimeuuidFcts.nowFct));
        declared.put("mintimeuuid", AbstractFunction.factory(TimeuuidFcts.minTimeuuidFct));
        declared.put("maxtimeuuid", AbstractFunction.factory(TimeuuidFcts.maxTimeuuidFct));
        declared.put("dateof", AbstractFunction.factory(TimeuuidFcts.dateOfFct));
        declared.put("unixtimestampof", AbstractFunction.factory(TimeuuidFcts.unixTimestampOfFct));
        declared.put("uuid", AbstractFunction.factory(UuidFcts.uuidFct));

        for (CQL3Type type : CQL3Type.Native.values())
        {
            // Note: because text and varchar ends up being synonimous, our automatic makeToBlobFunction doesn't work
            // for varchar, so we special case it below. We also skip blob for obvious reasons.
            if (type == CQL3Type.Native.VARCHAR || type == CQL3Type.Native.BLOB)
                continue;

            Function toBlob = BytesConversionFcts.makeToBlobFunction(type.getType());
            Function fromBlob = BytesConversionFcts.makeFromBlobFunction(type.getType());
            declared.put(toBlob.name(), AbstractFunction.factory(toBlob));
            declared.put(fromBlob.name(), AbstractFunction.factory(fromBlob));
        }
        declared.put("varcharasblob", AbstractFunction.factory(BytesConversionFcts.VarcharAsBlobFct));
        declared.put("blobasvarchar", AbstractFunction.factory(BytesConversionFcts.BlobAsVarcharFact));
    }

    public static AbstractType<?> getReturnType(String functionName, String ksName, String cfName)
    {
        List<Function.Factory> factories = declared.get(functionName.toLowerCase());
        return factories.isEmpty()
             ? null // That's ok, we'll complain later
             : factories.get(0).create(ksName, cfName).returnType();
    }

    public static ColumnSpecification makeArgSpec(ColumnSpecification receiver, Function fun, int i)
    {
        return new ColumnSpecification(receiver.ksName,
                receiver.cfName,
                new ColumnIdentifier("arg" + i +  "(" + fun.name() + ")", true),
                fun.argsType().get(i));
    }

    public static Function get(String keyspace, String name, List<? extends AssignementTestable> providedArgs, ColumnSpecification receiver) throws InvalidRequestException
    {
        List<Function.Factory> factories = declared.get(name.toLowerCase());
        if (factories.isEmpty())
            throw new InvalidRequestException(String.format("Unknown CQL3 function %s called", name));

        // Fast path if there is not choice
        if (factories.size() == 1)
        {
            Function fun = factories.get(0).create(receiver.ksName, receiver.cfName);
            validateTypes(keyspace, fun, providedArgs, receiver);
            return fun;
        }

        Function candidate = null;
        for (Function.Factory factory : factories)
        {
            Function toTest = factory.create(receiver.ksName, receiver.cfName);
            if (!isValidType(keyspace, toTest, providedArgs, receiver))
                continue;

            if (candidate == null)
                candidate = toTest;
            else
                throw new InvalidRequestException(String.format("Ambiguous call to function %s (can match both type signature %s and %s): use type casts to disambiguate", name, signature(candidate), signature(toTest)));
        }
        if (candidate == null)
            throw new InvalidRequestException(String.format("Invalid call to function %s, none of its type signature matches (known type signatures: %s)", name, signatures(factories, receiver)));
        return candidate;
    }

    private static void validateTypes(String keyspace, Function fun, List<? extends AssignementTestable> providedArgs, ColumnSpecification receiver) throws InvalidRequestException
    {
        if (!receiver.type.isValueCompatibleWith(fun.returnType()))
            throw new InvalidRequestException(String.format("Type error: cannot assign result of function %s (type %s) to %s (type %s)", fun.name(), fun.returnType().asCQL3Type(), receiver, receiver.type.asCQL3Type()));

        if (providedArgs.size() != fun.argsType().size())
            throw new InvalidRequestException(String.format("Invalid number of arguments in call to function %s: %d required but %d provided", fun.name(), fun.argsType().size(), providedArgs.size()));

        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignementTestable provided = providedArgs.get(i);

            // If the concrete argument is a bind variables, it can have any type.
            // We'll validate the actually provided value at execution time.
            if (provided == null)
                continue;

            ColumnSpecification expected = makeArgSpec(receiver, fun, i);
            if (!provided.isAssignableTo(keyspace, expected))
                throw new InvalidRequestException(String.format("Type error: %s cannot be passed as argument %d of function %s of type %s", provided, i, fun.name(), expected.type.asCQL3Type()));
        }
    }

    private static boolean isValidType(String keyspace, Function fun, List<? extends AssignementTestable> providedArgs, ColumnSpecification receiver) throws InvalidRequestException
    {
        if (!receiver.type.isValueCompatibleWith(fun.returnType()))
            return false;

        if (providedArgs.size() != fun.argsType().size())
            return false;

        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignementTestable provided = providedArgs.get(i);

            // If the concrete argument is a bind variables, it can have any type.
            // We'll validate the actually provided value at execution time.
            if (provided == null)
                continue;

            ColumnSpecification expected = makeArgSpec(receiver, fun, i);
            if (!provided.isAssignableTo(keyspace, expected))
                return false;
        }
        return true;
    }

    private static String signature(Function fun)
    {
        List<AbstractType<?>> args = fun.argsType();
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (int i = 0; i < args.size(); i++)
        {
            if (i > 0) sb.append(", ");
            sb.append(args.get(i).asCQL3Type());
        }
        sb.append(") -> ");
        sb.append(fun.returnType().asCQL3Type());
        return sb.toString();
    }

    private static String signatures(List<Function.Factory> factories, ColumnSpecification receiver)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < factories.size(); i++)
        {
            if (i > 0) sb.append(", ");
            sb.append(signature(factories.get(i).create(receiver.ksName, receiver.cfName)));
        }
        return sb.toString();
    }
}
