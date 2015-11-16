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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static java.util.stream.Collectors.joining;

public final class FunctionResolver
{
    private FunctionResolver()
    {
    }

    // We special case the token function because that's the only function whose argument types actually
    // depend on the table on which the function is called. Because it's the sole exception, it's easier
    // to handle it as a special case.
    private static final FunctionName TOKEN_FUNCTION_NAME = FunctionName.nativeFunction("token");

    public static ColumnSpecification makeArgSpec(String receiverKs, String receiverCf, Function fun, int i)
    {
        return new ColumnSpecification(receiverKs,
                                       receiverCf,
                                       new ColumnIdentifier("arg" + i + '(' + fun.name().toString().toLowerCase() + ')', true),
                                       fun.argTypes().get(i));
    }

    /**
     * @param keyspace the current keyspace
     * @param name the name of the function
     * @param providedArgs the arguments provided for the function call
     * @param receiverKs the receiver's keyspace
     * @param receiverCf the receiver's table
     * @param receiverType if the receiver type is known (during inserts, for example), this should be the type of
     *                     the receiver
     * @throws InvalidRequestException
     */
    public static Function get(String keyspace,
                               FunctionName name,
                               List<? extends AssignmentTestable> providedArgs,
                               String receiverKs,
                               String receiverCf,
                               AbstractType<?> receiverType)
    throws InvalidRequestException
    {
        if (name.equalsNativeFunction(TOKEN_FUNCTION_NAME))
            return new TokenFct(Schema.instance.getCFMetaData(receiverKs, receiverCf));

        // The toJson() function can accept any type of argument, so instances of it are not pre-declared.  Instead,
        // we create new instances as needed while handling selectors (which is the only place that toJson() is supported,
        // due to needing to know the argument types in advance).
        if (name.equalsNativeFunction(ToJsonFct.NAME))
            throw new InvalidRequestException("toJson() may only be used within the selection clause of SELECT statements");

        // Similarly, we can only use fromJson when we know the receiver type (such as inserts)
        if (name.equalsNativeFunction(FromJsonFct.NAME))
        {
            if (receiverType == null)
                throw new InvalidRequestException("fromJson() cannot be used in the selection clause of a SELECT statement");
            return FromJsonFct.getInstance(receiverType);
        }

        Collection<Function> candidates;
        if (!name.hasKeyspace())
        {
            // function name not fully qualified
            candidates = new ArrayList<>();
            // add 'SYSTEM' (native) candidates
            candidates.addAll(Schema.instance.getFunctions(name.asNativeFunction()));
            // add 'current keyspace' candidates
            candidates.addAll(Schema.instance.getFunctions(new FunctionName(keyspace, name.name)));
        }
        else
        {
            // function name is fully qualified (keyspace + name)
            candidates = Schema.instance.getFunctions(name);
        }

        if (candidates.isEmpty())
            return null;

        // Fast path if there is only one choice
        if (candidates.size() == 1)
        {
            Function fun = candidates.iterator().next();
            validateTypes(keyspace, fun, providedArgs, receiverKs, receiverCf);
            return fun;
        }

        List<Function> compatibles = null;
        for (Function toTest : candidates)
        {
            AssignmentTestable.TestResult r = matchAguments(keyspace, toTest, providedArgs, receiverKs, receiverCf);
            switch (r)
            {
                case EXACT_MATCH:
                    // We always favor exact matches
                    return toTest;
                case WEAKLY_ASSIGNABLE:
                    if (compatibles == null)
                        compatibles = new ArrayList<>();
                    compatibles.add(toTest);
                    break;
            }
        }

        if (compatibles == null)
        {
            throw new InvalidRequestException(String.format("Invalid call to function %s, none of its type signatures match (known type signatures: %s)",
                                                            name, format(candidates)));
        }

        if (compatibles.size() > 1)
            throw new InvalidRequestException(String.format("Ambiguous call to function %s (can be matched by following signatures: %s): use type casts to disambiguate",
                        name, format(compatibles)));

        return compatibles.get(0);
    }

    // This method and matchArguments are somewhat duplicate, but this method allows us to provide more precise errors in the common
    // case where there is no override for a given function. This is thus probably worth the minor code duplication.
    private static void validateTypes(String keyspace,
                                      Function fun,
                                      List<? extends AssignmentTestable> providedArgs,
                                      String receiverKs,
                                      String receiverCf)
    throws InvalidRequestException
    {
        if (providedArgs.size() != fun.argTypes().size())
            throw new InvalidRequestException(String.format("Invalid number of arguments in call to function %s: %d required but %d provided", fun.name(), fun.argTypes().size(), providedArgs.size()));

        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignmentTestable provided = providedArgs.get(i);

            // If the concrete argument is a bind variables, it can have any type.
            // We'll validate the actually provided value at execution time.
            if (provided == null)
                continue;

            ColumnSpecification expected = makeArgSpec(receiverKs, receiverCf, fun, i);
            if (!provided.testAssignment(keyspace, expected).isAssignable())
                throw new InvalidRequestException(String.format("Type error: %s cannot be passed as argument %d of function %s of type %s", provided, i, fun.name(), expected.type.asCQL3Type()));
        }
    }

    private static AssignmentTestable.TestResult matchAguments(String keyspace,
                                                               Function fun,
                                                               List<? extends AssignmentTestable> providedArgs,
                                                               String receiverKs,
                                                               String receiverCf)
    {
        if (providedArgs.size() != fun.argTypes().size())
            return AssignmentTestable.TestResult.NOT_ASSIGNABLE;

        // It's an exact match if all are exact match, but is not assignable as soon as any is non assignable.
        AssignmentTestable.TestResult res = AssignmentTestable.TestResult.EXACT_MATCH;
        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignmentTestable provided = providedArgs.get(i);
            if (provided == null)
            {
                res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                continue;
            }

            ColumnSpecification expected = makeArgSpec(receiverKs, receiverCf, fun, i);
            AssignmentTestable.TestResult argRes = provided.testAssignment(keyspace, expected);
            if (argRes == AssignmentTestable.TestResult.NOT_ASSIGNABLE)
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            if (argRes == AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE)
                res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        }
        return res;
    }

    private static String format(Collection<Function> funs)
    {
        return funs.stream().map(Function::toString).collect(joining(", "));
    }
}
