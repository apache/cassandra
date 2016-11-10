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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.MigrationListener;
import org.apache.cassandra.service.MigrationManager;

public abstract class Functions
{
    // We special case the token function because that's the only function whose argument types actually
    // depend on the table on which the function is called. Because it's the sole exception, it's easier
    // to handle it as a special case.
    private static final FunctionName TOKEN_FUNCTION_NAME = FunctionName.nativeFunction("token");

    private Functions() {}

    private static final ConcurrentMap<FunctionName, List<Function>> declared = new ConcurrentHashMap<>();

    static
    {
        declare(AggregateFcts.countRowsFunction);
        declare(TimeFcts.nowFct);
        declare(TimeFcts.minTimeuuidFct);
        declare(TimeFcts.maxTimeuuidFct);
        declare(TimeFcts.dateOfFct);
        declare(TimeFcts.unixTimestampOfFct);
        declare(TimeFcts.timeUuidtoDate);
        declare(TimeFcts.timeUuidToTimestamp);
        declare(TimeFcts.timeUuidToUnixTimestamp);
        declare(TimeFcts.timestampToDate);
        declare(TimeFcts.timestampToUnixTimestamp);
        declare(TimeFcts.dateToTimestamp);
        declare(TimeFcts.dateToUnixTimestamp);
        declare(UuidFcts.uuidFct);

        for (CQL3Type type : CQL3Type.Native.values())
        {
            // Note: because text and varchar ends up being synonymous, our automatic makeToBlobFunction doesn't work
            // for varchar, so we special case it below. We also skip blob for obvious reasons.
            if (type != CQL3Type.Native.VARCHAR && type != CQL3Type.Native.BLOB)
            {
                declare(BytesConversionFcts.makeToBlobFunction(type.getType()));
                declare(BytesConversionFcts.makeFromBlobFunction(type.getType()));
            }
        }
        declare(BytesConversionFcts.VarcharAsBlobFct);
        declare(BytesConversionFcts.BlobAsVarcharFact);

        for (CQL3Type type : CQL3Type.Native.values())
        {
            // special case varchar to avoid duplicating functions for UTF8Type
            if (type != CQL3Type.Native.VARCHAR)
            {
                declare(AggregateFcts.makeCountFunction(type.getType()));
                declare(AggregateFcts.makeMaxFunction(type.getType()));
                declare(AggregateFcts.makeMinFunction(type.getType()));
            }
        }
        declare(AggregateFcts.sumFunctionForByte);
        declare(AggregateFcts.sumFunctionForShort);
        declare(AggregateFcts.sumFunctionForInt32);
        declare(AggregateFcts.sumFunctionForLong);
        declare(AggregateFcts.sumFunctionForFloat);
        declare(AggregateFcts.sumFunctionForDouble);
        declare(AggregateFcts.sumFunctionForDecimal);
        declare(AggregateFcts.sumFunctionForVarint);
        declare(AggregateFcts.sumFunctionForCounter);
        declare(AggregateFcts.avgFunctionForByte);
        declare(AggregateFcts.avgFunctionForShort);
        declare(AggregateFcts.avgFunctionForInt32);
        declare(AggregateFcts.avgFunctionForLong);
        declare(AggregateFcts.avgFunctionForFloat);
        declare(AggregateFcts.avgFunctionForDouble);
        declare(AggregateFcts.avgFunctionForVarint);
        declare(AggregateFcts.avgFunctionForDecimal);
        declare(AggregateFcts.avgFunctionForCounter);

        MigrationManager.instance.register(new FunctionsMigrationListener());
    }

    private static void declare(Function fun)
    {
        synchronized (declared)
        {
            List<Function> functions = declared.get(fun.name());
            if (functions == null)
            {
                functions = new CopyOnWriteArrayList<>();
                List<Function> existing = declared.putIfAbsent(fun.name(), functions);
                if (existing != null)
                    functions = existing;
            }
            functions.add(fun);
        }
    }

    public static ColumnSpecification makeArgSpec(String receiverKs, String receiverCf, Function fun, int i)
    {
        return new ColumnSpecification(receiverKs,
                                       receiverCf,
                                       new ColumnIdentifier("arg" + i + '(' + fun.name().toString().toLowerCase() + ')', true),
                                       fun.argTypes().get(i));
    }

    public static int getOverloadCount(FunctionName name)
    {
        return find(name).size();
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

        List<Function> candidates;
        if (!name.hasKeyspace())
        {
            // function name not fully qualified
            candidates = new ArrayList<>();
            // add 'SYSTEM' (native) candidates
            candidates.addAll(find(name.asNativeFunction()));
            // add 'current keyspace' candidates
            candidates.addAll(find(new FunctionName(keyspace, name.name)));
        }
        else
            // function name is fully qualified (keyspace + name)
            candidates = find(name);

        if (candidates.isEmpty())
            return null;

        // Fast path if there is only one choice
        if (candidates.size() == 1)
        {
            Function fun = candidates.get(0);
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

        if (compatibles == null || compatibles.isEmpty())
            throw new InvalidRequestException(String.format("Invalid call to function %s, none of its type signatures match (known type signatures: %s)",
                                                            name, toString(candidates)));

        if (compatibles.size() > 1)
            throw new InvalidRequestException(String.format("Ambiguous call to function %s (can be matched by following signatures: %s): use type casts to disambiguate",
                        name, toString(compatibles)));

        return compatibles.get(0);
    }

    public static List<Function> find(FunctionName name)
    {
        List<Function> functions = declared.get(name);
        return functions != null ? functions : Collections.<Function>emptyList();
    }

    public static Function find(FunctionName name, List<AbstractType<?>> argTypes)
    {
        assert name.hasKeyspace() : "function name not fully qualified";
        for (Function f : find(name))
        {
            if (typeEquals(f.argTypes(), argTypes))
                return f;
        }
        return null;
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

    private static String toString(List<Function> funs)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < funs.size(); i++)
        {
            if (i > 0) sb.append(", ");
            sb.append(funs.get(i));
        }
        return sb.toString();
    }

    public static void addOrReplaceFunction(AbstractFunction fun)
    {
        // We shouldn't get there unless that function don't exist
        removeFunction(fun.name(), fun.argTypes());
        declare(fun);
    }

    // Same remarks than for addFunction
    public static void removeFunction(FunctionName name, List<AbstractType<?>> argTypes)
    {
        assert name.hasKeyspace() : "function name " + name + " not fully qualified";
        synchronized (declared)
        {
            List<Function> functions = find(name);
            for (int i = 0; i < functions.size(); i++)
            {
                Function f = functions.get(i);
                if (!typeEquals(f.argTypes(), argTypes))
                    continue;
                assert !f.isNative();
                functions.remove(i);
                if (functions.isEmpty())
                    declared.remove(name);
                return;
            }
        }
    }

    public static List<Function> getReferencesTo(Function old)
    {
        List<Function> references = new ArrayList<>();
        for (List<Function> functions : declared.values())
            for (Function function : functions)
                if (function.hasReferenceTo(old))
                    references.add(function);
        return references;
    }

    public static Collection<Function> all()
    {
        List<Function> all = new ArrayList<>();
        for (List<Function> functions : declared.values())
            all.addAll(functions);
        return all;
    }

    /*
     * We need to compare the CQL3 representation of the type because comparing
     * the AbstractType will fail for example if a UDT has been changed.
     * Reason is that UserType.equals() takes the field names and types into account.
     * Example CQL sequence that would fail when comparing AbstractType:
     *    CREATE TYPE foo ...
     *    CREATE FUNCTION bar ( par foo ) RETURNS foo ...
     *    ALTER TYPE foo ADD ...
     * or
     *    ALTER TYPE foo ALTER ...
     * or
     *    ALTER TYPE foo RENAME ...
     */
    public static boolean typeEquals(AbstractType<?> t1, AbstractType<?> t2)
    {
        return t1.asCQL3Type().toString().equals(t2.asCQL3Type().toString());
    }

    public static boolean typeEquals(List<AbstractType<?>> t1, List<AbstractType<?>> t2)
    {
        if (t1.size() != t2.size())
            return false;
        for (int i = 0; i < t1.size(); i ++)
            if (!typeEquals(t1.get(i), t2.get(i)))
                return false;
        return true;
    }

    public static int typeHashCode(AbstractType<?> t)
    {
        return t.asCQL3Type().toString().hashCode();
    }

    public static int typeHashCode(List<AbstractType<?>> types)
    {
        int h = 0;
        for (AbstractType<?> type : types)
            h = h * 31 + typeHashCode(type);
        return h;
    }

    private static class FunctionsMigrationListener extends MigrationListener
    {
        public void onUpdateUserType(String ksName, String typeName) {
            for (Function function : all())
                if (function instanceof UDFunction)
                    ((UDFunction)function).userTypeUpdated(ksName, typeName);
        }
    }
}
