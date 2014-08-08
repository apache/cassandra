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
package org.apache.cassandra.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.apache.cassandra.cql3.AssignementTestable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.CqlLexer;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.udf.UDFFunctionOverloads;
import org.apache.cassandra.cql3.udf.UDFRegistry;
import org.apache.cassandra.db.CFRowAdder;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;

/**
 * Defined (and loaded) user functions.
 * <p/>
 * In practice, because user functions are global, we have only one instance of
 * this class that retrieve through the Schema class.
 */
public final class UFMetaData
{
    public final String namespace;
    public final String functionName;
    public final String qualifiedName;
    public final String returnType;
    public final List<String> argumentNames;
    public final List<String> argumentTypes;
    public final String language;
    public final String body;
    public final boolean deterministic;

    public final String signature;
    public final List<CQL3Type> cqlArgumentTypes;
    public final CQL3Type cqlReturnType;

    static final CompositeType partKey = (CompositeType) CFMetaData.SchemaFunctionsCf.getKeyValidator();

    // TODO tracking "valid" status via an exception field is really bad style - but we need some way to mark a function as "dead"
    public InvalidRequestException invalid;

    public UFMetaData(String namespace, String functionName, boolean deterministic, List<String> argumentNames,
                      List<String> argumentTypes, String returnType, String language, String body)
    {
        this.namespace = namespace != null ? namespace.toLowerCase() : "";
        this.functionName = functionName.toLowerCase();
        this.qualifiedName = qualifiedName(namespace, functionName);
        this.returnType = returnType;
        this.argumentNames = argumentNames;
        this.argumentTypes = argumentTypes;
        this.language = language == null ? "class" : language.toLowerCase();
        this.body = body;
        this.deterministic = deterministic;

        this.cqlArgumentTypes = new ArrayList<>(argumentTypes.size());
        InvalidRequestException inv = null;
        CQL3Type rt = null;
        try
        {
            rt = parseCQLType(returnType);
            for (String argumentType : argumentTypes)
                cqlArgumentTypes.add(parseCQLType(argumentType));
        }
        catch (InvalidRequestException e)
        {
            inv = e;
        }
        this.invalid = inv;
        this.cqlReturnType = rt;

        StringBuilder signature = new StringBuilder();
        signature.append(qualifiedName);
        for (String argumentType : argumentTypes)
        {
            signature.append(',');
            signature.append(argumentType);
        }
        this.signature = signature.toString();
    }

    public boolean compatibleArgs(String ksName, String cfName, List<? extends AssignementTestable> providedArgs)
    {
        int cnt = cqlArgumentTypes.size();
        if (cnt != providedArgs.size())
            return false;
        for (int i = 0; i < cnt; i++)
        {
            AssignementTestable provided = providedArgs.get(i);

            if (provided == null)
                continue;

            AbstractType<?> argType = cqlArgumentTypes.get(i).getType();

            ColumnSpecification expected = makeArgSpec(ksName, cfName, argType, i);
            if (!provided.isAssignableTo(ksName, expected))
                return false;
        }

        return true;
    }

    public ColumnSpecification makeArgSpec(String ksName, String cfName, AbstractType<?> argType, int i)
    {
        return new ColumnSpecification(ksName,
                                       cfName,
                                       new ColumnIdentifier("arg" + i + "(" + qualifiedName + ")", true), argType);
    }

    private static CQL3Type parseCQLType(String cqlType)
    throws InvalidRequestException
    {
        CharStream stream = new ANTLRStringStream(cqlType);
        CqlLexer lexer = new CqlLexer(stream);

        TokenStream tokenStream = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(tokenStream);
        try
        {
            CQL3Type.Raw rawType = parser.comparatorType();
            // TODO CASSANDRA-7563 use appropiate keyspace here ... keyspace must be fully qualified
            CQL3Type t = rawType.prepare(null);
            // TODO CASSANDRA-7563 support "complex" types (UDT, tuples, collections), remove catch-NPE below
            if (!(t instanceof CQL3Type.Native))
                throw new InvalidRequestException("non-native CQL type '" + cqlType + "' not supported");
            return t;
        }
        catch (NullPointerException | InvalidRequestException | RecognitionException e)
        {
            throw new InvalidRequestException("invalid CQL type '" + cqlType + "'");
        }
    }

    public static String qualifiedName(String namespace, String functionName)
    {
        if (namespace == null)
            return "::" + functionName;
        return (namespace + "::" + functionName).toLowerCase();
    }

    public static Mutation dropFunction(long timestamp, String namespace, String functionName)
    {
        UDFFunctionOverloads sigMap = UDFRegistry.getFunctionSigMap(UFMetaData.qualifiedName(namespace, functionName));
        if (sigMap == null || sigMap.isEmpty())
            return null;

        Mutation mutation = new Mutation(Keyspace.SYSTEM_KS, partKey.decompose(namespace, functionName));
        ColumnFamily cf = mutation.addOrGet(SystemKeyspace.SCHEMA_FUNCTIONS_CF);

        int ldt = (int) (System.currentTimeMillis() / 1000);
        for (UFMetaData f : sigMap.values())
            udfRemove(timestamp, cf, ldt, f);

        return mutation;
    }

    private static Composite udfSignatureKey(UFMetaData function)
    {
        return CFMetaData.SchemaFunctionsCf.comparator.make(function.signature);
    }

    private static void udfRemove(long timestamp, ColumnFamily cf, int ldt, UFMetaData f)
    {
        Composite prefix = udfSignatureKey(f);
        cf.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));
    }

    public static Mutation createOrReplaceFunction(long timestamp, UFMetaData f)
    throws ConfigurationException, SyntaxException
    {
        Mutation mutation = new Mutation(Keyspace.SYSTEM_KS, partKey.decompose(f.namespace, f.functionName));
        ColumnFamily cf = mutation.addOrGet(SystemKeyspace.SCHEMA_FUNCTIONS_CF);

        Composite prefix = udfSignatureKey(f);
        CFRowAdder adder = new CFRowAdder(cf, prefix, timestamp);

        adder.resetCollection("argument_names");
        adder.resetCollection("argument_types");
        adder.add("name", f.functionName);
        adder.add("return_type", f.returnType);
        adder.add("language", f.language);
        adder.add("body", f.body);
        adder.add("deterministic", f.deterministic);

        for (String argName : f.argumentNames)
            adder.addListEntry("argument_names", argName);
        for (String argType : f.argumentTypes)
            adder.addListEntry("argument_types", argType);

        return mutation;
    }

    public static UFMetaData fromSchema(UntypedResultSet.Row row)
    {
        String namespace = row.getString("namespace");
        String name = row.getString("name");
        List<String> argumentNames = row.getList("argument_names", UTF8Type.instance);
        List<String> argumentTypes = row.getList("argument_types", UTF8Type.instance);
        String returnType = row.getString("return_type");
        boolean deterministic = row.getBoolean("deterministic");
        String language = row.getString("language");
        String body = row.getString("body");

        return new UFMetaData(namespace, name, deterministic, argumentNames, argumentTypes, returnType, language, body);
    }

    public static Map<String, UFMetaData> fromSchema(Row row)
    {
        UntypedResultSet results = QueryProcessor.resultify("SELECT * FROM system." + SystemKeyspace.SCHEMA_FUNCTIONS_CF, row);
        Map<String, UFMetaData> udfs = new HashMap<>(results.size());
        for (UntypedResultSet.Row result : results)
        {
            UFMetaData udf = fromSchema(result);
            udfs.put(udf.signature, udf);
        }
        return udfs;
    }

    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        UFMetaData that = (UFMetaData) o;
        if (!signature.equals(that.signature))
            return false;
        if (deterministic != that.deterministic)
            return false;
        if (argumentNames != null ? !argumentNames.equals(that.argumentNames) : that.argumentNames != null)
            return false;
        if (body != null ? !body.equals(that.body) : that.body != null)
            return false;
        if (!namespace.equals(that.namespace))
            return false;
        if (!language.equals(that.language))
            return false;
        if (returnType != null ? !returnType.equals(that.returnType) : that.returnType != null)
            return false;

        return true;
    }

    public int hashCode()
    {
        int result = signature.hashCode();
        result = 31 * result + (returnType != null ? returnType.hashCode() : 0);
        result = 31 * result + (argumentNames != null ? argumentNames.hashCode() : 0);
        result = 31 * result + (argumentTypes.hashCode());
        result = 31 * result + (language.hashCode());
        result = 31 * result + (body != null ? body.hashCode() : 0);
        result = 31 * result + (deterministic ? 1 : 0);
        return result;
    }

    public String toString()
    {
        return new ToStringBuilder(this)
               .append("signature", signature)
               .append("returnType", returnType)
               .append("deterministic", deterministic)
               .append("language", language)
               .append("body", body)
               .toString();
    }
}
