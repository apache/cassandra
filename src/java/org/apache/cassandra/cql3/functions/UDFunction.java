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
import java.util.*;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.UserType;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Base class for User Defined Functions.
 */
public abstract class UDFunction extends AbstractFunction implements ScalarFunction
{
    protected static final Logger logger = LoggerFactory.getLogger(UDFunction.class);

    protected final List<ColumnIdentifier> argNames;
    protected final String language;
    protected final String body;
    private final boolean deterministic;

    protected final DataType[] argDataTypes;
    protected final DataType returnDataType;

    protected UDFunction(FunctionName name,
                         List<ColumnIdentifier> argNames,
                         List<AbstractType<?>> argTypes,
                         AbstractType<?> returnType,
                         String language,
                         String body,
                         boolean deterministic)
    {
        this(name, argNames, argTypes, UDHelper.driverTypes(argTypes), returnType,
             UDHelper.driverType(returnType), language, body, deterministic);
    }

    protected UDFunction(FunctionName name,
                         List<ColumnIdentifier> argNames,
                         List<AbstractType<?>> argTypes,
                         DataType[] argDataTypes,
                         AbstractType<?> returnType,
                         DataType returnDataType,
                         String language,
                         String body,
                         boolean deterministic)
    {
        super(name, argTypes, returnType);
        assert new HashSet<>(argNames).size() == argNames.size() : "duplicate argument names";
        this.argNames = argNames;
        this.language = language;
        this.body = body;
        this.deterministic = deterministic;
        this.argDataTypes = argDataTypes;
        this.returnDataType = returnDataType;
    }

    public static UDFunction create(FunctionName name,
                                    List<ColumnIdentifier> argNames,
                                    List<AbstractType<?>> argTypes,
                                    AbstractType<?> returnType,
                                    String language,
                                    String body,
                                    boolean deterministic)
    throws InvalidRequestException
    {
        switch (language)
        {
            case "java": return JavaSourceUDFFactory.buildUDF(name, argNames, argTypes, returnType, body, deterministic);
            default: return new ScriptBasedUDF(name, argNames, argTypes, returnType, language, body, deterministic);
        }
    }

    /**
     * It can happen that a function has been declared (is listed in the scheam) but cannot
     * be loaded (maybe only on some nodes). This is the case for instance if the class defining
     * the class is not on the classpath for some of the node, or after a restart. In that case,
     * we create a "fake" function so that:
     *  1) the broken function can be dropped easily if that is what people want to do.
     *  2) we return a meaningful error message if the function is executed (something more precise
     *     than saying that the function doesn't exist)
     */
    private static UDFunction createBrokenFunction(FunctionName name,
                                                   List<ColumnIdentifier> argNames,
                                                   List<AbstractType<?>> argTypes,
                                                   AbstractType<?> returnType,
                                                   String language,
                                                   String body,
                                                   final InvalidRequestException reason)
    {
        return new UDFunction(name, argNames, argTypes, returnType, language, body, true)
        {
            public ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
            {
                throw new InvalidRequestException(String.format("Function '%s' exists but hasn't been loaded successfully for the following reason: %s. "
                                                              + "Please see the server log for more details", this, reason.getMessage()));
            }
        };
    }

    public boolean isAggregate()
    {
        return false;
    }

    public boolean isPure()
    {
        return deterministic;
    }

    public boolean isNative()
    {
        return false;
    }

    /**
     * Used by UDF implementations (both Java code generated by {@link org.apache.cassandra.cql3.functions.JavaSourceUDFFactory}
     * and script executor {@link org.apache.cassandra.cql3.functions.ScriptBasedUDF}) to convert the C*
     * serialized representation to the Java object representation.
     *
     * @param protocolVersion the native protocol version used for serialization
     * @param argIndex        index of the UDF input argument
     */
    protected Object compose(int protocolVersion, int argIndex, ByteBuffer value)
    {
        return value == null ? null : argDataTypes[argIndex].deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    /**
     * Used by UDF implementations (both Java code generated by {@link org.apache.cassandra.cql3.functions.JavaSourceUDFFactory}
     * and script executor {@link org.apache.cassandra.cql3.functions.ScriptBasedUDF}) to convert the Java
     * object representation for the return value to the C* serialized representation.
     *
     * @param protocolVersion the native protocol version used for serialization
     */
    protected ByteBuffer decompose(int protocolVersion, Object value)
    {
        return value == null ? null : returnDataType.serialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    private static Mutation makeSchemaMutation(FunctionName name)
    {
        UTF8Type kv = (UTF8Type)SystemKeyspace.SchemaFunctionsTable.getKeyValidator();
        return new Mutation(SystemKeyspace.NAME, kv.decompose(name.keyspace));
    }

    public Mutation toSchemaDrop(long timestamp)
    {
        Mutation mutation = makeSchemaMutation(name);
        ColumnFamily cf = mutation.addOrGet(SystemKeyspace.SCHEMA_FUNCTIONS_TABLE);

        Composite prefix = SystemKeyspace.SchemaFunctionsTable.comparator.make(name.name, UDHelper.computeSignature(argTypes));
        int ldt = (int) (System.currentTimeMillis() / 1000);
        cf.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));

        return mutation;
    }

    public static Map<Composite, UDFunction> fromSchema(Row row)
    {
        UntypedResultSet results = QueryProcessor.resultify("SELECT * FROM system." + SystemKeyspace.SCHEMA_FUNCTIONS_TABLE, row);
        Map<Composite, UDFunction> udfs = new HashMap<>(results.size());
        for (UntypedResultSet.Row result : results)
            udfs.put(SystemKeyspace.SchemaFunctionsTable.comparator.make(result.getString("function_name"), result.getBlob("signature")),
                     fromSchema(result));
        return udfs;
    }

    public Mutation toSchemaUpdate(long timestamp)
    {
        Mutation mutation = makeSchemaMutation(name);
        ColumnFamily cf = mutation.addOrGet(SystemKeyspace.SCHEMA_FUNCTIONS_TABLE);

        Composite prefix = SystemKeyspace.SchemaFunctionsTable.comparator.make(name.name, UDHelper.computeSignature(argTypes));
        CFRowAdder adder = new CFRowAdder(cf, prefix, timestamp);

        adder.resetCollection("argument_names");
        adder.resetCollection("argument_types");
        adder.add("return_type", returnType.toString());
        adder.add("language", language);
        adder.add("body", body);
        adder.add("deterministic", deterministic);

        for (int i = 0; i < argNames.size(); i++)
        {
            adder.addListEntry("argument_names", argNames.get(i).bytes);
            adder.addListEntry("argument_types", argTypes.get(i).toString());
        }

        return mutation;
    }

    public static UDFunction fromSchema(UntypedResultSet.Row row)
    {
        String ksName = row.getString("keyspace_name");
        String functionName = row.getString("function_name");
        FunctionName name = new FunctionName(ksName, functionName);

        List<String> names = row.getList("argument_names", UTF8Type.instance);
        List<String> types = row.getList("argument_types", UTF8Type.instance);

        List<ColumnIdentifier> argNames;
        if (names == null)
            argNames = Collections.emptyList();
        else
        {
            argNames = new ArrayList<>(names.size());
            for (String arg : names)
                argNames.add(new ColumnIdentifier(arg, true));
        }

        List<AbstractType<?>> argTypes;
        if (types == null)
            argTypes = Collections.emptyList();
        else
        {
            argTypes = new ArrayList<>(types.size());
            for (String type : types)
                argTypes.add(parseType(type));
        }

        AbstractType<?> returnType = parseType(row.getString("return_type"));

        boolean deterministic = row.getBoolean("deterministic");
        String language = row.getString("language");
        String body = row.getString("body");

        try
        {
            return create(name, argNames, argTypes, returnType, language, body, deterministic);
        }
        catch (InvalidRequestException e)
        {
            logger.error(String.format("Cannot load function '%s' from schema: this function won't be available (on this node)", name), e);
            return createBrokenFunction(name, argNames, argTypes, returnType, language, body, e);
        }
    }

    private static AbstractType<?> parseType(String str)
    {
        // We only use this when reading the schema where we shouldn't get an error
        try
        {
            return TypeParser.parse(str);
        }
        catch (SyntaxException | ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof UDFunction))
            return false;

        UDFunction that = (UDFunction)o;
        return Objects.equal(this.name, that.name)
            && Functions.typeEquals(this.argTypes, that.argTypes)
            && Functions.typeEquals(this.returnType, that.returnType)
            && Objects.equal(this.argNames, that.argNames)
            && Objects.equal(this.language, that.language)
            && Objects.equal(this.body, that.body)
            && Objects.equal(this.deterministic, that.deterministic);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, argTypes, returnType, argNames, language, body, deterministic);
    }

    public void userTypeUpdated(String ksName, String typeName)
    {
        boolean updated = false;

        for (int i = 0; i < argDataTypes.length; i++)
        {
            DataType dataType = argDataTypes[i];
            if (dataType instanceof UserType)
            {
                UserType userType = (UserType) dataType;
                if (userType.getKeyspace().equals(ksName) && userType.getTypeName().equals(typeName))
                {
                    KSMetaData ksm = Schema.instance.getKSMetaData(ksName);
                    assert ksm != null;

                    org.apache.cassandra.db.marshal.UserType ut = ksm.userTypes.getType(ByteBufferUtil.bytes(typeName));

                    DataType newUserType = UDHelper.driverType(ut);
                    argDataTypes[i] = newUserType;

                    argTypes.set(i, ut);

                    updated = true;
                }
            }
        }

        if (updated)
            MigrationManager.announceNewFunction(this, true);
    }
}
