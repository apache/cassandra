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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
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
import org.apache.cassandra.utils.FBUtilities;

/**
 * Base class for User Defined Functions.
 */
public abstract class UDFunction extends AbstractFunction implements ScalarFunction
{
    protected static final Logger logger = LoggerFactory.getLogger(UDFunction.class);

    // TODO make these c'tors and methods public in Java-Driver - see https://datastax-oss.atlassian.net/browse/JAVA-502
    static final MethodHandle methodParseOne;
    static
    {
        try
        {
            Class<?> cls = Class.forName("com.datastax.driver.core.CassandraTypeParser");
            Method m = cls.getDeclaredMethod("parseOne", String.class);
            m.setAccessible(true);
            methodParseOne = MethodHandles.lookup().unreflect(m);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Construct an array containing the Java classes for the given Java Driver {@link com.datastax.driver.core.DataType}s.
     *
     * @param dataTypes array with UDF argument types
     * @return array of same size with UDF arguments
     */
    public static Class<?>[] javaTypes(DataType[] dataTypes)
    {
        Class<?> paramTypes[] = new Class[dataTypes.length];
        for (int i = 0; i < paramTypes.length; i++)
            paramTypes[i] = dataTypes[i].asJavaClass();
        return paramTypes;
    }

    /**
     * Construct an array containing the Java Driver {@link com.datastax.driver.core.DataType}s for the
     * C* internal types.
     *
     * @param abstractTypes list with UDF argument types
     * @return array with argument types as {@link com.datastax.driver.core.DataType}
     */
    public static DataType[] driverTypes(List<AbstractType<?>> abstractTypes)
    {
        DataType[] argDataTypes = new DataType[abstractTypes.size()];
        for (int i = 0; i < argDataTypes.length; i++)
            argDataTypes[i] = driverType(abstractTypes.get(i));
        return argDataTypes;
    }

    /**
     * Returns the Java Driver {@link com.datastax.driver.core.DataType} for the C* internal type.
     */
    public static DataType driverType(AbstractType abstractType)
    {
        CQL3Type cqlType = abstractType.asCQL3Type();
        try
        {
            return (DataType) methodParseOne.invoke(cqlType.getType().toString());
        }
        catch (RuntimeException | Error e)
        {
            // immediately rethrow these...
            throw e;
        }
        catch (Throwable e)
        {
            throw new RuntimeException("cannot parse driver type " + cqlType.getType().toString(), e);
        }
    }

    // instance vars

    protected final List<ColumnIdentifier> argNames;

    protected final String language;
    protected final String body;
    protected final boolean deterministic;

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
        this(name, argNames, argTypes, driverTypes(argTypes), returnType,
             driverType(returnType), language, body, deterministic);
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

    public boolean isAggregate()
    {
        return false;
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

    // We allow method overloads, so a function is not uniquely identified by its name only, but
    // also by its argument types. To distinguish overloads of given function name in the schema 
    // we use a "signature" which is just a SHA-1 of it's argument types (we could replace that by
    // using a "signature" UDT that would be comprised of the function name and argument types,
    // which we could then use as clustering column. But as we haven't yet used UDT in system tables,
    // We'll left that decision to #6717).
    private static ByteBuffer computeSignature(List<AbstractType<?>> argTypes)
    {
        MessageDigest digest = FBUtilities.newMessageDigest("SHA-1");
        for (AbstractType<?> type : argTypes)
            digest.update(type.asCQL3Type().toString().getBytes(StandardCharsets.UTF_8));
        return ByteBuffer.wrap(digest.digest());
    }

    public boolean isPure()
    {
        return deterministic;
    }

    public boolean isNative()
    {
        return false;
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

        Composite prefix = SystemKeyspace.SchemaFunctionsTable.comparator.make(name.name, computeSignature(argTypes));
        int ldt = (int) (System.currentTimeMillis() / 1000);
        cf.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));

        return mutation;
    }

    public Mutation toSchemaUpdate(long timestamp)
    {
        Mutation mutation = makeSchemaMutation(name);
        ColumnFamily cf = mutation.addOrGet(SystemKeyspace.SCHEMA_FUNCTIONS_TABLE);

        Composite prefix = SystemKeyspace.SchemaFunctionsTable.comparator.make(name.name, computeSignature(argTypes));
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

    public static Map<Composite, UDFunction> fromSchema(Row row)
    {
        UntypedResultSet results = QueryProcessor.resultify("SELECT * FROM system." + SystemKeyspace.SCHEMA_FUNCTIONS_TABLE, row);
        Map<Composite, UDFunction> udfs = new HashMap<>(results.size());
        for (UntypedResultSet.Row result : results)
            udfs.put(SystemKeyspace.SchemaFunctionsTable.comparator.make(result.getString("function_name"), result.getBlob("signature")), fromSchema(result));
        return udfs;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof UDFunction))
            return false;

        UDFunction that = (UDFunction)o;
        return Objects.equal(this.name, that.name)
            && Objects.equal(this.argNames, that.argNames)
            && Functions.typeEquals(this.argTypes, that.argTypes)
            && Functions.typeEquals(this.returnType, that.returnType)
            && Objects.equal(this.language, that.language)
            && Objects.equal(this.body, that.body)
            && Objects.equal(this.deterministic, that.deterministic);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, argNames, argTypes, returnType, language, body, deterministic);
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

                    DataType newUserType = driverType(ut);
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
