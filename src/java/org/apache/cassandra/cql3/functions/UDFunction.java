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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture; // checkstyle: permit this import
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.SchemaElement;
import org.apache.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.cql3.functions.types.TypeCodec;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Base class for User Defined Functions.
 */
public abstract class UDFunction extends AbstractFunction implements ScalarFunction, SchemaElement
{
    protected static final Logger logger = LoggerFactory.getLogger(UDFunction.class);

    static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    protected final List<ColumnIdentifier> argNames;

    protected final String language;
    protected final String body;

    protected final TypeCodec<Object>[] argCodecs;
    protected final TypeCodec<Object> returnCodec;
    protected final boolean calledOnNullInput;

    protected final UDFContext udfContext;

    //
    // Access to classes is controlled via allow and disallow lists.
    //
    // When a class is requested (both during compilation and runtime),
    // the allowedPatterns array is searched first, whether the
    // requested name matches one of the patterns. If not, nothing is
    // returned from the class-loader - meaning ClassNotFoundException
    // during runtime and "type could not resolved" during compilation.
    //
    // If an allowed pattern has been found, the disallowedPatterns
    // array is searched for a match. If a match is found, class-loader
    // rejects access. Otherwise the class/resource can be loaded.
    //
    private static final String[] allowedPatterns =
    {
    "com/google/common/reflect/TypeToken",
    "java/io/IOException.class",
    "java/io/Serializable.class",
    "java/lang/",
    "java/math/",
    "java/net/InetAddress.class",
    "java/net/Inet4Address.class",
    "java/net/Inet6Address.class",
    "java/net/UnknownHostException.class", // req'd by InetAddress
    "java/net/NetworkInterface.class", // req'd by InetAddress
    "java/net/SocketException.class", // req'd by InetAddress
    "java/nio/Buffer.class",
    "java/nio/ByteBuffer.class",
    "java/text/",
    "java/time/",
    "java/util/",
    "org/apache/cassandra/cql3/functions/types/",
    "org/apache/cassandra/cql3/functions/JavaUDF.class",
    "org/apache/cassandra/cql3/functions/UDFContext.class",
    "org/apache/cassandra/exceptions/",
    "org/apache/cassandra/transport/ProtocolVersion.class"
    };
    // Only need to disallow a pattern, if it would otherwise be allowed via allowedPatterns
    private static final String[] disallowedPatterns =
    {
    "com/datastax/driver/core/Cluster.class",
    "com/datastax/driver/core/Metrics.class",
    "com/datastax/driver/core/NettyOptions.class",
    "com/datastax/driver/core/Session.class",
    "com/datastax/driver/core/Statement.class",
    "com/datastax/driver/core/TimestampGenerator.class", // indirectly covers ServerSideTimestampGenerator + ThreadLocalMonotonicTimestampGenerator
    "java/lang/Compiler.class",
    "java/lang/InheritableThreadLocal.class",
    "java/lang/Package.class",
    "java/lang/Process.class",
    "java/lang/ProcessBuilder.class",
    "java/lang/ProcessEnvironment.class",
    "java/lang/ProcessImpl.class",
    "java/lang/Runnable.class",
    "java/lang/Runtime.class",
    "java/lang/Shutdown.class",
    "java/lang/Thread.class",
    "java/lang/ThreadGroup.class",
    "java/lang/ThreadLocal.class",
    "java/lang/instrument/",
    "java/lang/invoke/",
    "java/lang/management/",
    "java/lang/ref/",
    "java/lang/reflect/",
    "java/util/ServiceLoader.class",
    "java/util/Timer.class",
    "java/util/concurrent/",
    "java/util/function/",
    "java/util/jar/",
    "java/util/logging/",
    "java/util/prefs/",
    "java/util/spi/",
    "java/util/stream/",
    "java/util/zip/",
    };

    private static final String[] disallowedPatternsSyncUDF =
    {
    "java/lang/System.class"
    };

    static boolean secureResource(String resource)
    {
        while (resource.startsWith("/"))
            resource = resource.substring(1);

        for (String allowed : allowedPatterns)
            if (resource.startsWith(allowed))
            {
                // resource is in allowedPatterns, let's see if it is not explicitly disallowed
                for (String disallowed : disallowedPatterns)
                {
                    if (resource.startsWith(disallowed))
                    {
                        logger.trace("access denied: resource {}", resource);
                        return false;
                    }
                }
                if (!DatabaseDescriptor.enableUserDefinedFunctionsThreads() && !DatabaseDescriptor.allowExtraInsecureUDFs())
                {
                    for (String disallowed : disallowedPatternsSyncUDF)
                    {
                        if (resource.startsWith(disallowed))
                        {
                            logger.trace("access denied: resource {}", resource);
                            return false;
                        }
                    }
                }

                return true;
            }

        logger.trace("access denied: resource {}", resource);
        return false;
    }

    // setup the UDF class loader with a context class loader as a parent so that we have full control about what class/resource UDF uses
    static final ClassLoader udfClassLoader = new UDFClassLoader();

    protected UDFunction(FunctionName name,
                         List<ColumnIdentifier> argNames,
                         List<AbstractType<?>> argTypes,
                         AbstractType<?> returnType,
                         boolean calledOnNullInput,
                         String language,
                         String body)
    {
        this(name, argNames, argTypes, UDHelper.driverTypes(argTypes), returnType,
             UDHelper.driverType(returnType), calledOnNullInput, language, body);
    }

    protected UDFunction(FunctionName name,
                         List<ColumnIdentifier> argNames,
                         List<AbstractType<?>> argTypes,
                         DataType[] argDataTypes,
                         AbstractType<?> returnType,
                         DataType returnDataType,
                         boolean calledOnNullInput,
                         String language,
                         String body)
    {
        super(name, argTypes, returnType);
        assert new HashSet<>(argNames).size() == argNames.size() : "duplicate argument names";
        this.argNames = argNames;
        this.language = language;
        this.body = body;
        this.argCodecs = UDHelper.codecsFor(argDataTypes);
        this.returnCodec = UDHelper.codecFor(returnDataType);
        this.calledOnNullInput = calledOnNullInput;
        this.udfContext = new UDFContextImpl(argNames, argCodecs, returnCodec, name.keyspace);
    }

    public static UDFunction tryCreate(FunctionName name,
                                       List<ColumnIdentifier> argNames,
                                       List<AbstractType<?>> argTypes,
                                       AbstractType<?> returnType,
                                       boolean calledOnNullInput,
                                       String language,
                                       String body)
    {
        try
        {
            return create(name, argNames, argTypes, returnType, calledOnNullInput, language, body);
        }
        catch (InvalidRequestException e)
        {
            return createBrokenFunction(name, argNames, argTypes, returnType, calledOnNullInput, language, body, e);
        }
    }

    public static UDFunction create(FunctionName name,
                                    List<ColumnIdentifier> argNames,
                                    List<AbstractType<?>> argTypes,
                                    AbstractType<?> returnType,
                                    boolean calledOnNullInput,
                                    String language,
                                    String body)
    {
        assertUdfsEnabled(language);

        switch (language)
        {
            case "java":
                return new JavaBasedUDFunction(name, argNames, argTypes, returnType, calledOnNullInput, body);
            default:
                return new ScriptBasedUDFunction(name, argNames, argTypes, returnType, calledOnNullInput, language, body);
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
    public static UDFunction createBrokenFunction(FunctionName name,
                                                  List<ColumnIdentifier> argNames,
                                                  List<AbstractType<?>> argTypes,
                                                  AbstractType<?> returnType,
                                                  boolean calledOnNullInput,
                                                  String language,
                                                  String body,
                                                  InvalidRequestException reason)
    {
        return new UDFunction(name, argNames, argTypes, returnType, calledOnNullInput, language, body)
        {
            protected ExecutorService executor()
            {
                return ImmediateExecutor.INSTANCE;
            }

            protected Object executeAggregateUserDefined(ProtocolVersion protocolVersion, Object firstParam, List<ByteBuffer> parameters)
            {
                throw broken();
            }

            public ByteBuffer executeUserDefined(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                throw broken();
            }

            private InvalidRequestException broken()
            {
                return new InvalidRequestException(String.format("Function '%s' exists but hasn't been loaded successfully "
                                                                 + "for the following reason: %s. Please see the server log for details",
                                                                 this,
                                                                 reason.getMessage()));
            }
        };
    }

    @Override
    public SchemaElementType elementType()
    {
        return SchemaElementType.FUNCTION;
    }

    @Override
    public String toCqlString(boolean withInternals, boolean ifNotExists)
    {
        CqlBuilder builder = new CqlBuilder();
        builder.append("CREATE FUNCTION ");

        if (ifNotExists)
        {
            builder.append("IF NOT EXISTS ");
        }

        builder.append(name()).append("(");

        for (int i = 0, m = argNames().size(); i < m; i++)
        {
            if (i > 0)
                builder.append(", ");
            builder.append(argNames().get(i))
                   .append(' ')
                   .append(toCqlString(argTypes().get(i)));
        }

        builder.append(')')
               .newLine()
               .increaseIndent()
               .append(isCalledOnNullInput() ? "CALLED" : "RETURNS NULL")
               .append(" ON NULL INPUT")
               .newLine()
               .append("RETURNS ")
               .append(toCqlString(returnType()))
               .newLine()
               .append("LANGUAGE ")
               .append(language())
               .newLine()
               .append("AS $$")
               .append(body())
               .append("$$;");

        return builder.toString();
    }

    public boolean isPure()
    {
        // Right now, we have no way to check if an UDF is pure. Due to that we consider them as non pure to avoid any risk.
        return false;
    }

    public final ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
    {
        assertUdfsEnabled(language);

        if (!isCallableWrtNullable(parameters))
            return null;

        long tStart = nanoTime();
        parameters = makeEmptyParametersNull(parameters);

        try
        {
            // Using async UDF execution is expensive (adds about 100us overhead per invocation on a Core-i7 MBPr).
            ByteBuffer result = DatabaseDescriptor.enableUserDefinedFunctionsThreads()
                                ? executeAsync(protocolVersion, parameters)
                                : executeUserDefined(protocolVersion, parameters);

            Tracing.trace("Executed UDF {} in {}\u03bcs", name(), (nanoTime() - tStart) / 1000);
            return result;
        }
        catch (InvalidRequestException e)
        {
            throw e;
        }
        catch (Throwable t)
        {
            logger.trace("Invocation of user-defined function '{}' failed", this, t);
            if (t instanceof VirtualMachineError)
                throw (VirtualMachineError) t;
            throw FunctionExecutionException.create(this, t);
        }
    }

    /**
     * Like {@link ScalarFunction#execute(ProtocolVersion, List)} but the first parameter is already in non-serialized form.
     * Remaining parameters (2nd paramters and all others) are in {@code parameters}.
     * This is used to prevent superfluous (de)serialization of the state of aggregates.
     * Means: scalar functions of aggregates are called using this variant.
     */
    public final Object executeForAggregate(ProtocolVersion protocolVersion, Object firstParam, List<ByteBuffer> parameters)
    {
        assertUdfsEnabled(language);

        if (!calledOnNullInput && firstParam == null || !isCallableWrtNullable(parameters))
            return null;

        long tStart = nanoTime();
        parameters = makeEmptyParametersNull(parameters);

        try
        {
            // Using async UDF execution is expensive (adds about 100us overhead per invocation on a Core-i7 MBPr).
            Object result = DatabaseDescriptor.enableUserDefinedFunctionsThreads()
                                ? executeAggregateAsync(protocolVersion, firstParam, parameters)
                                : executeAggregateUserDefined(protocolVersion, firstParam, parameters);
            Tracing.trace("Executed UDF {} in {}\u03bcs", name(), (nanoTime() - tStart) / 1000);
            return result;
        }
        catch (InvalidRequestException e)
        {
            throw e;
        }
        catch (Throwable t)
        {
            logger.debug("Invocation of user-defined function '{}' failed", this, t);
            if (t instanceof VirtualMachineError)
                throw (VirtualMachineError) t;
            throw FunctionExecutionException.create(this, t);
        }
    }

    public static void assertUdfsEnabled(String language)
    {
        if (!DatabaseDescriptor.enableUserDefinedFunctions())
            throw new InvalidRequestException("User-defined functions are disabled in cassandra.yaml - set user_defined_functions_enabled=true to enable");
        if (!"java".equalsIgnoreCase(language) && !DatabaseDescriptor.enableScriptedUserDefinedFunctions())
            throw new InvalidRequestException("Scripted user-defined functions are disabled in cassandra.yaml - set scripted_user_defined_functions_enabled=true to enable if you are aware of the security risks");
    }

    static void initializeThread()
    {
        // Get the TypeCodec stuff in Java Driver initialized.
        // This is to get the classes loaded outside of the restricted sandbox's security context of a UDF.
        TypeCodec.inet().format(InetAddress.getLoopbackAddress());
        TypeCodec.ascii().format("");
    }

    private static final class ThreadIdAndCpuTime extends CompletableFuture<Object>
    {
        long threadId;
        long cpuTime;

        ThreadIdAndCpuTime()
        {
            // Looks weird?
            // This call "just" links this class to java.lang.management - otherwise UDFs (script UDFs) might fail due to
            //      java.security.AccessControlException: access denied: ("java.lang.RuntimePermission" "accessClassInPackage.java.lang.management")
            // because class loading would be deferred until setup() is executed - but setup() is called with
            // limited privileges.
            threadMXBean.getCurrentThreadCpuTime();
        }

        void setup()
        {
            this.threadId = Thread.currentThread().getId();
            this.cpuTime = threadMXBean.getCurrentThreadCpuTime();
            complete(null);
        }
    }

    private ByteBuffer executeAsync(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
    {
        ThreadIdAndCpuTime threadIdAndCpuTime = new ThreadIdAndCpuTime();

        return async(threadIdAndCpuTime, () -> {
            threadIdAndCpuTime.setup();
            return executeUserDefined(protocolVersion, parameters);
        });
    }

    /**
     * Like {@link #executeAsync(ProtocolVersion, List)} but the first parameter is already in non-serialized form.
     * Remaining parameters (2nd paramters and all others) are in {@code parameters}.
     * This is used to prevent superfluous (de)serialization of the state of aggregates.
     * Means: scalar functions of aggregates are called using this variant.
     */
    private Object executeAggregateAsync(ProtocolVersion protocolVersion, Object firstParam, List<ByteBuffer> parameters)
    {
        ThreadIdAndCpuTime threadIdAndCpuTime = new ThreadIdAndCpuTime();

        return async(threadIdAndCpuTime, () -> {
            threadIdAndCpuTime.setup();
            return executeAggregateUserDefined(protocolVersion, firstParam, parameters);
        });
    }

    private <T> T async(ThreadIdAndCpuTime threadIdAndCpuTime, Callable<T> callable)
    {
        Future<T> future = executor().submit(callable);

        try
        {
            if (DatabaseDescriptor.getUserDefinedFunctionWarnTimeout() > 0)
                try
                {
                    return future.get(DatabaseDescriptor.getUserDefinedFunctionWarnTimeout(), TimeUnit.MILLISECONDS);
                }
                catch (TimeoutException e)
                {

                    // log and emit a warning that UDF execution took long
                    String warn = String.format("User defined function %s ran longer than %dms", this, DatabaseDescriptor.getUserDefinedFunctionWarnTimeout());
                    logger.warn(warn);
                    ClientWarn.instance.warn(warn);
                }

            // retry with difference of warn-timeout to fail-timeout
            return future.get(DatabaseDescriptor.getUserDefinedFunctionFailTimeout() - DatabaseDescriptor.getUserDefinedFunctionWarnTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            Throwable c = e.getCause();
            if (c instanceof RuntimeException)
                throw (RuntimeException) c;
            throw new RuntimeException(c);
        }
        catch (TimeoutException e)
        {
            // retry a last time with the difference of UDF-fail-timeout to consumed CPU time (just in case execution hit a badly timed GC)
            try
            {
                //The threadIdAndCpuTime shouldn't take a long time to be set so this should return immediately
                threadIdAndCpuTime.get(1, TimeUnit.SECONDS);

                long cpuTimeMillis = threadMXBean.getThreadCpuTime(threadIdAndCpuTime.threadId) - threadIdAndCpuTime.cpuTime;
                cpuTimeMillis /= 1000000L;

                return future.get(Math.max(DatabaseDescriptor.getUserDefinedFunctionFailTimeout() - cpuTimeMillis, 0L),
                                  TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e1)
            {
                Thread.currentThread().interrupt();
                throw new UncheckedInterruptedException(e1);
            }
            catch (ExecutionException e1)
            {
                Throwable c = e.getCause();
                if (c instanceof RuntimeException)
                    throw (RuntimeException) c;
                throw new RuntimeException(c);
            }
            catch (TimeoutException e1)
            {
                TimeoutException cause = new TimeoutException(String.format("User defined function %s ran longer than %dms%s",
                                                                            this,
                                                                            DatabaseDescriptor.getUserDefinedFunctionFailTimeout(),
                                                                            DatabaseDescriptor.getUserFunctionTimeoutPolicy() == Config.UserFunctionTimeoutPolicy.ignore
                                                                            ? "" : " - will stop Cassandra VM"));
                FunctionExecutionException fe = FunctionExecutionException.create(this, cause);
                JVMStabilityInspector.userFunctionTimeout(cause);
                throw fe;
            }
        }
    }

    private List<ByteBuffer> makeEmptyParametersNull(List<ByteBuffer> parameters)
    {
        List<ByteBuffer> r = new ArrayList<>(parameters.size());
        for (int i = 0; i < parameters.size(); i++)
        {
            ByteBuffer param = parameters.get(i);
            r.add(UDHelper.isNullOrEmpty(argTypes.get(i), param)
                  ? null : param);
        }
        return r;
    }

    protected abstract ExecutorService executor();

    public boolean isCallableWrtNullable(List<ByteBuffer> parameters)
    {
        if (!calledOnNullInput)
            for (int i = 0; i < parameters.size(); i++)
                if (UDHelper.isNullOrEmpty(argTypes.get(i), parameters.get(i)))
                    return false;
        return true;
    }

    protected abstract ByteBuffer executeUserDefined(ProtocolVersion protocolVersion, List<ByteBuffer> parameters);

    protected abstract Object executeAggregateUserDefined(ProtocolVersion protocolVersion, Object firstParam, List<ByteBuffer> parameters);

    public boolean isAggregate()
    {
        return false;
    }

    public boolean isNative()
    {
        return false;
    }

    public boolean isCalledOnNullInput()
    {
        return calledOnNullInput;
    }

    public List<ColumnIdentifier> argNames()
    {
        return argNames;
    }

    public String body()
    {
        return body;
    }

    public String language()
    {
        return language;
    }

    /**
     * Used by UDF implementations (both Java code generated by {@link JavaBasedUDFunction}
     * and script executor {@link ScriptBasedUDFunction}) to convert the C*
     * serialized representation to the Java object representation.
     *
     * @param protocolVersion the native protocol version used for serialization
     * @param argIndex        index of the UDF input argument
     */
    protected Object compose(ProtocolVersion protocolVersion, int argIndex, ByteBuffer value)
    {
        return compose(argCodecs, protocolVersion, argIndex, value);
    }

    protected static Object compose(TypeCodec<Object>[] codecs, ProtocolVersion protocolVersion, int argIndex, ByteBuffer value)
    {
        return value == null ? null : UDHelper.deserialize(codecs[argIndex], protocolVersion, value);
    }

    /**
     * Used by UDF implementations (both Java code generated by {@link JavaBasedUDFunction}
     * and script executor {@link ScriptBasedUDFunction}) to convert the Java
     * object representation for the return value to the C* serialized representation.
     *
     * @param protocolVersion the native protocol version used for serialization
     */
    protected ByteBuffer decompose(ProtocolVersion protocolVersion, Object value)
    {
        return decompose(returnCodec, protocolVersion, value);
    }

    protected static ByteBuffer decompose(TypeCodec<Object> codec, ProtocolVersion protocolVersion, Object value)
    {
        return value == null ? null : UDHelper.serialize(codec, protocolVersion, value);
    }

    @Override
    public boolean referencesUserType(ByteBuffer name)
    {
        return any(argTypes(), t -> t.referencesUserType(name)) || returnType.referencesUserType(name);
    }

    public UDFunction withUpdatedUserType(UserType udt)
    {
        if (!referencesUserType(udt.name))
            return this;

        return tryCreate(name,
                         argNames,
                         Lists.newArrayList(transform(argTypes, t -> t.withUpdatedUserType(udt))),
                         returnType.withUpdatedUserType(udt),
                         calledOnNullInput,
                         language,
                         body);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof UDFunction))
            return false;

        UDFunction that = (UDFunction)o;
        return equalsWithoutTypes(that)
            && argTypes.equals(that.argTypes)
            && returnType.equals(that.returnType);
    }

    private boolean equalsWithoutTypes(UDFunction other)
    {
        return name.equals(other.name)
            && argTypes.size() == other.argTypes.size()
            && argNames.equals(other.argNames)
            && body.equals(other.body)
            && language.equals(other.language)
            && calledOnNullInput == other.calledOnNullInput;
    }

    @Override
    public Optional<Difference> compare(Function function)
    {
        if (!(function instanceof UDFunction))
            throw new IllegalArgumentException();

        UDFunction other = (UDFunction) function;

        if (!equalsWithoutTypes(other))
            return Optional.of(Difference.SHALLOW);

        boolean typesDifferDeeply = false;

        if (!returnType.equals(other.returnType))
        {
            if (returnType.asCQL3Type().toString().equals(other.returnType.asCQL3Type().toString()))
                typesDifferDeeply = true;
            else
                return Optional.of(Difference.SHALLOW);
        }

        for (int i = 0; i < argTypes().size(); i++)
        {
            AbstractType<?> thisType = argTypes.get(i);
            AbstractType<?> thatType = other.argTypes.get(i);

            if (!thisType.equals(thatType))
            {
                if (thisType.asCQL3Type().toString().equals(thatType.asCQL3Type().toString()))
                    typesDifferDeeply = true;
                else
                    return Optional.of(Difference.SHALLOW);
            }
        }

        return typesDifferDeeply ? Optional.of(Difference.DEEP) : Optional.empty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, Functions.typeHashCode(argTypes), Functions.typeHashCode(returnType), returnType, language, body);
    }

    private static class UDFClassLoader extends ClassLoader
    {
        // insecureClassLoader is the C* class loader
        static final ClassLoader insecureClassLoader = UDFClassLoader.class.getClassLoader();

        private UDFClassLoader()
        {
            super(insecureClassLoader);
        }

        public URL getResource(String name)
        {
            if (!secureResource(name))
                return null;
            return insecureClassLoader.getResource(name);
        }

        protected URL findResource(String name)
        {
            return getResource(name);
        }

        public Enumeration<URL> getResources(String name)
        {
            return Collections.emptyEnumeration();
        }

        protected Class<?> findClass(String name) throws ClassNotFoundException
        {
            if (!secureResource(name.replace('.', '/') + ".class"))
                throw new ClassNotFoundException(name);
            return insecureClassLoader.loadClass(name);
        }

        public Class<?> loadClass(String name) throws ClassNotFoundException
        {
            if (!secureResource(name.replace('.', '/') + ".class"))
                throw new ClassNotFoundException(name);
            return super.loadClass(name);
        }
    }
}
