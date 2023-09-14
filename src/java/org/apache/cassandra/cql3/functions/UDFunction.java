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
public abstract class UDFunction extends UserFunction implements ScalarFunction
{
    protected static final Logger logger = LoggerFactory.getLogger(UDFunction.class);

    static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    protected final List<ColumnIdentifier> argNames;

    protected final String language;
    protected final String body;

    protected final List<UDFDataType> argumentTypes;
    protected final UDFDataType resultType;
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
    "org/apache/cassandra/cql3/functions/Arguments.class",
    "org/apache/cassandra/cql3/functions/UDFDataType.class",
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
        super(name, argTypes, returnType);
        assert new HashSet<>(argNames).size() == argNames.size() : "duplicate argument names";
        this.argNames = argNames;
        this.language = language;
        this.body = body;
        this.argumentTypes = UDFDataType.wrap(argTypes, !calledOnNullInput);
        this.resultType = UDFDataType.wrap(returnType, !calledOnNullInput);
        this.calledOnNullInput = calledOnNullInput;
        this.udfContext = new UDFContextImpl(argNames, argumentTypes, resultType, name.keyspace);
    }

    @Override
    public Arguments newArguments(ProtocolVersion version)
    {
        return FunctionArguments.newInstanceForUdf(version, argumentTypes);
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

        return new JavaBasedUDFunction(name, argNames, argTypes, returnType, calledOnNullInput, body);
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

            @Override
            protected Object executeAggregateUserDefined(Object firstParam, Arguments arguments)
            {
                throw broken();
            }

            @Override
            public ByteBuffer executeUserDefined(Arguments arguments)
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

    @Override
    public boolean isPure()
    {
        // Right now, we have no way to check if an UDF is pure. Due to that we consider them as non pure to avoid any risk.
        return false;
    }

    @Override
    public final ByteBuffer execute(Arguments arguments)
    {
        assertUdfsEnabled(language);

        if (!isCallableWrtNullable(arguments))
            return null;

        long tStart = nanoTime();

        try
        {
            // Using async UDF execution is expensive (adds about 100us overhead per invocation on a Core-i7 MBPr).
            ByteBuffer result = DatabaseDescriptor.enableUserDefinedFunctionsThreads()
                                ? executeAsync(arguments)
                                : executeUserDefined(arguments);

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

    public final Object executeForAggregate(Object state, Arguments arguments)
    {
        assertUdfsEnabled(language);

        if (!calledOnNullInput && state == null || !isCallableWrtNullable(arguments))
            return null;

        long tStart = nanoTime();

        try
        {
            // Using async UDF execution is expensive (adds about 100us overhead per invocation on a Core-i7 MBPr).
            Object result = DatabaseDescriptor.enableUserDefinedFunctionsThreads()
                                ? executeAggregateAsync(state, arguments)
                                : executeAggregateUserDefined(state, arguments);
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
        if (!"java".equalsIgnoreCase(language))
            throw new InvalidRequestException("Currently only Java UDFs are available in Cassandra. For more information - CASSANDRA-18252 and CASSANDRA-17281");
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

    private ByteBuffer executeAsync(Arguments arguments)
    {
        ThreadIdAndCpuTime threadIdAndCpuTime = new ThreadIdAndCpuTime();

        return async(threadIdAndCpuTime, () -> {
            threadIdAndCpuTime.setup();
            return executeUserDefined(arguments);
        });
    }

    private Object executeAggregateAsync(Object state, Arguments arguments)
    {
        ThreadIdAndCpuTime threadIdAndCpuTime = new ThreadIdAndCpuTime();

        return async(threadIdAndCpuTime, () -> {
            threadIdAndCpuTime.setup();
            return executeAggregateUserDefined(state, arguments);
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

    protected abstract ExecutorService executor();

    public boolean isCallableWrtNullable(Arguments arguments)
    {
        return calledOnNullInput || !arguments.containsNulls();
    }

    protected abstract ByteBuffer executeUserDefined(Arguments arguments);

    protected abstract Object executeAggregateUserDefined(Object firstParam, Arguments arguments);

    public boolean isAggregate()
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
     * Used by UDF implementations (both Java code generated by {@link JavaBasedUDFunction}) to convert the Java
     * object representation for the return value to the C* serialized representation.
     *
     * @param protocolVersion the native protocol version used for serialization
     */
    protected ByteBuffer decompose(ProtocolVersion protocolVersion, Object value)
    {
        return resultType.decompose(protocolVersion, value);
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
        return Objects.hashCode(name, UserFunctions.typeHashCode(argTypes), returnType, language, body);
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
