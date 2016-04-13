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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.cert.Certificate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.TypeCodec;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;
import org.eclipse.jdt.core.compiler.IProblem;
import org.eclipse.jdt.internal.compiler.*;
import org.eclipse.jdt.internal.compiler.Compiler;
import org.eclipse.jdt.internal.compiler.classfmt.ClassFileReader;
import org.eclipse.jdt.internal.compiler.classfmt.ClassFormatException;
import org.eclipse.jdt.internal.compiler.env.ICompilationUnit;
import org.eclipse.jdt.internal.compiler.env.INameEnvironment;
import org.eclipse.jdt.internal.compiler.env.NameEnvironmentAnswer;
import org.eclipse.jdt.internal.compiler.impl.CompilerOptions;
import org.eclipse.jdt.internal.compiler.problem.DefaultProblemFactory;

public final class JavaBasedUDFunction extends UDFunction
{
    private static final String BASE_PACKAGE = "org.apache.cassandra.cql3.udf.gen";

    private static final Pattern JAVA_LANG_PREFIX = Pattern.compile("\\bjava\\.lang\\.");

    static final Logger logger = LoggerFactory.getLogger(JavaBasedUDFunction.class);

    private static final AtomicInteger classSequence = new AtomicInteger();

    // use a JVM standard ExecutorService as DebuggableThreadPoolExecutor references internal
    // classes, which triggers AccessControlException from the UDF sandbox
    private static final UDFExecutorService executor =
        new UDFExecutorService(new NamedThreadFactory("UserDefinedFunctions",
                                                      Thread.MIN_PRIORITY,
                                                      udfClassLoader,
                                                      new SecurityThreadGroup("UserDefinedFunctions", null, UDFunction::initializeThread)),
                               "userfunction");

    private static final EcjTargetClassLoader targetClassLoader = new EcjTargetClassLoader();

    private static final UDFByteCodeVerifier udfByteCodeVerifier = new UDFByteCodeVerifier();

    private static final ProtectionDomain protectionDomain;

    private static final IErrorHandlingPolicy errorHandlingPolicy = DefaultErrorHandlingPolicies.proceedWithAllProblems();
    private static final IProblemFactory problemFactory = new DefaultProblemFactory(Locale.ENGLISH);
    private static final CompilerOptions compilerOptions;

    /**
     * Poor man's template - just a text file splitted at '#' chars.
     * Each string at an even index is a constant string (just copied),
     * each string at an odd index is an 'instruction'.
     */
    private static final String[] javaSourceTemplate;

    static
    {
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/Class", "forName");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/Class", "getClassLoader");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/Class", "getResource");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/Class", "getResourceAsStream");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "clearAssertionStatus");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getResource");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getResourceAsStream");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getResources");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getSystemClassLoader");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getSystemResource");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getSystemResourceAsStream");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "getSystemResources");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "loadClass");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "setClassAssertionStatus");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "setDefaultAssertionStatus");
        udfByteCodeVerifier.addDisallowedMethodCall("java/lang/ClassLoader", "setPackageAssertionStatus");
        udfByteCodeVerifier.addDisallowedMethodCall("java/nio/ByteBuffer", "allocateDirect");
        for (String ia : new String[]{"java/net/InetAddress", "java/net/Inet4Address", "java/net/Inet6Address"})
        {
            // static method, probably performing DNS lookups (despite SecurityManager)
            udfByteCodeVerifier.addDisallowedMethodCall(ia, "getByAddress");
            udfByteCodeVerifier.addDisallowedMethodCall(ia, "getAllByName");
            udfByteCodeVerifier.addDisallowedMethodCall(ia, "getByName");
            udfByteCodeVerifier.addDisallowedMethodCall(ia, "getLocalHost");
            // instance methods, probably performing DNS lookups (despite SecurityManager)
            udfByteCodeVerifier.addDisallowedMethodCall(ia, "getHostName");
            udfByteCodeVerifier.addDisallowedMethodCall(ia, "getCanonicalHostName");
            // ICMP PING
            udfByteCodeVerifier.addDisallowedMethodCall(ia, "isReachable");
        }
        udfByteCodeVerifier.addDisallowedClass("java/net/NetworkInterface");
        udfByteCodeVerifier.addDisallowedClass("java/net/SocketException");

        Map<String, String> settings = new HashMap<>();
        settings.put(CompilerOptions.OPTION_LineNumberAttribute,
                     CompilerOptions.GENERATE);
        settings.put(CompilerOptions.OPTION_SourceFileAttribute,
                     CompilerOptions.DISABLED);
        settings.put(CompilerOptions.OPTION_ReportDeprecation,
                     CompilerOptions.IGNORE);
        settings.put(CompilerOptions.OPTION_Source,
                     CompilerOptions.VERSION_1_8);
        settings.put(CompilerOptions.OPTION_TargetPlatform,
                     CompilerOptions.VERSION_1_8);

        compilerOptions = new CompilerOptions(settings);
        compilerOptions.parseLiteralExpressionsAsConstants = true;

        try (InputStream input = JavaBasedUDFunction.class.getResource("JavaSourceUDF.txt").openConnection().getInputStream())
        {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            FBUtilities.copy(input, output, Long.MAX_VALUE);
            String template = output.toString();

            StringTokenizer st = new StringTokenizer(template, "#");
            javaSourceTemplate = new String[st.countTokens()];
            for (int i = 0; st.hasMoreElements(); i++)
                javaSourceTemplate[i] = st.nextToken();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        CodeSource codeSource;
        try
        {
            codeSource = new CodeSource(new URL("udf", "localhost", 0, "/java", new URLStreamHandler()
            {
                protected URLConnection openConnection(URL u)
                {
                    return null;
                }
            }), (Certificate[])null);
        }
        catch (MalformedURLException e)
        {
            throw new RuntimeException(e);
        }

        protectionDomain = new ProtectionDomain(codeSource, ThreadAwareSecurityManager.noPermissions, targetClassLoader, null);
    }

    private final JavaUDF javaUDF;

    JavaBasedUDFunction(FunctionName name, List<ColumnIdentifier> argNames, List<AbstractType<?>> argTypes,
                        AbstractType<?> returnType, boolean calledOnNullInput, String body)
    {
        super(name, argNames, argTypes, UDHelper.driverTypes(argTypes),
              returnType, UDHelper.driverType(returnType), calledOnNullInput, "java", body);

        // javaParamTypes is just the Java representation for argTypes resp. argDataTypes
        TypeToken<?>[] javaParamTypes = UDHelper.typeTokens(argCodecs, calledOnNullInput);
        // javaReturnType is just the Java representation for returnType resp. returnDataType
        TypeToken<?> javaReturnType = returnCodec.getJavaType();

        // put each UDF in a separate package to prevent cross-UDF code access
        String pkgName = BASE_PACKAGE + '.' + generateClassName(name, 'p');
        String clsName = generateClassName(name, 'C');

        String executeInternalName = generateClassName(name, 'x');

        StringBuilder javaSourceBuilder = new StringBuilder();
        int lineOffset = 1;
        for (int i = 0; i < javaSourceTemplate.length; i++)
        {
            String s = javaSourceTemplate[i];

            // strings at odd indexes are 'instructions'
            if ((i & 1) == 1)
            {
                switch (s)
                {
                    case "package_name":
                        s = pkgName;
                        break;
                    case "class_name":
                        s = clsName;
                        break;
                    case "body":
                        lineOffset = countNewlines(javaSourceBuilder);
                        s = body;
                        break;
                    case "arguments":
                        s = generateArguments(javaParamTypes, argNames);
                        break;
                    case "argument_list":
                        s = generateArgumentList(javaParamTypes, argNames);
                        break;
                    case "return_type":
                        s = javaSourceName(javaReturnType);
                        break;
                    case "execute_internal_name":
                        s = executeInternalName;
                        break;
                }
            }

            javaSourceBuilder.append(s);
        }

        String targetClassName = pkgName + '.' + clsName;

        String javaSource = javaSourceBuilder.toString();

        logger.trace("Compiling Java source UDF '{}' as class '{}' using source:\n{}", name, targetClassName, javaSource);

        try
        {
            EcjCompilationUnit compilationUnit = new EcjCompilationUnit(javaSource, targetClassName);

            Compiler compiler = new Compiler(compilationUnit,
                                                                               errorHandlingPolicy,
                                                                               compilerOptions,
                                                                               compilationUnit,
                                                                               problemFactory);
            compiler.compile(new ICompilationUnit[]{ compilationUnit });

            if (compilationUnit.problemList != null && !compilationUnit.problemList.isEmpty())
            {
                boolean fullSource = false;
                StringBuilder problems = new StringBuilder();
                for (IProblem problem : compilationUnit.problemList)
                {
                    long ln = problem.getSourceLineNumber() - lineOffset;
                    if (ln < 1L)
                    {
                        if (problem.isError())
                        {
                            // if generated source around UDF source provided by the user is buggy,
                            // this code is appended.
                            problems.append("GENERATED SOURCE ERROR: line ")
                                    .append(problem.getSourceLineNumber())
                                    .append(" (in generated source): ")
                                    .append(problem.getMessage())
                                    .append('\n');
                            fullSource = true;
                        }
                    }
                    else
                    {
                        problems.append("Line ")
                                .append(Long.toString(ln))
                                .append(": ")
                                .append(problem.getMessage())
                                .append('\n');
                    }
                }

                if (fullSource)
                    throw new InvalidRequestException("Java source compilation failed:\n" + problems + "\n generated source:\n" + javaSource);
                else
                    throw new InvalidRequestException("Java source compilation failed:\n" + problems);
            }

            // Verify the UDF bytecode against use of probably dangerous code
            Set<String> errors = udfByteCodeVerifier.verify(targetClassLoader.classData(targetClassName));
            String validDeclare = "not allowed method declared: " + executeInternalName + '(';
            for (Iterator<String> i = errors.iterator(); i.hasNext();)
            {
                String error = i.next();
                // we generate a random name of the private, internal execute method, which is detected by the byte-code verifier
                if (error.startsWith(validDeclare))
                    i.remove();
            }
            if (!errors.isEmpty())
                throw new InvalidRequestException("Java UDF validation failed: " + errors);

            // Load the class and create a new instance of it
            Thread thread = Thread.currentThread();
            ClassLoader orig = thread.getContextClassLoader();
            try
            {
                thread.setContextClassLoader(UDFunction.udfClassLoader);
                // Execute UDF intiialization from UDF class loader

                Class cls = Class.forName(targetClassName, false, targetClassLoader);

                // Count only non-synthetic methods, so code coverage instrumentation doesn't cause a miscount
                int nonSyntheticMethodCount = 0;
                for (Method m : cls.getDeclaredMethods())
                {
                    if (!m.isSynthetic())
                    {
                        nonSyntheticMethodCount += 1;
                    }
                }

                if (nonSyntheticMethodCount != 2 || cls.getDeclaredConstructors().length != 1)
                    throw new InvalidRequestException("Check your source to not define additional Java methods or constructors");
                MethodType methodType = MethodType.methodType(void.class)
                                                  .appendParameterTypes(TypeCodec.class, TypeCodec[].class, UDFContext.class);
                MethodHandle ctor = MethodHandles.lookup().findConstructor(cls, methodType);
                this.javaUDF = (JavaUDF) ctor.invokeWithArguments(returnCodec, argCodecs, udfContext);
            }
            finally
            {
                thread.setContextClassLoader(orig);
            }
        }
        catch (InvocationTargetException e)
        {
            // in case of an ITE, use the cause
            throw new InvalidRequestException(String.format("Could not compile function '%s' from Java source: %s", name, e.getCause()));
        }
        catch (VirtualMachineError e)
        {
            throw e;
        }
        catch (Throwable e)
        {
            throw new InvalidRequestException(String.format("Could not compile function '%s' from Java source: %s", name, e));
        }
    }

    protected ExecutorService executor()
    {
        return executor;
    }

    protected ByteBuffer executeUserDefined(int protocolVersion, List<ByteBuffer> params)
    {
        return javaUDF.executeImpl(protocolVersion, params);
    }


    private static int countNewlines(StringBuilder javaSource)
    {
        int ln = 0;
        for (int i = 0; i < javaSource.length(); i++)
            if (javaSource.charAt(i) == '\n')
                ln++;
        return ln;
    }

    private static String generateClassName(FunctionName name, char prefix)
    {
        String qualifiedName = name.toString();

        StringBuilder sb = new StringBuilder(qualifiedName.length() + 10);
        sb.append(prefix);
        for (int i = 0; i < qualifiedName.length(); i++)
        {
            char c = qualifiedName.charAt(i);
            if (Character.isJavaIdentifierPart(c))
                sb.append(c);
            else
                sb.append(Integer.toHexString(((short)c)&0xffff));
        }
        sb.append('_')
          .append(ThreadLocalRandom.current().nextInt() & 0xffffff)
          .append('_')
          .append(classSequence.incrementAndGet());
        return sb.toString();
    }

    @VisibleForTesting
    public static String javaSourceName(TypeToken<?> type)
    {
        String n = type.toString();
        return JAVA_LANG_PREFIX.matcher(n).replaceAll("");
    }

    private static String generateArgumentList(TypeToken<?>[] paramTypes, List<ColumnIdentifier> argNames)
    {
        // initial builder size can just be a guess (prevent temp object allocations)
        StringBuilder code = new StringBuilder(32 * paramTypes.length);
        for (int i = 0; i < paramTypes.length; i++)
        {
            if (i > 0)
                code.append(", ");
            code.append(javaSourceName(paramTypes[i]))
                .append(' ')
                .append(argNames.get(i));
        }
        return code.toString();
    }

    private static String generateArguments(TypeToken<?>[] paramTypes, List<ColumnIdentifier> argNames)
    {
        StringBuilder code = new StringBuilder(64 * paramTypes.length);
        for (int i = 0; i < paramTypes.length; i++)
        {
            if (i > 0)
                code.append(",\n");

            if (logger.isTraceEnabled())
                code.append("            /* parameter '").append(argNames.get(i)).append("' */\n");

            code
                // cast to Java type
                .append("            (").append(javaSourceName(paramTypes[i])).append(") ")
                // generate object representation of input parameter (call UDFunction.compose)
                .append(composeMethod(paramTypes[i])).append("(protocolVersion, ").append(i).append(", params.get(").append(i).append("))");
        }
        return code.toString();
    }

    private static String composeMethod(TypeToken<?> type)
    {
        return (type.isPrimitive()) ? ("super.compose_" + type.getRawType().getName()) : "super.compose";
    }

    // Java source UDFs are a very simple compilation task, which allows us to let one class implement
    // all interfaces required by ECJ.
    static final class EcjCompilationUnit implements ICompilationUnit, ICompilerRequestor, INameEnvironment
    {
        List<IProblem> problemList;
        private final String className;
        private final char[] sourceCode;

        EcjCompilationUnit(String sourceCode, String className)
        {
            this.className = className;
            this.sourceCode = sourceCode.toCharArray();
        }

        // ICompilationUnit

        @Override
        public char[] getFileName()
        {
            return sourceCode;
        }

        @Override
        public char[] getContents()
        {
            return sourceCode;
        }

        @Override
        public char[] getMainTypeName()
        {
            int dot = className.lastIndexOf('.');
            return ((dot > 0) ? className.substring(dot + 1) : className).toCharArray();
        }

        @Override
        public char[][] getPackageName()
        {
            StringTokenizer izer = new StringTokenizer(className, ".");
            char[][] result = new char[izer.countTokens() - 1][];
            for (int i = 0; i < result.length; i++)
                result[i] = izer.nextToken().toCharArray();
            return result;
        }

        @Override
        public boolean ignoreOptionalProblems()
        {
            return false;
        }

        // ICompilerRequestor

        @Override
        public void acceptResult(CompilationResult result)
        {
            if (result.hasErrors())
            {
                IProblem[] problems = result.getProblems();
                if (problemList == null)
                    problemList = new ArrayList<>(problems.length);
                Collections.addAll(problemList, problems);
            }
            else
            {
                ClassFile[] classFiles = result.getClassFiles();
                for (ClassFile classFile : classFiles)
                    targetClassLoader.addClass(className, classFile.getBytes());
            }
        }

        // INameEnvironment

        @Override
        public NameEnvironmentAnswer findType(char[][] compoundTypeName)
        {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < compoundTypeName.length; i++)
            {
                if (i > 0)
                    result.append('.');
                result.append(compoundTypeName[i]);
            }
            return findType(result.toString());
        }

        @Override
        public NameEnvironmentAnswer findType(char[] typeName, char[][] packageName)
        {
            StringBuilder result = new StringBuilder();
            int i = 0;
            for (; i < packageName.length; i++)
            {
                if (i > 0)
                    result.append('.');
                result.append(packageName[i]);
            }
            if (i > 0)
                result.append('.');
            result.append(typeName);
            return findType(result.toString());
        }

        private NameEnvironmentAnswer findType(String className)
        {
            if (className.equals(this.className))
            {
                return new NameEnvironmentAnswer(this, null);
            }

            String resourceName = className.replace('.', '/') + ".class";

            try (InputStream is = UDFunction.udfClassLoader.getResourceAsStream(resourceName))
            {
                if (is != null)
                {
                    byte[] classBytes = ByteStreams.toByteArray(is);
                    char[] fileName = className.toCharArray();
                    ClassFileReader classFileReader = new ClassFileReader(classBytes, fileName, true);
                    return new NameEnvironmentAnswer(classFileReader, null);
                }
            }
            catch (IOException | ClassFormatException exc)
            {
                throw new RuntimeException(exc);
            }
            return null;
        }

        private boolean isPackage(String result)
        {
            if (result.equals(this.className))
                return false;
            String resourceName = result.replace('.', '/') + ".class";
            try (InputStream is = UDFunction.udfClassLoader.getResourceAsStream(resourceName))
            {
                return is == null;
            }
            catch (IOException e)
            {
                // we are here, since close on is failed. That means it was not null
                return false;
            }
        }

        @Override
        public boolean isPackage(char[][] parentPackageName, char[] packageName)
        {
            StringBuilder result = new StringBuilder();
            int i = 0;
            if (parentPackageName != null)
                for (; i < parentPackageName.length; i++)
                {
                    if (i > 0)
                        result.append('.');
                    result.append(parentPackageName[i]);
                }

            if (Character.isUpperCase(packageName[0]) && !isPackage(result.toString()))
                return false;
            if (i > 0)
                result.append('.');
            result.append(packageName);

            return isPackage(result.toString());
        }

        @Override
        public void cleanup()
        {
        }
    }

    static final class EcjTargetClassLoader extends SecureClassLoader
    {
        EcjTargetClassLoader()
        {
            super(UDFunction.udfClassLoader);
        }

        // This map is usually empty.
        // It only contains data *during* UDF compilation but not during runtime.
        //
        // addClass() is invoked by ECJ after successful compilation of the generated Java source.
        // loadClass(targetClassName) is invoked by buildUDF() after ECJ returned from successful compilation.
        //
        private final Map<String, byte[]> classes = new ConcurrentHashMap<>();

        void addClass(String className, byte[] classData)
        {
            classes.put(className, classData);
        }

        byte[] classData(String className)
        {
            return classes.get(className);
        }

        protected Class<?> findClass(String name) throws ClassNotFoundException
        {
            // remove the class binary - it's only used once - so it's wasting heap
            byte[] classData = classes.remove(name);

            if (classData != null)
                return defineClass(name, classData, 0, classData.length, protectionDomain);

            return getParent().loadClass(name);
        }

        protected PermissionCollection getPermissions(CodeSource codesource)
        {
            return ThreadAwareSecurityManager.noPermissions;
        }
    }}
