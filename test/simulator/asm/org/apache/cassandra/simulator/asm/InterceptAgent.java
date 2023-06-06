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

package org.apache.cassandra.simulator.asm;

import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.ClassDefinition;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

import static org.apache.cassandra.simulator.asm.Flag.DETERMINISTIC;
import static org.apache.cassandra.simulator.asm.Flag.LOCK_SUPPORT;
import static org.apache.cassandra.simulator.asm.Flag.NO_PROXY_METHODS;
import static org.apache.cassandra.simulator.asm.Flag.SYSTEM_CLOCK;
import static org.apache.cassandra.simulator.asm.InterceptClasses.BYTECODE_VERSION;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.GETFIELD;
import static org.objectweb.asm.Opcodes.GETSTATIC;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.IRETURN;
import static org.objectweb.asm.Opcodes.RETURN;

// checkstyle: suppress below 'blockSystemPropertyUsage'

/**
 * A mechanism for weaving classes loaded by the bootstrap classloader that we cannot override.
 * The design supports weaving of the internals of these classes, and in future we may want to
 * weave LockSupport or the internals of other blocking concurrency primitives.
 *
 * Ultimately this wasn't necessary for the initial functionality, but we have maintained
 * the layout so that it will be easier to enable such functionality in future should it be needed.
 *
 * To this end, the asm package and simulator-asm.jar is as self-contained set of classes for performing
 * simulator byteweaving, and simulator-bootstrap.jar contains a self-contained class and interface for
 * replacing important system methods.
 */
public class InterceptAgent
{
    public static void premain(final String agentArgs, final Instrumentation instrumentation) throws UnmodifiableClassException, ClassNotFoundException, IOException
    {
        setup(agentArgs, instrumentation);
    }

    public void agentmain(final String agentArgs, final Instrumentation instrumentation) throws UnmodifiableClassException, ClassNotFoundException, IOException
    {
        setup(agentArgs, instrumentation);
    }

    private static void setup(final String agentArgs, final Instrumentation instrumentation) throws UnmodifiableClassException, ClassNotFoundException, IOException
    {
        instrumentation.addTransformer(new ClassFileTransformer()
        {
            @Override
            public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] bytecode) throws IllegalClassFormatException
            {
                if (className == null)
                    return null;

                if (className.equals("java/lang/Object"))
                    return transformObject(bytecode);

                if (className.equals("java/lang/Enum"))
                    return transformEnum(bytecode);

                if (className.equals("java/util/Random"))
                    return transformRandom(bytecode);

                if (className.equals("java/util/concurrent/ThreadLocalRandom"))
                    return transformThreadLocalRandom(bytecode);

                if (className.startsWith("java/util/concurrent/ConcurrentHashMap"))
                    return transformConcurrent(className, bytecode, DETERMINISTIC, NO_PROXY_METHODS);

                if (className.startsWith("java/util/concurrent/locks"))
                    return transformConcurrent(className, bytecode, SYSTEM_CLOCK, LOCK_SUPPORT, NO_PROXY_METHODS);

                return null;
            }
        });

        Pattern reloadPattern = Pattern.compile("java\\.(lang\\.Enum|util\\.concurrent\\.(locks\\..*|ConcurrentHashMap)|util\\.(concurrent\\.ThreadLocal)?Random|lang\\.Object)");
        List<ClassDefinition> redefine = new ArrayList<>();
        for (Class<?> loadedClass : instrumentation.getAllLoadedClasses())
        {
            if (reloadPattern.matcher(loadedClass.getName()).matches())
                redefine.add(new ClassDefinition(loadedClass, readDefinition(loadedClass)));
        }
        if (!redefine.isEmpty())
            instrumentation.redefineClasses(redefine.toArray(new ClassDefinition[0]));
    }

    private static byte[] readDefinition(Class<?> clazz) throws IOException
    {
        return readDefinition(clazz.getName().replaceAll("\\.", "/"));
    }

    private static byte[] readDefinition(String className) throws IOException
    {
        byte[] bytes = new byte[1024];
        try (InputStream in = ClassLoader.getSystemResourceAsStream(className + ".class"))
        {
            int count = 0;
            while (true)
            {
                int add = in.read(bytes, count, bytes.length - count);
                if (add < 0)
                    break;
                if (add == 0)
                    bytes = Arrays.copyOf(bytes, bytes.length * 2);
                count += add;
            }
            return Arrays.copyOf(bytes, count);
        }
    }

    /**
     * We don't want Object.toString() to invoke our overridden identityHashCode by virtue of invoking some overridden hashCode()
     * So we overwrite Object.toString() to replace calls to Object.hashCode() with direct calls to System.identityHashCode()
     */
    private static byte[] transformObject(byte[] bytes)
    {
        class ObjectVisitor extends ClassVisitor
        {
            public ObjectVisitor(int api, ClassVisitor classVisitor)
            {
                super(api, classVisitor);
            }

            @Override
            public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions)
            {
                if (descriptor.equals("()Ljava/lang/String;") && name.equals("toString"))
                    return Utils.deterministicToString(super.visitMethod(access, name, descriptor, signature, exceptions));
                else
                    return super.visitMethod(access, name, descriptor, signature, exceptions);
            }
        }
        return transform(bytes, ObjectVisitor::new);
    }

    /**
     * We want Enum to have a deterministic hashCode() so we simply forward calls to ordinal()
     */
    private static byte[] transformEnum(byte[] bytes)
    {
        class EnumVisitor extends ClassVisitor
        {
            public EnumVisitor(int api, ClassVisitor classVisitor)
            {
                super(api, classVisitor);
            }

            @Override
            public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions)
            {
                if (descriptor.equals("()I") && name.equals("hashCode"))
                {
                    MethodVisitor visitor = super.visitMethod(access, name, descriptor, signature, exceptions);
                    visitor.visitLabel(new Label());
                    visitor.visitIntInsn(ALOAD, 0);
                    visitor.visitFieldInsn(GETFIELD, "java/lang/Enum", "ordinal", "I");
                    visitor.visitInsn(IRETURN);
                    visitor.visitLabel(new Label());
                    visitor.visitMaxs(1, 1);
                    visitor.visitEnd();

                    return new MethodVisitor(BYTECODE_VERSION) {};
                }
                else
                {
                    return super.visitMethod(access, name, descriptor, signature, exceptions);
                }
            }
        }
        return transform(bytes, EnumVisitor::new);
    }

    /**
     * We want Random to be initialised deterministically, so we modify the default constructor to fetch
     * some deterministically generated seed to pass to its seed constructor
     */
    private static byte[] transformRandom(byte[] bytes)
    {
        class RandomVisitor extends ClassVisitor
        {
            public RandomVisitor(int api, ClassVisitor classVisitor)
            {
                super(api, classVisitor);
            }

            @Override
            public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions)
            {
                if (descriptor.equals("()V") && name.equals("<init>"))
                {
                    MethodVisitor visitor = super.visitMethod(access, name, descriptor, signature, exceptions);
                    visitor.visitLabel(new Label());
                    visitor.visitIntInsn(ALOAD, 0);
                    visitor.visitMethodInsn(INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", "randomSeed", "()J", false);
                    visitor.visitMethodInsn(INVOKESPECIAL, "java/util/Random", "<init>", "(J)V", false);
                    visitor.visitInsn(RETURN);
                    visitor.visitLabel(new Label());
                    visitor.visitMaxs(3, 1);
                    visitor.visitEnd();

                    return new MethodVisitor(BYTECODE_VERSION) {};
                }
                else
                {
                    return super.visitMethod(access, name, descriptor, signature, exceptions);
                }
            }
        }
        return transform(bytes, RandomVisitor::new);
    }

    /**
     * We require ThreadLocalRandom to be deterministic, so we modify its initialisation method to invoke a
     * global deterministic random value generator
     */
    private static byte[] transformThreadLocalRandom(byte[] bytes)
    {
        class ThreadLocalRandomVisitor extends ClassVisitor
        {
            // CassandraRelevantProperties is not available to us here
            final boolean determinismCheck = System.getProperty("cassandra.test.simulator.determinismcheck", "none").matches("relaxed|strict");

            public ThreadLocalRandomVisitor(int api, ClassVisitor classVisitor)
            {
                super(api, classVisitor);
            }

            String unsafeDescriptor;
            String unsafeFieldName;

            @Override
            public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value)
            {
                if (descriptor.equals("Lsun/misc/Unsafe;") || descriptor.equals("Ljdk/internal/misc/Unsafe;"))
                {
                    unsafeFieldName = name;
                    unsafeDescriptor = descriptor;
                }
                return super.visitField(access, name, descriptor, signature, value);
            }

            @Override
            public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions)
            {
                if (descriptor.equals("()V") && name.equals("localInit"))
                {
                    if (unsafeFieldName == null)
                    {
                        String version = System.getProperty("java.version");
                        if (version.startsWith("11.")) { unsafeFieldName = "U"; unsafeDescriptor = "Ljdk/internal/misc/Unsafe;"; }
                        else if (version.startsWith("1.8")) { unsafeFieldName = "UNSAFE"; unsafeDescriptor = "Lsun/misc/Unsafe;"; }
                        else throw new AssertionError("Unsupported Java Version");
                    }

                    MethodVisitor visitor = super.visitMethod(access, name, descriptor, signature, exceptions);
                    visitor.visitLabel(new Label());
                    visitor.visitIntInsn(ALOAD, 0);
                    visitor.visitFieldInsn(GETSTATIC, "java/util/concurrent/ThreadLocalRandom", unsafeFieldName, unsafeDescriptor);
                    visitor.visitMethodInsn(INVOKESTATIC, "java/lang/Thread", "currentThread", "()Ljava/lang/Thread;", false);
                    visitor.visitFieldInsn(GETSTATIC, "java/util/concurrent/ThreadLocalRandom", "SEED", "J");
                    visitor.visitMethodInsn(INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", "randomSeed", "()J", false);

                    String unsafeClass = Utils.descriptorToClassName(unsafeDescriptor);
                    visitor.visitMethodInsn(INVOKEVIRTUAL, unsafeClass, "putLong", "(Ljava/lang/Object;JJ)V", false);
                    visitor.visitFieldInsn(GETSTATIC, "java/util/concurrent/ThreadLocalRandom", unsafeFieldName, unsafeDescriptor);
                    visitor.visitMethodInsn(INVOKESTATIC, "java/lang/Thread", "currentThread", "()Ljava/lang/Thread;", false);
                    visitor.visitFieldInsn(GETSTATIC, "java/util/concurrent/ThreadLocalRandom", "PROBE", "J");
                    visitor.visitLdcInsn(0);
                    visitor.visitMethodInsn(INVOKEVIRTUAL, unsafeClass, "putInt", "(Ljava/lang/Object;JI)V", false);
                    visitor.visitInsn(RETURN);
                    visitor.visitLabel(new Label());
                    visitor.visitMaxs(6, 1);
                    visitor.visitEnd();

                    return new MethodVisitor(BYTECODE_VERSION) {};
                }
                else
                {
                    MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);
                    if (determinismCheck && (name.equals("nextSeed") || name.equals("nextSecondarySeed")))
                        mv = new ThreadLocalRandomCheckTransformer(api, mv);
                    return mv;
                }
            }
        }
        return transform(bytes, ThreadLocalRandomVisitor::new);
    }

    private static byte[] transform(byte[] bytes, BiFunction<Integer, ClassWriter, ClassVisitor> constructor)
    {
        ClassWriter out = new ClassWriter(0);
        ClassReader in = new ClassReader(bytes);
        ClassVisitor transform = constructor.apply(BYTECODE_VERSION, out);
        in.accept(transform, 0);
        return out.toByteArray();
    }

    private static byte[] transformConcurrent(String className, byte[] bytes, Flag flag, Flag ... flags)
    {
        ClassTransformer transformer = new ClassTransformer(BYTECODE_VERSION, className, EnumSet.of(flag, flags), null);
        transformer.readAndTransform(bytes);
        if (!transformer.isTransformed())
            return null;
        return transformer.toBytes();
    }
}
