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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nullable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ConstantDynamic;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import org.apache.cassandra.exceptions.UnrecoverableIllegalStateException;

import static org.apache.cassandra.utils.FBUtilities.ASM_BYTECODE_VERSION;

/**
 * Verifies Java UDF byte code.
 * Checks for disallowed method calls (e.g. {@code Object.finalize()}),
 * additional code in the constructor,
 * use of {@code synchronized} blocks,
 * too many methods.
 * With a {@link UDFSandboxPolicy}, as in the bytecode UDF sandbox, also checks every type, method and field used
 * against its allowlist.
 */
public final class UDFByteCodeVerifier
{

    public static final String JAVA_UDF_NAME = JavaUDF.class.getName().replace('.', '/');
    public static final String OBJECT_NAME = Object.class.getName().replace('.', '/');
    public static final String CTOR_SIG = "(Lorg/apache/cassandra/cql3/functions/UDFDataType;Lorg/apache/cassandra/cql3/functions/UDFContext;)V";
    private static final String UDF_CONTEXT_FIELD = "udfContext";

    // UDFs cannot construct these or their subclasses, as JVMStabilityInspector stops the node on OutOfMemoryError
    // and UnrecoverableIllegalStateException. Class.newInstance and reflection are denied separately.
    private static final Set<Class<? extends Throwable>> FATAL_THROWABLES = ImmutableSet.of(VirtualMachineError.class,
                                                                                            UnrecoverableIllegalStateException.class);

    private final Set<String> disallowedClasses = new HashSet<>();
    private final Multimap<String, String> disallowedMethodCalls = HashMultimap.create();
    private final List<String> disallowedPackages = new ArrayList<>();
    @Nullable
    private final UDFSandboxPolicy policy;

    public UDFByteCodeVerifier()
    {
        this(null);
    }

    /**
     * @param policy the allowlist to enforce, or null to only apply the denylist
     */
    public UDFByteCodeVerifier(@Nullable UDFSandboxPolicy policy)
    {
        this.policy = policy;
        addDisallowedMethodCall(OBJECT_NAME, "clone");
        addDisallowedMethodCall(OBJECT_NAME, "finalize");
        addDisallowedMethodCall(OBJECT_NAME, "notify");
        addDisallowedMethodCall(OBJECT_NAME, "notifyAll");
        addDisallowedMethodCall(OBJECT_NAME, "wait");
    }

    public UDFByteCodeVerifier addDisallowedClass(String clazz)
    {
        disallowedClasses.add(clazz);
        return this;
    }

    public UDFByteCodeVerifier addDisallowedMethodCall(String clazz, String method)
    {
        disallowedMethodCalls.put(clazz, method);
        return this;
    }

    public UDFByteCodeVerifier addDisallowedPackage(String pkg)
    {
        disallowedPackages.add(pkg);
        return this;
    }

    /**
     * Verifies a class without knowing the name of its implementation method, so only the constructor,
     * {@code executeImpl} and {@code executeAggregateImpl} may be declared.
     */
    public Set<String> verify(String clsName, byte[] bytes)
    {
        return verify(clsName, bytes, null, null);
    }

    /**
     * @param implName name of the generated method holding the UDF body, or null
     * @param implDesc descriptor of that method, or null
     * @return the set of errors, empty if the class is valid
     */
    public Set<String> verify(String clsName, byte[] bytes, @Nullable String implName, @Nullable String implDesc)
    {
        String clsNameSl = clsName.replace('.', '/');
        Set<String> errors = new TreeSet<>(); // it's a TreeSet for unit tests
        ClassVisitor classVisitor = new ClassVisitor(ASM_BYTECODE_VERSION)
        {
            public FieldVisitor visitField(int access, String name, String desc, String signature, Object value)
            {
                errors.add("field declared: " + name);
                return null;
            }

            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions)
            {
                if ("<init>".equals(name) && CTOR_SIG.equals(desc))
                {
                    if (Opcodes.ACC_PUBLIC != access)
                        errors.add("constructor not public");
                    // allowed constructor - JavaUDF(UDFDataType returnType, UDFContext udfContext)
                    return new ConstructorVisitor(errors);
                }
                if ("executeImpl".equals(name) && "(Lorg/apache/cassandra/cql3/functions/Arguments;)Ljava/nio/ByteBuffer;".equals(desc))
                {
                    if (Opcodes.ACC_PROTECTED != access)
                        errors.add("executeImpl not protected");
                    // the executeImpl method - ByteBuffer executeImpl(Arguments arguments)
                    return new ExecuteImplVisitor(errors, clsNameSl, implName, implDesc);
                }
                if ("executeAggregateImpl".equals(name) && "(Ljava/lang/Object;Lorg/apache/cassandra/cql3/functions/Arguments;)Ljava/lang/Object;".equals(desc))
                {
                    if (Opcodes.ACC_PROTECTED != access)
                        errors.add("executeAggregateImpl not protected");
                    // the executeImpl method - ByteBuffer executeImpl(Object state, Arguments arguments)
                    return new ExecuteImplVisitor(errors, clsNameSl, implName, implDesc);
                }
                if (implName != null && implName.equals(name) && desc.equals(implDesc))
                {
                    return new ExecuteImplVisitor(errors, clsNameSl, implName, implDesc);
                }
                if ("<clinit>".equals(name))
                {
                    errors.add("static initializer declared");
                }
                else
                {
                    errors.add("not allowed method declared: " + name + desc);
                    return new ExecuteImplVisitor(errors, clsNameSl, implName, implDesc);
                }
                return null;
            }

            public void visit(int version, int access, String name, String signature, String superName, String[] interfaces)
            {
                if (!JAVA_UDF_NAME.equals(superName))
                {
                    errors.add("class does not extend " + JavaUDF.class.getName());
                }
                if (access != (Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER))
                {
                    errors.add("class not public final");
                }
                super.visit(version, access, name, signature, superName, interfaces);
            }

            public void visitInnerClass(String name, String outerName, String innerName, int access)
            {
                if (clsNameSl.equals(outerName)) // outerName might be null, which is true for anonymous inner classes
                    errors.add("class declared as inner class");
                super.visitInnerClass(name, outerName, innerName, access);
            }

            // With a policy only. JavaBasedUDFunction rejects any class besides the UDF class in either case.
            public void visitNestHost(String nestHost)
            {
                if (policy != null)
                    errors.add("class declared as nest member");
            }

            public void visitNestMember(String nestMember)
            {
                if (policy != null)
                    errors.add("class declared as nest host");
            }
        };

        ClassReader classReader = new ClassReader(bytes);
        classReader.accept(classVisitor, ClassReader.SKIP_DEBUG);

        return errors;
    }

    private static boolean isFatalThrowable(String internalName)
    {
        Class<?> cls;
        try
        {
            cls = Class.forName(internalName.replace('/', '.'), false, UDFByteCodeVerifier.class.getClassLoader());
        }
        catch (ClassNotFoundException | LinkageError e)
        {
            return false;
        }
        for (Class<?> fatal : FATAL_THROWABLES)
            if (fatal.isAssignableFrom(cls))
                return true;
        return false;
    }

    private static String javaName(String internalName)
    {
        return internalName.replace('/', '.');
    }

    private class ExecuteImplVisitor extends MethodVisitor
    {
        private final Set<String> errors;
        private final String clsName;
        private final String implName;
        private final String implDesc;

        ExecuteImplVisitor(Set<String> errors, String clsName, String implName, String implDesc)
        {
            super(ASM_BYTECODE_VERSION);
            this.errors = errors;
            this.clsName = clsName;
            this.implName = implName;
            this.implDesc = implDesc;
        }

        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf)
        {
            if (disallowedClasses.contains(owner))
            {
                errorDisallowed(owner, name);
            }
            Collection<String> disallowed = disallowedMethodCalls.get(owner);
            if (disallowed != null && disallowed.contains(name))
            {
                errorDisallowed(owner, name);
            }
            if (!JAVA_UDF_NAME.equals(owner))
            {
                for (String pkg : disallowedPackages)
                {
                    if (owner.startsWith(pkg))
                        errorDisallowed(owner, name);
                }
            }
            if (policy != null && !isMethodAllowed(owner, name, desc))
                errorDisallowed(owner, name);
            super.visitMethodInsn(opcode, owner, name, desc, itf);
        }

        private boolean isMethodAllowed(String owner, String name, String desc)
        {
            if (owner.equals(clsName))
                return (name.equals(implName) && desc.equals(implDesc)) || "decompose".equals(name);
            // super.decompose(...)
            if (owner.equals(JAVA_UDF_NAME))
                return "decompose".equals(name);
            if (owner.startsWith("["))
                return ("clone".equals(name) || "getClass".equals(name)) && policy.isTypeAllowed(owner);
            return policy.isMethodAllowed(owner, name, desc);
        }

        private void errorDisallowed(String owner, String name)
        {
            errors.add("call to " + javaName(owner) + '.' + name + "()");
        }

        public void visitFieldInsn(int opcode, String owner, String name, String desc)
        {
            if (policy != null)
            {
                boolean read = opcode == Opcodes.GETFIELD || opcode == Opcodes.GETSTATIC;
                // super.udfContext names JavaUDF as the owner
                boolean allowed = owner.equals(clsName) || owner.equals(JAVA_UDF_NAME)
                                  ? opcode == Opcodes.GETFIELD && UDF_CONTEXT_FIELD.equals(name)
                                  : read && policy.isFieldAllowed(owner, name, desc);
                if (!allowed)
                    errors.add("access to field " + javaName(owner) + '.' + name);
            }
            super.visitFieldInsn(opcode, owner, name, desc);
        }

        public void visitTypeInsn(int opcode, String type)
        {
            if (opcode == Opcodes.NEW && isFatalThrowable(type))
                errorType(type);
            checkType(type);
            super.visitTypeInsn(opcode, type);
        }

        public void visitMultiANewArrayInsn(String descriptor, int numDimensions)
        {
            checkType(descriptor);
            super.visitMultiANewArrayInsn(descriptor, numDimensions);
        }

        public void visitTryCatchBlock(Label start, Label end, Label handler, String type)
        {
            if (type != null)
                checkType(type);
            super.visitTryCatchBlock(start, end, handler, type);
        }

        public void visitLdcInsn(Object value)
        {
            if (policy != null)
            {
                if (value instanceof Type)
                {
                    Type type = (Type) value;
                    if (type.getSort() == Type.METHOD)
                        errors.add("use of unsupported constant");
                    else
                        checkType(type.getDescriptor().startsWith("[") ? type.getDescriptor() : type.getInternalName());
                }
                else if (value instanceof Handle || value instanceof ConstantDynamic)
                {
                    errors.add("use of unsupported constant");
                }
            }
            super.visitLdcInsn(value);
        }

        public void visitInvokeDynamicInsn(String name, String descriptor, Handle bootstrapMethodHandle, Object... bootstrapMethodArguments)
        {
            if (policy != null)
                errors.add("use of invokedynamic");
            super.visitInvokeDynamicInsn(name, descriptor, bootstrapMethodHandle, bootstrapMethodArguments);
        }

        private void checkType(String internalNameOrDescriptor)
        {
            if (policy != null && !internalNameOrDescriptor.equals(clsName) && !policy.isTypeAllowed(internalNameOrDescriptor))
                errorType(internalNameOrDescriptor);
        }

        private void errorType(String internalNameOrDescriptor)
        {
            String name = internalNameOrDescriptor.startsWith("[")
                          ? Type.getType(internalNameOrDescriptor).getElementType().getClassName()
                          : javaName(internalNameOrDescriptor);
            errors.add("use of type " + name);
        }

        public void visitInsn(int opcode)
        {
            switch (opcode)
            {
                case Opcodes.MONITORENTER:
                case Opcodes.MONITOREXIT:
                    errors.add("use of synchronized");
                    break;
            }
            super.visitInsn(opcode);
        }
    }

    /**
     * The constructor may only load its arguments, call {@code JavaUDF}'s constructor and return: it runs on the thread
     * that creates the function, outside UDF threads and their timeouts.
     */
    private static class ConstructorVisitor extends MethodVisitor
    {
        private final Set<String> errors;

        ConstructorVisitor(Set<String> errors)
        {
            super(ASM_BYTECODE_VERSION);
            this.errors = errors;
        }

        public void visitInvokeDynamicInsn(String name, String desc, Handle bsm, Object... bsmArgs)
        {
            initializerDeclared();
            super.visitInvokeDynamicInsn(name, desc, bsm, bsmArgs);
        }

        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf)
        {
            if (!(Opcodes.INVOKESPECIAL == opcode && JAVA_UDF_NAME.equals(owner) && "<init>".equals(name) && CTOR_SIG.equals(desc)))
                initializerDeclared();
            super.visitMethodInsn(opcode, owner, name, desc, itf);
        }

        public void visitInsn(int opcode)
        {
            if (Opcodes.RETURN != opcode)
                initializerDeclared();
            super.visitInsn(opcode);
        }

        public void visitVarInsn(int opcode, int var)
        {
            if (opcode != Opcodes.ALOAD)
                initializerDeclared();
            super.visitVarInsn(opcode, var);
        }

        public void visitFieldInsn(int opcode, String owner, String name, String descriptor)
        {
            initializerDeclared();
            super.visitFieldInsn(opcode, owner, name, descriptor);
        }

        public void visitTypeInsn(int opcode, String type)
        {
            initializerDeclared();
            super.visitTypeInsn(opcode, type);
        }

        public void visitLdcInsn(Object value)
        {
            initializerDeclared();
            super.visitLdcInsn(value);
        }

        public void visitIntInsn(int opcode, int operand)
        {
            initializerDeclared();
            super.visitIntInsn(opcode, operand);
        }

        public void visitIincInsn(int var, int increment)
        {
            initializerDeclared();
            super.visitIincInsn(var, increment);
        }

        public void visitJumpInsn(int opcode, Label label)
        {
            initializerDeclared();
            super.visitJumpInsn(opcode, label);
        }

        public void visitTableSwitchInsn(int min, int max, Label dflt, Label... labels)
        {
            initializerDeclared();
            super.visitTableSwitchInsn(min, max, dflt, labels);
        }

        public void visitLookupSwitchInsn(Label dflt, int[] keys, Label[] labels)
        {
            initializerDeclared();
            super.visitLookupSwitchInsn(dflt, keys, labels);
        }

        public void visitMultiANewArrayInsn(String descriptor, int numDimensions)
        {
            initializerDeclared();
            super.visitMultiANewArrayInsn(descriptor, numDimensions);
        }

        public void visitTryCatchBlock(Label start, Label end, Label handler, String type)
        {
            initializerDeclared();
            super.visitTryCatchBlock(start, end, handler, type);
        }

        private void initializerDeclared()
        {
            errors.add("initializer declared");
        }
    }
}
