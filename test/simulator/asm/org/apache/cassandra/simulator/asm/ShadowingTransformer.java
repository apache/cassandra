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

import java.util.EnumSet;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;
import org.objectweb.asm.TypePath;

/**
 * A SORT OF general purpose facility for creating a copy of a system class that we want to transform
 * and use in place of the system class without transforming the system class itself.
 *
 * NOTE that this does not implement this translation perfectly, so care must be taken when extending its usage
 * Some things not handled:
 *   - generic type signatures in class files
 *   - 
 *
 * While it is possible and safe in principle to modify ConcurrentHashMap in particular, in practice it messes
 * with class loading, as ConcurrentHashMap is used widely within the JDK, including for things like class loaders
 * and method handle caching. It seemed altogether more tractable and safe to selectively replace ConcurrentHashMap
 * with a shadowed variety.
 *
 * This approach makes some rough assumptions, namely that any public method on the root class should accept the
 * shadowed type, but that any inner class may safely use the shadow type.
 */
public class ShadowingTransformer extends ClassTransformer
{
    final String originalType;
    final String originalRootType;
    final String shadowRootType;
    final String originalOuterTypePrefix;
    final String shadowOuterTypePrefix;
    String originalSuperName;

    ShadowingTransformer(int api, String originalType, String shadowType, String originalRootType, String shadowRootType, String originalOuterTypePrefix, String shadowOuterTypePrefix, EnumSet<Flag> flags, ChanceSupplier monitorDelayChance, NemesisGenerator nemesis, NemesisFieldKind.Selector nemesisFieldSelector, Hashcode insertHashcode)
    {
        super(api, shadowType, flags, monitorDelayChance, nemesis, nemesisFieldSelector, insertHashcode, null);
        this.originalType = originalType;
        this.originalRootType = originalRootType;
        this.shadowRootType = shadowRootType;
        this.originalOuterTypePrefix = originalOuterTypePrefix;
        this.shadowOuterTypePrefix = shadowOuterTypePrefix;
    }

    private String toShadowType(String type)
    {
        if (type.startsWith("["))
            return toShadowTypeDescriptor(type);
        else if (type.equals(originalRootType))
            type = shadowRootType;
        else if (type.startsWith(originalOuterTypePrefix))
            type = shadowOuterTypePrefix + type.substring(originalOuterTypePrefix.length());
        else
            return type;

        witness(TransformationKind.SHADOW);
        return type;
    }

    private String toShadowTypeDescriptor(String owner)
    {
        return toShadowTypeDescriptor(owner, false);
    }

    private String toShadowTypeDescriptor(String desc, boolean innerTypeOnly)
    {
        int i = 0;
        while (i < desc.length() && desc.charAt(i) == '[') ++i;
        if (desc.charAt(i) != 'L')
            return desc;
        ++i;

        if (!innerTypeOnly && desc.regionMatches(i, originalRootType, 0, originalRootType.length()) && desc.length() == originalRootType.length() + 1 + i && desc.charAt(i + originalRootType.length()) == ';')
            desc = desc.substring(0, i) + shadowRootType + ';';
        else if (desc.regionMatches(i, originalOuterTypePrefix, 0, originalOuterTypePrefix.length()))
            desc = desc.substring(0, i) + shadowOuterTypePrefix + desc.substring(i + originalOuterTypePrefix.length());
        else
            return desc;

        witness(TransformationKind.SHADOW);
        return desc;
    }

    private Type toShadowTypeDescriptor(Type type)
    {
        String in = type.getDescriptor();
        String out = toShadowTypeDescriptor(in, false);
        if (in == out) return type;
        return Type.getType(out);
    }

    private Type toShadowInnerTypeDescriptor(Type type)
    {
        String in = type.getDescriptor();
        String out = toShadowTypeDescriptor(in, true);
        if (in == out) return type;
        return Type.getType(out);
    }

    Object[] toShadowTypes(Object[] in)
    {
        Object[] out = null;
        for (int i = 0 ; i < in.length ; ++i)
        {
            if (in[i] instanceof String)
            {
                // TODO (broader correctness): in some cases we want the original type, and others the new type
                String inv = (String) in[i];
                String outv = toShadowType(inv);
                if (inv != outv)
                {
                    if (out == null)
                    {
                        out = new Object[in.length];
                        System.arraycopy(in, 0, out, 0, i);
                    }
                    out[i] = outv;
                    continue;
                }
            }

            if (out != null)
                out[i] = in[i];
        }
        return out != null ? out : in;
    }

    String methodDescriptorToShadowInnerArgumentTypes(String descriptor)
    {
        Type ret = toShadowTypeDescriptor(Type.getReturnType(descriptor));
        Type[] args = Type.getArgumentTypes(descriptor);
        for (int i = 0 ; i < args.length ; ++i)
            args[i] = toShadowInnerTypeDescriptor(args[i]);
        return Type.getMethodDescriptor(ret, args);
    }

    String methodDescriptorToShadowTypes(String descriptor)
    {
        Type ret = toShadowTypeDescriptor(Type.getReturnType(descriptor));
        Type[] args = Type.getArgumentTypes(descriptor);
        for (int i = 0 ; i < args.length ; ++i)
            args[i] = toShadowTypeDescriptor(args[i]);
        return Type.getMethodDescriptor(ret, args);
    }

    class ShadowingMethodVisitor extends MethodVisitor
    {
        final boolean isConstructor;
        public ShadowingMethodVisitor(int api, boolean isConstructor, MethodVisitor methodVisitor)
        {
            super(api, methodVisitor);
            this.isConstructor = isConstructor;
        }

        @Override
        public AnnotationVisitor visitTypeAnnotation(int typeRef, TypePath typePath, String descriptor, boolean visible)
        {
            return super.visitTypeAnnotation(typeRef, typePath, descriptor, visible);
        }

        @Override
        public void visitFieldInsn(int opcode, String owner, String name, String descriptor)
        {
            super.visitFieldInsn(opcode, toShadowType(owner), name, toShadowTypeDescriptor(descriptor));
        }

        @Override
        public void visitTypeInsn(int opcode, String type)
        {
            // TODO (broader correctness): in some cases we want the original type, and others the new type
            super.visitTypeInsn(opcode, toShadowType(type));
        }

        @Override
        public void visitLocalVariable(String name, String descriptor, String signature, Label start, Label end, int index)
        {
            super.visitLocalVariable(name, toShadowTypeDescriptor(descriptor), signature, start, end, index);
        }

        @Override
        public void visitFrame(int type, int numLocal, Object[] local, int numStack, Object[] stack)
        {
            super.visitFrame(type, numLocal, toShadowTypes(local), numStack, toShadowTypes(stack));
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface)
        {
            // TODO (broader correctness): this is incorrect, but will do for ConcurrentHashMap (no general guarantee of same constructors)
            if (owner.equals(originalRootType)) descriptor = methodDescriptorToShadowInnerArgumentTypes(descriptor);
            else descriptor = methodDescriptorToShadowTypes(descriptor);
            if (isConstructor && name.equals("<init>") && owner.equals(originalSuperName) && originalType.equals(originalRootType)) owner = originalRootType;
            else owner = toShadowType(owner);
            super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
        }

        @Override
        public void visitInvokeDynamicInsn(String name, String descriptor, Handle bootstrapMethodHandle, Object... bootstrapMethodArguments)
        {
            if (bootstrapMethodHandle.getOwner().startsWith(originalRootType))
            {
                bootstrapMethodHandle = new Handle(bootstrapMethodHandle.getTag(), toShadowType(bootstrapMethodHandle.getOwner()),
                                                   bootstrapMethodHandle.getName(), bootstrapMethodHandle.getDesc(),
                                                   bootstrapMethodHandle.isInterface());
            }
            super.visitInvokeDynamicInsn(name, descriptor, bootstrapMethodHandle, bootstrapMethodArguments);
        }

        @Override
        public void visitLdcInsn(Object value)
        {
            if (value instanceof Type)
                value = toShadowTypeDescriptor((Type) value);
            super.visitLdcInsn(value);
        }
    }

    @Override
    public void visitInnerClass(String name, String outerName, String innerName, int access)
    {
        super.visitInnerClass(name, toShadowType(outerName), innerName, access);
    }

    @Override
    public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value)
    {
        return super.visitField(access, name, toShadowTypeDescriptor(descriptor), signature, value);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions)
    {
        if (originalType.equals(originalRootType)) descriptor = methodDescriptorToShadowInnerArgumentTypes(descriptor);
        else descriptor = methodDescriptorToShadowTypes(descriptor);
        return new ShadowingMethodVisitor(api, name.equals("<init>"), super.visitMethod(access, name, descriptor, signature, exceptions));
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces)
    {
        originalSuperName = superName;
        if (originalType.equals(originalRootType))
        {
            superName = name;
            name = shadowRootType;
        }
        else
        {
            name = toShadowType(name);
            superName = toShadowType(superName);
        }

        super.visit(version, access, name, signature, superName, interfaces);
    }

}
