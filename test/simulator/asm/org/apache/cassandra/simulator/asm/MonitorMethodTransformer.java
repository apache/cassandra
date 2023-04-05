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

import java.util.Comparator;
import java.util.ListIterator;

import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.FrameNode;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.LocalVariableNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TryCatchBlockNode;

import static org.apache.cassandra.simulator.asm.TransformationKind.MONITOR;

/**
 * For synchronized methods, we generate a new method that contains the source method's body, and the original method
 * instead invoke preMonitorEnter before invoking the new hidden method.
 */
class MonitorMethodTransformer extends MethodNode
{
    private final String className;
    private final MethodWriterSink methodWriterSink;
    private final ChanceSupplier monitorDelayChance;
    private final String baseName;
    private final boolean isInstanceMethod;
    private int returnCode;

    int maxLocalParams; // double counts long/double to match asm spec

    public MonitorMethodTransformer(MethodWriterSink methodWriterSink, String className, int api, int access, String name, String descriptor, String signature, String[] exceptions, ChanceSupplier monitorDelayChance)
    {
        super(api, access, name, descriptor, signature, exceptions);
        this.methodWriterSink = methodWriterSink;
        this.className = className;
        this.baseName = name;
        this.isInstanceMethod = (access & Opcodes.ACC_STATIC) == 0;
        this.monitorDelayChance = monitorDelayChance;
    }

    @Override
    public void visitInsn(int opcode)
    {
        switch (opcode)
        {
            case Opcodes.RETURN:
            case Opcodes.ARETURN:
            case Opcodes.IRETURN:
            case Opcodes.FRETURN:
            case Opcodes.LRETURN:
            case Opcodes.DRETURN:
                if (returnCode != 0) assert returnCode == opcode;
                else returnCode = opcode;
        }
        super.visitInsn(opcode);
    }

    int returnCode()
    {
        return returnCode;
    }

    // TODO (cleanup): this _should_ be possible to determine purely from the method signature
    int loadParamsAndReturnInvokeCode()
    {
        if (isInstanceMethod)
            instructions.add(new IntInsnNode(Opcodes.ALOAD, 0));

        ListIterator<LocalVariableNode> it = localVariables.listIterator();
        while (it.hasNext())
        {
            LocalVariableNode cur = it.next();
            if (cur.index < maxLocalParams)
            {
                if (!isInstanceMethod || cur.index > 0)
                {
                    int opcode;
                    switch (cur.desc.charAt(0))
                    {
                        case 'L':
                        case '[':
                            opcode = Opcodes.ALOAD;
                            break;
                        case 'J':
                            opcode = Opcodes.LLOAD;
                            break;
                        case 'D':
                            opcode = Opcodes.DLOAD;
                            break;
                        case 'F':
                            opcode = Opcodes.FLOAD;
                            break;
                        default:
                            opcode = Opcodes.ILOAD;
                            break;
                    }
                    instructions.add(new IntInsnNode(opcode, cur.index));
                }
            }
        }

        int invokeCode;
        if (isInstanceMethod && (access & Opcodes.ACC_PRIVATE) != 0) invokeCode = Opcodes.INVOKESPECIAL;
        else if (isInstanceMethod) invokeCode = Opcodes.INVOKEVIRTUAL;
        else invokeCode = Opcodes.INVOKESTATIC;
        return invokeCode;
    }

    void pushRef()
    {
        if (isInstanceMethod) instructions.add(new IntInsnNode(Opcodes.ALOAD, 0));
        else instructions.add(new LdcInsnNode(org.objectweb.asm.Type.getType('L' + className + ';')));
    }

    void pop()
    {
        instructions.add(new InsnNode(Opcodes.POP));
    }

    void invokePreMonitorExit()
    {
        pushRef();
        instructions.add(new MethodInsnNode(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", "preMonitorExit", "(Ljava/lang/Object;)Ljava/lang/Object;", false));
    }

    void invokePreMonitorEnter()
    {
        pushRef();
        instructions.add(new LdcInsnNode(monitorDelayChance.get()));
        instructions.add(new MethodInsnNode(Opcodes.INVOKESTATIC, "org/apache/cassandra/simulator/systems/InterceptorOfSystemMethods$Global", "preMonitorEnter", "(Ljava/lang/Object;F)Ljava/lang/Object;", false));
    }

    void invokeMonitor(int insn)
    {
        instructions.add(new InsnNode(insn));
    }

    void reset(Label start, Label end)
    {
        instructions.clear();
        tryCatchBlocks.clear();
        if (visibleLocalVariableAnnotations != null)
            visibleLocalVariableAnnotations.clear();
        if (invisibleLocalVariableAnnotations != null)
            invisibleLocalVariableAnnotations.clear();

        Type[] args = Type.getArgumentTypes(desc);
        // remove all local variables that aren't parameters and the `this` parameter
        maxLocals = args.length == 1 && Type.VOID_TYPE.equals(args[0]) ? 0 : args.length;
        if (isInstanceMethod) ++maxLocals;

        // sort our local variables and remove those that aren't parameters
        localVariables.sort(Comparator.comparingInt(c -> c.index));
        ListIterator<LocalVariableNode> it = localVariables.listIterator();
        while (it.hasNext())
        {
            LocalVariableNode cur = it.next();
            if (cur.index >= maxLocals)
            {
                it.remove();
            }
            else
            {
                it.set(new LocalVariableNode(cur.name, cur.desc, cur.signature, getLabelNode(start), getLabelNode(end), cur.index));
                switch (cur.desc.charAt(0))
                {
                    case 'J':
                    case 'D':
                        // doubles and longs take two local variable positions
                        ++maxLocals;
                }
            }
        }

        // save the number of pure-parameters for use elsewhere
        maxLocalParams = maxLocals;
    }

    void writeOriginal()
    {
        access &= ~Opcodes.ACC_SYNCHRONIZED;
        access |= Opcodes.ACC_SYNTHETIC;
        name = baseName + "$unsync";
        methodWriterSink.writeMethod(this);
    }

    // alternative approach (with writeInnerTryCatchSynchronized)
    @SuppressWarnings("unused")
    void writeOuterUnsynchronized()
    {
        access &= ~(Opcodes.ACC_SYNCHRONIZED | Opcodes.ACC_SYNTHETIC);
        name = baseName;

        Label start = new Label();
        Label end = new Label();

        reset(start, end);
        maxStack = maxLocalParams;

        instructions.add(getLabelNode(start));
        invokePreMonitorEnter();
        pop();

        int invokeCode = loadParamsAndReturnInvokeCode();
        instructions.add(new MethodInsnNode(invokeCode, className, baseName + "$catch", desc));
        instructions.add(new InsnNode(returnCode()));
        instructions.add(getLabelNode(end));
        methodWriterSink.writeMethod(this);
    }

    // alternative approach (with writeOuterUnsynchronized)
    @SuppressWarnings("unused")
    void writeInnerTryCatchSynchronized()
    {
        access |= Opcodes.ACC_SYNCHRONIZED | Opcodes.ACC_SYNTHETIC;
        name = baseName + "$catch";

        Label start = new Label();
        Label normal = new Label();
        Label except = new Label();
        Label end = new Label();
        reset(start, end);
        maxStack = Math.max(maxLocalParams, returnCode == Opcodes.RETURN ? 1 : 2); // must load self or class onto stack, and return value (if any)
        ++maxLocals;
        tryCatchBlocks.add(new TryCatchBlockNode(getLabelNode(start), getLabelNode(normal), getLabelNode(except), null));
        instructions.add(getLabelNode(start));
        int invokeCode = loadParamsAndReturnInvokeCode();
        instructions.add(new MethodInsnNode(invokeCode, className, baseName + "$unsync", desc));
        instructions.add(getLabelNode(normal));
        invokePreMonitorExit();
        instructions.add(new InsnNode(returnCode()));
        instructions.add(getLabelNode(except));
        instructions.add(new FrameNode(Opcodes.F_SAME1, 0, null, 1, new Object[]{ "java/lang/Throwable" }));
        instructions.add(new IntInsnNode(Opcodes.ASTORE, maxLocalParams));
        invokePreMonitorExit();
        instructions.add(new IntInsnNode(Opcodes.ALOAD, maxLocalParams));
        instructions.add(new InsnNode(Opcodes.ATHROW));
        instructions.add(getLabelNode(end));
        methodWriterSink.writeSyntheticMethod(MONITOR, this);
    }

    void writeTryCatchMonitorEnterExit()
    {
        access |= Opcodes.ACC_SYNTHETIC;
        name = baseName;

        Label start = new Label();
        Label inmonitor = new Label();
        Label normal = new Label();
        Label except = new Label(); // normal
        Label normalRetExcept = new Label(); // normal return failed
        Label exceptRetNormal = new Label(); // exceptional return success
        Label exceptRetExcept = new Label(); // exceptional return failed
        Label end = new Label();
        reset(start, end);
        ++maxLocals; // add a local variable slot to save any exceptions into (at maxLocalParams position)
        maxStack = Math.max(maxLocalParams, returnCode == Opcodes.RETURN ? 2 : 3); // must load self or class onto stack, and return value (if any)
        tryCatchBlocks.add(new TryCatchBlockNode(getLabelNode(inmonitor), getLabelNode(normal), getLabelNode(except), null));
        tryCatchBlocks.add(new TryCatchBlockNode(getLabelNode(normal), getLabelNode(normalRetExcept), getLabelNode(normalRetExcept), null));
        tryCatchBlocks.add(new TryCatchBlockNode(getLabelNode(except), getLabelNode(exceptRetNormal), getLabelNode(exceptRetExcept), null));
        // preMonitorEnter
        // monitorenter
        instructions.add(getLabelNode(start));
        invokePreMonitorEnter();
        invokeMonitor(Opcodes.MONITORENTER);
        {
            // try1 { val = original();
            instructions.add(getLabelNode(inmonitor));
            int invokeCode = loadParamsAndReturnInvokeCode();
            instructions.add(new MethodInsnNode(invokeCode, className, baseName + "$unsync", desc));
            {
                // try2 { preMonitorExit(); monitorexit; return val; }
                instructions.add(getLabelNode(normal));
                invokePreMonitorExit();
                invokeMonitor(Opcodes.MONITOREXIT);
                instructions.add(new InsnNode(returnCode())); // success
                // }
                // catch2 { monitorexit; throw }
                instructions.add(getLabelNode(normalRetExcept));
                instructions.add(new FrameNode(Opcodes.F_SAME1, 0, null, 1, new Object[]{ "java/lang/Throwable" }));
                instructions.add(new IntInsnNode(Opcodes.ASTORE, maxLocalParams));
                pushRef();
                invokeMonitor(Opcodes.MONITOREXIT);
                instructions.add(new IntInsnNode(Opcodes.ALOAD, maxLocalParams));
                instructions.add(new InsnNode(Opcodes.ATHROW));
                // }
            }
            // catch1 { try3 { preMonitorExit; monitorexit; throw
            instructions.add(getLabelNode(except));
            instructions.add(new FrameNode(Opcodes.F_SAME1, 0, null, 1, new Object[]{ "java/lang/Throwable" }));
            instructions.add(new IntInsnNode(Opcodes.ASTORE, maxLocalParams));
            invokePreMonitorExit();
            invokeMonitor(Opcodes.MONITOREXIT);
            instructions.add(new IntInsnNode(Opcodes.ALOAD, maxLocalParams));
            instructions.add(getLabelNode(exceptRetNormal));
            instructions.add(new InsnNode(Opcodes.ATHROW));
            instructions.add(getLabelNode(exceptRetExcept));
            instructions.add(new FrameNode(Opcodes.F_SAME1, 0, null, 1, new Object[]{ "java/lang/Throwable" }));
            instructions.add(new IntInsnNode(Opcodes.ASTORE, maxLocalParams));
            pushRef();
            invokeMonitor(Opcodes.MONITOREXIT);
            instructions.add(new IntInsnNode(Opcodes.ALOAD, maxLocalParams));
            instructions.add(new InsnNode(Opcodes.ATHROW));
        }
        instructions.add(getLabelNode(end));
        methodWriterSink.writeSyntheticMethod(MONITOR, this);
    }

    @Override
    public void visitEnd()
    {
        writeOriginal();
        writeTryCatchMonitorEnterExit();
        super.visitEnd();
    }
}
