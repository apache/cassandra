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
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import org.objectweb.asm.Opcodes;

// TODO (completeness): confirm that those classes we weave monitor-access for only extend other classes we also weave monitor access for
// TODO (completeness): confirm that those classes we weave monitor access for only take monitors on types we also weave monitor access for (and vice versa)
public class InterceptClasses implements BiFunction<String, byte[], byte[]>
{
    public static final int BYTECODE_VERSION = Opcodes.ASM7;

    // TODO (cleanup): use annotations
    private static final Pattern MONITORS = Pattern.compile( "org[/.]apache[/.]cassandra[/.]utils[/.]concurrent[/.].*" +
                                                            "|org[/.]apache[/.]cassandra[/.]concurrent[/.].*" +
                                                            "|org[/.]apache[/.]cassandra[/.]simulator[/.]test.*" +
                                                            "|org[/.]apache[/.]cassandra[/.]db[/.]ColumnFamilyStore.*" +
                                                            "|org[/.]apache[/.]cassandra[/.]db[/.]Keyspace.*" +
                                                            "|org[/.]apache[/.]cassandra[/.]db[/.]SystemKeyspace.*" +
                                                            "|org[/.]apache[/.]cassandra[/.]streaming[/.].*" +
                                                            "|org[/.]apache[/.]cassandra[/.]db.streaming[/.].*" +
                                                            "|org[/.]apache[/.]cassandra[/.]distributed[/.]impl[/.]DirectStreamingConnectionFactory.*" +
                                                            "|org[/.]apache[/.]cassandra[/.]db[/.]commitlog[/.].*" +
                                                            "|org[/.]apache[/.]cassandra[/.]service[/.]paxos[/.].*");

    private static final Pattern GLOBAL_METHODS = Pattern.compile("org[/.]apache[/.]cassandra[/.](?!simulator[/.]).*" +
                                                                  "|org[/.]apache[/.]cassandra[/.]simulator[/.]test[/.].*" +
                                                                  "|org[/.]apache[/.]cassandra[/.]simulator[/.]cluster[/.].*" +
                                                                  "|io[/.]netty[/.]util[/.]concurrent[/.]FastThreadLocal"); // intercept IdentityHashMap for execution consistency
    private static final Pattern NEMESIS = GLOBAL_METHODS;
    private static final Set<String> WARNED = Collections.newSetFromMap(new ConcurrentHashMap<>());

    static final Cached SENTINEL = new Cached(null);
    static class Cached
    {
        final byte[] cached;
        private Cached(byte[] cached)
        {
            this.cached = cached;
        }
    }

    private final Map<String, Cached> cache = new ConcurrentHashMap<>();

    private final int api;
    private final ChanceSupplier nemesisChance;
    private final ChanceSupplier monitorDelayChance;
    private final Hashcode insertHashcode;
    private final NemesisFieldKind.Selector nemesisFieldSelector;

    public InterceptClasses(ChanceSupplier monitorDelayChance, ChanceSupplier nemesisChance, NemesisFieldKind.Selector nemesisFieldSelector)
    {
        this(BYTECODE_VERSION, monitorDelayChance, nemesisChance, nemesisFieldSelector);
    }

    public InterceptClasses(int api, ChanceSupplier monitorDelayChance, ChanceSupplier nemesisChance, NemesisFieldKind.Selector nemesisFieldSelector)
    {
        this.api = api;
        this.nemesisChance = nemesisChance;
        this.monitorDelayChance = monitorDelayChance;
        this.insertHashcode = new Hashcode(api);
        this.nemesisFieldSelector = nemesisFieldSelector;
    }

    @Override
    public synchronized byte[] apply(String name, byte[] bytes)
    {
        if (bytes == null)
            return maybeSynthetic(name);

        Hashcode hashcode = insertHashCode(name);

        name = dotsToSlashes(name);
        EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
        if (MONITORS.matcher(name).matches())
        {
            flags.add(Flag.MONITORS);
        }
        if (GLOBAL_METHODS.matcher(name).matches())
        {
            flags.add(Flag.GLOBAL_METHODS);
            flags.add(Flag.LOCK_SUPPORT);
        }
        if (NEMESIS.matcher(name).matches())
        {
            flags.add(Flag.NEMESIS);
        }

        if (flags.isEmpty() && hashcode == null)
            return bytes;

        Cached prev = cache.get(name);
        if (prev != null)
        {
            if (prev == SENTINEL)
                return bytes;
            return prev.cached;
        }

        ClassTransformer transformer = new ClassTransformer(api, name, flags, monitorDelayChance, new NemesisGenerator(api, name, nemesisChance), nemesisFieldSelector, hashcode);
        transformer.readAndTransform(bytes);

        if (!transformer.isTransformed())
        {
            cache.put(name, SENTINEL);
            return bytes;
        }

        bytes = transformer.toBytes();
        if (transformer.isCacheablyTransformed())
            cache.put(name, new Cached(bytes));

        return bytes;
    }

    static String dotsToSlashes(String className)
    {
        return className.replace('.', '/');
    }

    static String dotsToSlashes(Class<?> clazz)
    {
        return dotsToSlashes(clazz.getName());
    }

    /**
     * Decide if we should insert our own hashCode() implementation that assigns deterministic hashes, i.e.
     *   - If it's one of our classes
     *   - If its parent is not one of our classes (else we'll assign it one anyway)
     *   - If it does not have its own hashCode() implementation that overrides Object's
     *   - If it is not Serializable OR it has a serialVersionUID
     *
     * Otherwise we either probably do not need it, or may break serialization between classloaders
     */
    private Hashcode insertHashCode(String name)
    {
        try
        {
            if (!name.startsWith("org.apache.cassandra"))
                return null;

            Class<?> sharedClass = getClass().getClassLoader().loadClass(name);
            if (sharedClass.isInterface() || sharedClass.isEnum() || sharedClass.isArray() || sharedClass.isSynthetic())
                return null;

            Class<?> parent = sharedClass.getSuperclass();
            if (parent.getName().startsWith("org.apache.cassandra"))
                return null;

            try
            {
                Method method = sharedClass.getMethod("hashCode");
                if (method.getDeclaringClass() != Object.class)
                    return null;
            }
            catch (NoSuchMethodException ignore)
            {
            }

            if (!Serializable.class.isAssignableFrom(sharedClass))
                return insertHashcode;

            try
            {
                // if we haven't specified serialVersionUID we break ObjectInputStream transfers between class loaders
                // (might be easiest to switch to serialization that doesn't require it)
                sharedClass.getDeclaredField("serialVersionUID");
                return insertHashcode;
            }
            catch (NoSuchFieldException e)
            {
                if (!Throwable.class.isAssignableFrom(sharedClass) && WARNED.add(name))
                    System.err.println("No serialVersionUID on Serializable " + sharedClass);
                return null;
            }
        }
        catch (ClassNotFoundException e)
        {
            System.err.println("Unable to determine if should insert hashCode() for " + name);
            e.printStackTrace();
        }
        return null;
    }

    static final String shadowRootExternalType = "org.apache.cassandra.simulator.systems.InterceptibleConcurrentHashMap";
    static final String shadowRootType = "org/apache/cassandra/simulator/systems/InterceptibleConcurrentHashMap";
    static final String originalRootType = Utils.toInternalName(ConcurrentHashMap.class);
    static final String shadowOuterTypePrefix = shadowRootType + '$';
    static final String originalOuterTypePrefix = originalRootType + '$';

    protected byte[] maybeSynthetic(String name)
    {
        if (!name.startsWith(shadowRootExternalType))
            return null;

        try
        {
            String originalType, shadowType = Utils.toInternalName(name);
            if (!shadowType.startsWith(shadowOuterTypePrefix))
                originalType = originalRootType;
            else
                originalType = originalOuterTypePrefix + name.substring(shadowOuterTypePrefix.length());

            EnumSet<Flag> flags = EnumSet.of(Flag.GLOBAL_METHODS, Flag.MONITORS, Flag.LOCK_SUPPORT);
            if (NEMESIS.matcher(name).matches()) flags.add(Flag.NEMESIS);
            NemesisGenerator nemesis = new NemesisGenerator(api, name, nemesisChance);

            ShadowingTransformer transformer;
            transformer = new ShadowingTransformer(InterceptClasses.BYTECODE_VERSION,
                                                   originalType, shadowType, originalRootType, shadowRootType,
                                                   originalOuterTypePrefix, shadowOuterTypePrefix,
                                                   flags, monitorDelayChance, nemesis, nemesisFieldSelector, null);
            transformer.readAndTransform(Utils.readDefinition(originalType + ".class"));
            return transformer.toBytes();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }

    }

}