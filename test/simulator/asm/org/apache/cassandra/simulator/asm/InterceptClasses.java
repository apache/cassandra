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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.objectweb.asm.Opcodes;

import static org.apache.cassandra.simulator.asm.InterceptClasses.Cached.Kind.MODIFIED;
import static org.apache.cassandra.simulator.asm.InterceptClasses.Cached.Kind.UNMODIFIED;
import static org.apache.cassandra.simulator.asm.InterceptClasses.Cached.Kind.UNSHAREABLE;

// TODO (completeness): confirm that those classes we weave monitor-access for only extend other classes we also weave monitor access for
// TODO (completeness): confirm that those classes we weave monitor access for only take monitors on types we also weave monitor access for (and vice versa)
// WARNING: does not implement IClassTransformer directly as must be accessible to bootstrap class loader
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

    static final byte[] SENTINEL = new byte[0];
    static class Cached
    {
        enum Kind { MODIFIED, UNMODIFIED, UNSHAREABLE }
        final Kind kind;
        final byte[] bytes;
        final Set<String> uncacheablePeers;
        private Cached(Kind kind, byte[] bytes, Set<String> uncacheablePeers)
        {
            this.kind = kind;
            this.bytes = bytes;
            this.uncacheablePeers = uncacheablePeers;
        }
    }

    static class PeerGroup
    {
        final Set<String> uncacheablePeers = new TreeSet<>();
        final Cached unmodified = new Cached(UNMODIFIED, null, uncacheablePeers);
    }

    class SubTransformer implements BiFunction<String, byte[], byte[]>
    {
        private final Map<String, byte[]> isolatedCache = new ConcurrentHashMap<>();

        @Override
        public byte[] apply(String name, byte[] bytes)
        {
            return transformTransitiveClosure(name, bytes, isolatedCache);
        }
    }

    private final Map<String, Cached> cache = new ConcurrentHashMap<>();

    private final int api;
    private final ChanceSupplier nemesisChance;
    private final ChanceSupplier monitorDelayChance;
    private final Hashcode insertHashcode;
    private final NemesisFieldKind.Selector nemesisFieldSelector;
    private final ClassLoader prewarmClassLoader;
    private final Predicate<String> prewarm;
    private final byte[] bufIn = new byte[4096];
    private final ByteArrayOutputStream bufOut = new ByteArrayOutputStream();

    public InterceptClasses(ChanceSupplier monitorDelayChance, ChanceSupplier nemesisChance, NemesisFieldKind.Selector nemesisFieldSelector, ClassLoader prewarmClassLoader, Predicate<String> prewarm)
    {
        this(BYTECODE_VERSION, monitorDelayChance, nemesisChance, nemesisFieldSelector, prewarmClassLoader, prewarm);
    }

    public InterceptClasses(int api, ChanceSupplier monitorDelayChance, ChanceSupplier nemesisChance, NemesisFieldKind.Selector nemesisFieldSelector, ClassLoader prewarmClassLoader, Predicate<String> prewarm)
    {
        this.api = api;
        this.nemesisChance = nemesisChance;
        this.monitorDelayChance = monitorDelayChance;
        this.insertHashcode = new Hashcode(api);
        this.nemesisFieldSelector = nemesisFieldSelector;
        this.prewarmClassLoader = prewarmClassLoader;
        this.prewarm = prewarm;
    }

    @Override
    public byte[] apply(String name, byte[] bytes)
    {
        return transformTransitiveClosure(name, bytes, null);
    }

    private synchronized byte[] transformTransitiveClosure(String externalName, byte[] input, Map<String, byte[]> isolatedCache)
    {
        if (input == null)
            return maybeSynthetic(externalName);

        String internalName = dotsToSlashes(externalName);
        if (isolatedCache != null)
        {
            byte[] isolatedCached = isolatedCache.get(internalName);
            if (isolatedCached != null)
                return isolatedCached == SENTINEL ? input : isolatedCached;
        }

        Cached cached = cache.get(internalName);
        if (cached != null)
        {
            if (isolatedCache == null)
            {
                switch (cached.kind)
                {
                    default: throw new AssertionError();
                    case MODIFIED:
                        return cached.bytes;
                    case UNMODIFIED:
                        return input;
                    case UNSHAREABLE:
                        return transform(internalName, externalName, null, input, null, null);
                }
            }

            for (String peer : cached.uncacheablePeers)
                transform(peer, slashesToDots(peer), null, cache.get(peer).bytes, isolatedCache, null);

            switch (cached.kind)
            {
                default: throw new AssertionError();
                case MODIFIED:
                    return cached.bytes;
                case UNMODIFIED:
                    return input;
                case UNSHAREABLE:
                    return isolatedCache.get(internalName);
            }
        }

        Set<String> visited = new HashSet<>();
        visited.add(internalName);
        NavigableSet<String> load = new TreeSet<>();
        Consumer<String> dependentTypeConsumer = type -> {
            if (prewarm.test(type) && visited.add(type))
                load.add(type);
        };

        final PeerGroup peerGroup = new PeerGroup();
        byte[] result = transform(internalName, externalName, peerGroup, input, isolatedCache, dependentTypeConsumer);
        for (String next = load.pollFirst(); next != null; next = load.pollFirst())
        {
            // TODO (now): otherwise merge peer groups
            Cached existing = cache.get(next);
            if (existing == null)
                transform(next, slashesToDots(next), peerGroup, read(next), isolatedCache, dependentTypeConsumer);
        }

        return result;
    }

    private byte[] read(String name)
    {
        try (InputStream in = prewarmClassLoader.getResourceAsStream(dotsToSlashes(name) + ".class"))
        {
            if (in == null)
                throw new NoClassDefFoundError(dotsToSlashes(name) + ".class");

            bufOut.reset();
            for (int c = in.read(bufIn) ; c >= 0 ; c = in.read(bufIn))
                bufOut.write(bufIn, 0, c);
            return bufOut.toByteArray();
        }
        catch (IOException e)
        {
            throw new NoClassDefFoundError(name);
        }
    }

    private byte[] transform(String internalName, String externalName, PeerGroup peerGroup, byte[] input, Map<String, byte[]> isolatedCache, Consumer<String> dependentTypes)
    {
        Hashcode hashcode = insertHashCode(externalName);

        EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
        if (MONITORS.matcher(internalName).matches())
        {
            flags.add(Flag.MONITORS);
        }
        if (GLOBAL_METHODS.matcher(internalName).matches())
        {
            flags.add(Flag.GLOBAL_METHODS);
            flags.add(Flag.LOCK_SUPPORT);
        }
        if (NEMESIS.matcher(internalName).matches())
        {
            flags.add(Flag.NEMESIS);
        }

        if (flags.isEmpty() && hashcode == null)
        {
            cache.put(internalName, peerGroup.unmodified);
            return input;
        }

        ClassTransformer transformer = new ClassTransformer(api, internalName, flags, monitorDelayChance, new NemesisGenerator(api, internalName, nemesisChance), nemesisFieldSelector, hashcode, dependentTypes);
        transformer.setUpdateVisibility(true);
        transformer.readAndTransform(input);

        if (!transformer.isTransformed())
        {
            cache.put(internalName, peerGroup.unmodified);
            return input;
        }

        byte[] output = transformer.toBytes();
        if (transformer.isCacheablyTransformed())
        {
            cache.put(internalName, new Cached(MODIFIED, output, peerGroup.uncacheablePeers));
        }
        else
        {
            if (peerGroup != null)
            {
                cache.put(internalName, new Cached(UNSHAREABLE, input, peerGroup.uncacheablePeers));
                peerGroup.uncacheablePeers.add(internalName);
            }
            if (isolatedCache != null)
                isolatedCache.put(internalName, output);
        }

        return output;
    }

    static String dotsToSlashes(String className)
    {
        return className.replace('.', '/');
    }

    static String dotsToSlashes(Class<?> clazz)
    {
        return dotsToSlashes(clazz.getName());
    }

    static String slashesToDots(String className)
    {
        return className.replace('/', '.');
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
    private Hashcode insertHashCode(String externalName)
    {
        try
        {
            if (!externalName.startsWith("org.apache.cassandra"))
                return null;

            Class<?> sharedClass = getClass().getClassLoader().loadClass(externalName);
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
                if (!Throwable.class.isAssignableFrom(sharedClass) && WARNED.add(externalName))
                    System.err.println("No serialVersionUID on Serializable " + sharedClass);
                return null;
            }
        }
        catch (ClassNotFoundException e)
        {
            System.err.println("Unable to determine if should insert hashCode() for " + externalName);
            e.printStackTrace();
        }
        return null;
    }

    static final String shadowRootExternalType = "org.apache.cassandra.simulator.systems.InterceptibleConcurrentHashMap";
    static final String shadowRootType = "org/apache/cassandra/simulator/systems/InterceptibleConcurrentHashMap";
    static final String originalRootType = Utils.toInternalName(ConcurrentHashMap.class);
    static final String shadowOuterTypePrefix = shadowRootType + '$';
    static final String originalOuterTypePrefix = originalRootType + '$';

    protected byte[] maybeSynthetic(String externalName)
    {
        if (!externalName.startsWith(shadowRootExternalType))
            return null;

        try
        {
            String originalType, shadowType = Utils.toInternalName(externalName);
            if (!shadowType.startsWith(shadowOuterTypePrefix))
                originalType = originalRootType;
            else
                originalType = originalOuterTypePrefix + externalName.substring(shadowOuterTypePrefix.length());

            EnumSet<Flag> flags = EnumSet.of(Flag.GLOBAL_METHODS, Flag.MONITORS, Flag.LOCK_SUPPORT);
            if (NEMESIS.matcher(externalName).matches()) flags.add(Flag.NEMESIS);
            NemesisGenerator nemesis = new NemesisGenerator(api, externalName, nemesisChance);

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