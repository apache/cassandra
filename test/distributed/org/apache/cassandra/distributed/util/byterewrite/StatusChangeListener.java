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

package org.apache.cassandra.distributed.util.byterewrite;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.impl.InstanceIDDefiner;
import org.apache.cassandra.distributed.util.TwoWay;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;

public class StatusChangeListener
{
    public enum Status
    {
        LEAVING("startLeaving"),
        LEAVE("leaveRing");

        private final String method;

        Status(String method)
        {
            this.method = method;
        }
    }

    public static void install(ClassLoader cl, int node, Status first, Status... rest)
    {
        install(cl, node, EnumSet.of(first, rest));
    }

    public static void install(ClassLoader cl, int node, Set<Status> statuses)
    {
        if (statuses.isEmpty()) throw new IllegalStateException("Need a set of status to listen to");

        State.hooks.put(node, new Hooks());
        DynamicType.Builder<StorageService> builder = new ByteBuddy().rebase(StorageService.class);
        for (Status s : statuses)
            builder = builder.method(named(s.method)).intercept(MethodDelegation.to(BB.class));
        builder.make().load(cl, ClassLoadingStrategy.Default.INJECTION);
    }

    public static void close()
    {
        for (Hooks hook : State.hooks.values())
            hook.close();
    }

    public static Hooks hooks(int node)
    {
        return Objects.requireNonNull(State.hooks.get(node), "Unknown node" + node);
    }

    @Shared
    public static class State
    {
        public static final Map<Integer, Hooks> hooks = new ConcurrentHashMap<>();
    }

    @Shared
    public static class Hooks implements AutoCloseable
    {
        public static final TwoWay leaving = new TwoWay();
        public static final TwoWay leave = new TwoWay();

        @Override
        public void close()
        {
            for (TwoWay condition: Arrays.asList(leaving, leave))
                condition.close();
        }
    }

    public static class BB
    {
        private static volatile int NODE = -1;

        public static void startLeaving(@SuperCall Runnable zuper)
        {
            // see org.apache.cassandra.service.StorageService.startLeaving
            hooks().leaving.enter();
            zuper.run();
        }

        public static void leaveRing(@SuperCall Runnable zuper)
        {
            // see org.apache.cassandra.service.StorageService.leaveRing
            hooks().leave.enter();
            zuper.run();
        }

        private static Hooks hooks()
        {
            return State.hooks.get(node());
        }

        private static int node()
        {
            int node = NODE;
            if (node == -1)
                node = NODE = Integer.parseInt(InstanceIDDefiner.getInstanceId().replace("node", ""));
            return node;
        }
    }
}
