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

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;

public class Undead
{
    public static void install(ClassLoader cl)
    {
        new ByteBuddy().rebase(EndpointState.class)
                       .method(named("markDead")).intercept(MethodDelegation.to(BB.class))
                       .make()
                       .load(cl, ClassLoadingStrategy.Default.INJECTION);
    }

    public static void close()
    {
        State.enabled = false;
    }

    @Shared
    public static class State
    {
        public static volatile boolean enabled = true;
    }

    public static class BB
    {
        public static void markDead(@SuperCall Runnable real)
        {
            if (State.enabled)
            {
                // don't let anything get marked dead...
                return;
            }
            real.run();
        }
    }
}
