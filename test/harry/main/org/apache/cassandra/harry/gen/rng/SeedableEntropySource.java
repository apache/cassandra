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

package org.apache.cassandra.harry.gen.rng;

import java.util.function.Consumer;
import java.util.function.Function;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.harry.gen.EntropySource;

public class SeedableEntropySource
{
    private static final FastThreadLocal<EntropySource> THREAD_LOCAL = new FastThreadLocal<>();

    public static <T> T computeWithSeed(long seed, Function<EntropySource, T> fn)
    {
        return fn.apply(entropySource(seed));
    }

    public static void doWithSeed(long seed, Consumer<EntropySource> fn)
    {
        fn.accept(entropySource(seed));
    }

    private static EntropySource entropySource(long seed)
    {
        EntropySource entropySource = THREAD_LOCAL.get();
        if (entropySource == null)
        {
            entropySource = new JdkRandomEntropySource(0);
            THREAD_LOCAL.set(entropySource);
        }
        entropySource.seed(seed);
        return entropySource;
    }
}