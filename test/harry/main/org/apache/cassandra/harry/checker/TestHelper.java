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

package org.apache.cassandra.harry.checker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.test.ExecUtil;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

public class TestHelper
{
    private static final Logger logger = LoggerFactory.getLogger(TestHelper.class);

    public static void withRandom(ModelChecker.ThrowingConsumer<EntropySource> rng)
    {
        withRandom(System.nanoTime(), rng);
    }

    public static void withRandom(long seed, ModelChecker.ThrowingConsumer<EntropySource> rng)
    {
        try
        {
            logger.info("Seed: {}", seed);
            rng.accept(new JdkRandomEntropySource(seed));
        }
        catch (Throwable t)
        {
            throw new AssertionError(String.format("Caught an exception at seed:%dL", seed), t);
        }
    }

    public static void repeat(int num, ExecUtil.ThrowingSerializableRunnable<?> r)
    {
        for (int i = 0; i < num; i++)
        {
            try
            {
                r.run();
            }
            catch (Throwable throwable)
            {
                throw new AssertionError(throwable);
            }
        }
    }
}
