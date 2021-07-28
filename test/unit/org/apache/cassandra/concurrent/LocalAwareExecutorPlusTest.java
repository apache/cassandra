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

package org.apache.cassandra.concurrent;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class LocalAwareExecutorPlusTest extends AbstractExecutorPlusTest
{
    final ExecutorLocals locals = new ExecutorLocals(null, null);

    @Test
    public void testPooled() throws Throwable
    {
        locals.get();
        testPooled(() -> executorFactory().localAware().configurePooled("test", 1));
    }

    @Test
    public void testSequential() throws Throwable
    {
        locals.get();
        testSequential(() -> executorFactory().localAware().configureSequential("test"));
    }

    @Override
    Runnable wrapSubmit(Runnable submit)
    {
        return () -> {
            Assert.assertEquals(locals, ExecutorLocals.current());
            submit.run();
        };
    }
}
