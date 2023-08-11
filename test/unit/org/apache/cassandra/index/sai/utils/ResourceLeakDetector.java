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

package org.apache.cassandra.index.sai.utils;

import java.util.List;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.carrotsearch.randomizedtesting.rules.StatementAdapter;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.junit.Assert.assertEquals;

public class ResourceLeakDetector implements TestRule
{
    private static final Injections.Counter RESOURCE_LEAK_COUNTER = Injections.newCounter("ResourceLeakCounter")
                                                                              .add(InvokePointBuilder.newInvokePoint()
                                                                                                     .onClass("org.apache.cassandra.utils.concurrent.Ref$State")
                                                                                                     .onMethod("reportLeak"))
                                                                              .build();

    @Override
    public Statement apply(Statement statement, Description description)
    {
        return new StatementAdapter(statement)
        {
            @Override
            protected void before() throws Throwable
            {
                ResourceLeakDetector.this.before();
            }

            @Override
            protected void afterAlways(List<Throwable> errors)
            {
                ResourceLeakDetector.this.afterAlways();
            }

            @Override
            protected void afterIfSuccessful()
            {
                ResourceLeakDetector.this.afterIfSuccessful();
            }
        };
    }

    protected void before() throws Throwable
    {
        Injections.inject(RESOURCE_LEAK_COUNTER);
    }

    protected void afterIfSuccessful()
    {
        assertEquals("Resource leaks were detected during this test. Add -Dcassandra.debugrefcount=true to analyze the leaks", 0, RESOURCE_LEAK_COUNTER.get());
    }

    protected void afterAlways()
    {
        Injections.deleteAll();
        RESOURCE_LEAK_COUNTER.reset();
    }
}
