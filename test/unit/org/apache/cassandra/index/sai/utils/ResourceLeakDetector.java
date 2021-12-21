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

import java.lang.annotation.Annotation;
import java.util.List;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.carrotsearch.randomizedtesting.rules.StatementAdapter;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;
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
        if (isResourceLeakCheckEnabled(description))
        {
            return new StatementAdapter(statement)
            {
                @Override
                protected void before() throws Throwable
                {
                    ResourceLeakDetector.this.before();
                }

                @Override
                protected void afterAlways(List<Throwable> errors) throws Throwable
                {
                    ResourceLeakDetector.this.afterAlways(errors);
                }

                @Override
                protected void afterIfSuccessful() throws Throwable
                {
                    ResourceLeakDetector.this.afterIfSuccessful();
                }
            };
        }
        return statement;
    }

    protected void before() throws Throwable
    {
        Injections.inject(RESOURCE_LEAK_COUNTER);
    }

    protected void afterIfSuccessful() throws Throwable
    {
        assertEquals("Resource leaks were detected during this test. Add -Dcassandra.debugrefcount=true to analyze the leaks", 0, RESOURCE_LEAK_COUNTER.get());
    }

    protected void afterAlways(List<Throwable> errors) throws Throwable
    {
        Injections.deleteAll();
        RESOURCE_LEAK_COUNTER.reset();
    }

    private boolean isResourceLeakCheckEnabled(Description description)
    {
        return !hasAnnotation(description, SuppressLeakCheck.class);
    }

    private boolean hasAnnotation(Description description, Class<? extends Annotation> annotation)
    {
        return ((description.getAnnotation(annotation) != null) ||
                (description.getTestClass().getAnnotation(annotation) != null));
    }
}
