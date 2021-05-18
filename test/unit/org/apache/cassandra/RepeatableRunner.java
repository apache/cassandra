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
package org.apache.cassandra;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.internal.builders.AllDefaultPossibilitiesBuilder;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.RunnerBuilder;

/**
 * This class comes useful when debugging flaky tests that will fail only when the full test suite is ran. It is
 * intended for test failure investigation only.
 * <p>
 * Decorate your class with the runner and iterations you want. Defaults to 10.
 * Beware of tests that change singleton status as those won't work.
 * <pre>{@code
 * @RunWith(RepeatableRunner.class)
 * @RepeatableRunnerConfiguration(iterations = 2, runner = BlockJUnit4ClassRunner.class)
 * public class MyTest
 * {
 * ...
 * }
 * }</pre>
 */
public class RepeatableRunner extends Runner
{
    private static final int DEFAULT_REPETITIONS = 10;
    private static final Class<? extends Runner> DEFAULT_RUNNER_CLASS = BlockJUnit4ClassRunner.class;

    private final Runner runner;
    private final int iterations;

    public RepeatableRunner(Class<?> testClass) throws Throwable
    {
        Class<? extends Runner> runnerClass = DEFAULT_RUNNER_CLASS;

        if (testClass.isAnnotationPresent(RepeatableRunnerConfiguration.class))
        {
            RepeatableRunnerConfiguration configuration = testClass.getAnnotation(RepeatableRunnerConfiguration.class);
            iterations = configuration.iterations();
            runnerClass = configuration.runner();
        }
        else
        {
            iterations = DEFAULT_REPETITIONS;
        }

        runner = buildRunner(runnerClass, testClass);
    }

    @Override
    public Description getDescription()
    {
        return runner.getDescription();
    }

    @Override
    public void run(RunNotifier notifier)
    {
        for (int i = 0; i < iterations; i++)
        {
            runner.run(notifier);
        }
    }

    private static Runner buildRunner(Class<? extends Runner> runnerClass, Class<?> testClass) throws Throwable
    {
        try
        {
            return runnerClass.getConstructor(Class.class).newInstance(testClass);
        }
        catch (NoSuchMethodException e)
        {
            return runnerClass.getConstructor(Class.class, RunnerBuilder.class)
                              .newInstance(testClass, new AllDefaultPossibilitiesBuilder(true));
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface RepeatableRunnerConfiguration
    {
        int iterations() default DEFAULT_REPETITIONS;

        Class<? extends Runner> runner() default BlockJUnit4ClassRunner.class;
    }
}
