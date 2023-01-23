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

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;

/*
 * Many tests declare their own test base and duplicate functionality
 * Hopefully this can serve as a place to put common initialization patterns and annotations
 * So people have fewer problems to solve when authoring tests.
 */
public class CassandraTestBase
{
    @Retention(RetentionPolicy.RUNTIME)
    public @interface UseMurmur3Partitioner {}

    @Retention(RetentionPolicy.RUNTIME)
    public @interface DDDaemonInitialization {}

    @BeforeClass
    public static void cassandraTestBaseBeforeClass()
    {
        if (hasClassAnnotation(DDDaemonInitialization.class))
            DatabaseDescriptor.daemonInitialization();
    }

    public static boolean hasClassAnnotation(Class<? extends Annotation> annotation)
    {
        return testClass.getAnnotation(annotation) != null;
    }

    private static Class<?> testClass;

    @ClassRule
    public static TestWatcher classWatcher = new TestWatcher()
    {
        @Override
        public void starting(Description description)
        {
            testClass = description.getTestClass();
        }
    };

    @Rule
    public TestName testMethodName = new TestName();
    public Method testMethod;
    private IPartitioner originalPartitioner;

    @Before
    public void cassandraTestBaseSetUp() throws Exception
    {
        testMethod = testClass.getMethod(testMethodName.getMethodName());
        if (hasMethodAnnotation(UseMurmur3Partitioner.class))
            originalPartitioner = DatabaseDescriptor.setPartitionerUnsafe(new org.apache.cassandra.dht.Murmur3Partitioner());
    }

    private boolean hasMethodAnnotation(Class<? extends Annotation> annotation)
    {
        return testMethod.getAnnotation(annotation) != null;
    }

    @After
    public void cassandraTestBaseTearDown() throws Exception
    {
        if (originalPartitioner != null)
        {
            DatabaseDescriptor.setPartitionerUnsafe(originalPartitioner);
            originalPartitioner = null;
        }
    }
}
