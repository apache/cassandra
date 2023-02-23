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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.LengthPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.service.StorageService;

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
    public @interface UseRandomPartitioner {}

    @Retention(RetentionPolicy.RUNTIME)
    public @interface UseOrderPreservingPartitioner {}

    @Retention(RetentionPolicy.RUNTIME)
    public @interface UseLengthPartitioner {}

    @Retention(RetentionPolicy.RUNTIME)
    public @interface UseByteOrderedPartitioner {}

    @Retention(RetentionPolicy.RUNTIME)
    public @interface DDDaemonInitialization {}

    @Retention(RetentionPolicy.RUNTIME)
    public @interface SchemaLoaderPrepareServer {}

    @Retention(RetentionPolicy.RUNTIME)
    public @interface SchemaLoaderLoadSchema {}

    private static boolean classResetStorageServicePartitioner;

    @BeforeClass
    public static void cassandraTestBaseBeforeClass()
    {
        if (hasClassAnnotation(DDDaemonInitialization.class))
            DatabaseDescriptor.daemonInitialization();
        else if (hasClassAnnotation(SchemaLoaderPrepareServer.class))
            SchemaLoader.prepareServer();
        else if (hasClassAnnotation(SchemaLoaderLoadSchema.class))
            SchemaLoader.loadSchema();
        else
            DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);

        if (hasClassAnnotation(UseMurmur3Partitioner.class))
        {
            classResetStorageServicePartitioner = true;
            StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        }
        if (hasClassAnnotation(UseRandomPartitioner.class))
        {
            classResetStorageServicePartitioner = true;
            StorageService.instance.setPartitionerUnsafe(RandomPartitioner.instance);
        }
        if (hasClassAnnotation(UseOrderPreservingPartitioner.class))
        {
            classResetStorageServicePartitioner = true;
            StorageService.instance.setPartitionerUnsafe(OrderPreservingPartitioner.instance);
        }
        if (hasClassAnnotation(UseLengthPartitioner.class))
        {
            classResetStorageServicePartitioner = true;
            StorageService.instance.setPartitionerUnsafe(LengthPartitioner.instance);
        }
        if (hasClassAnnotation(UseByteOrderedPartitioner.class))
        {
            classResetStorageServicePartitioner = true;
            StorageService.instance.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        }
    }

    @AfterClass
    public static void cassandraTestBaseAfterClass()
    {
        if (classResetStorageServicePartitioner)
        {
            StorageService.instance.resetPartitionerUnsafe();
            classResetStorageServicePartitioner = false;
        }
    }

    public static boolean hasClassAnnotation(Class<? extends Annotation> annotation)
    {
        return hasClassAnnotation(testClass, annotation);
    }

    public static boolean hasClassAnnotation(Class<?> clazz, Class<? extends Annotation> annotation)
    {
        if (clazz == null)
            return false;
        if (clazz.getAnnotation(annotation) != null)
            return true;
        return hasClassAnnotation(clazz.getSuperclass(), annotation);
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

    private boolean testResetStorageServicePartitioner;

    @Before
    public void cassandraTestBaseSetUp() throws Exception
    {
        testMethod = testClass.getMethod(testMethodName.getMethodName());
        if (hasMethodAnnotation(UseMurmur3Partitioner.class))
        {
            testResetStorageServicePartitioner = true;
            StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        }
        if (hasMethodAnnotation(UseRandomPartitioner.class))
        {
            testResetStorageServicePartitioner = true;
            StorageService.instance.setPartitionerUnsafe(RandomPartitioner.instance);
        }
        if (hasMethodAnnotation(UseOrderPreservingPartitioner.class))
        {
            testResetStorageServicePartitioner = true;
            StorageService.instance.setPartitionerUnsafe(OrderPreservingPartitioner.instance);
        }
        if (hasMethodAnnotation(UseLengthPartitioner.class))
        {
            testResetStorageServicePartitioner = true;
            StorageService.instance.setPartitionerUnsafe(LengthPartitioner.instance);
        }
        if (hasMethodAnnotation(UseByteOrderedPartitioner.class))
        {
            testResetStorageServicePartitioner = true;
            StorageService.instance.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        }
    }

    private boolean hasMethodAnnotation(Class<? extends Annotation> annotation)
    {
        return testMethod.getAnnotation(annotation) != null;
    }

    @After
    public void cassandraTestBaseTearDown() throws Exception
    {
        if (testResetStorageServicePartitioner)
        {
            StorageService.instance.resetPartitionerUnsafe();
            testResetStorageServicePartitioner = true;
        }
    }
}
