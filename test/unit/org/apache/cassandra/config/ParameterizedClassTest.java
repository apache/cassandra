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

package org.apache.cassandra.config;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ClassLoadingTestNonAssignable;
import org.apache.cassandra.utils.ClassLoadingTestSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ParameterizedClassTest
{
    @Test
    public void testParameterizedClassEmptyConstructorHasNullParameters()
    {
        ParameterizedClass parameterizedClass = new ParameterizedClass();
        assertNull(parameterizedClass.parameters);
    }

    @Test
    public void testParameterizedClassConstructorWithClassNameHasNonNullParameters()
    {
        ParameterizedClass parameterizedClass = new ParameterizedClass("TestClass");
        assertNotNull(parameterizedClass.parameters);
    }

    @Test
    public void testParameterizedClassConstructorWithClassNameAndParametersHasNullParamters()
    {
        ParameterizedClass parameterizedClass = new ParameterizedClass("TestClass", null);
        assertNull(parameterizedClass.parameters);
    }

    @Test
    public void testNewInstanceWithNonExistentClassFailsWithConfigurationException()
    {
        assertThatThrownBy(() -> ParameterizedClass.newInstance(new ParameterizedClass("NonExistentClass"),
                                                                List.of("org.apache.cassandra.config")))
        .hasMessage("Unable to find class NonExistentClass in packages [\"org.apache.cassandra.config\"]")
        .isInstanceOf(ConfigurationException.class);
    }

    @Test
    public void testNewInstanceWithSingleEmptyConstructorUsesEmptyConstructor()
    {
        ParameterizedClass parameterizedClass = new ParameterizedClass(AllowAllAuthorizer.class.getName());
        IAuthorizer instance = ParameterizedClass.newInstance(parameterizedClass, null);
        assertNotNull(instance);
    }

    @Test
    public void testNewInstanceWithValidConstructorsFavorsMapConstructor()
    {
        ParameterizedClass parameterizedClass = new ParameterizedClass(ParameterizedClassExample.class.getName());
        ParameterizedClassExample instance = ParameterizedClass.newInstance(parameterizedClass, null);
        assertNotNull(instance);
    }

    @Test
    public void testNewInstanceWithValidConstructorsUsingNullParamtersFavorsMapConstructor()
    {
        ParameterizedClass parameterizedClass = new ParameterizedClass(ParameterizedClassExample.class.getName());
        parameterizedClass.parameters = null;

        ParameterizedClassExample instance = ParameterizedClass.newInstance(parameterizedClass, null);
        assertNotNull(instance);
    }

    @Test
    public void testNewInstanceWithConstructorExceptionPreservesOriginalFailure()
    {
        assertThatThrownBy(() -> ParameterizedClass.newInstance(new ParameterizedClass(ParameterizedClassExample.class.getName(),
                                                                                       Map.of("fail", "true")), null))
        .hasMessageStartingWith("Failed to instantiate class")
        .hasMessageContaining("Simulated failure")
        .isInstanceOf(ConfigurationException.class);
    }

    @Test
    public void testTypedNewInstanceRejectsWithoutInitializing()
    {
        ClassLoadingTestSupport.assertNotInitialized(ClassLoadingTestNonAssignable.class);
        ParameterizedClass parameterizedClass = new ParameterizedClass(ClassLoadingTestNonAssignable.class.getName());

        assertThatThrownBy(() -> ParameterizedClass.newInstance(parameterizedClass, null, Runnable.class))
        .hasMessageContaining("must extend or implement " + Runnable.class.getName())
        .isInstanceOf(ConfigurationException.class);
        assertThat(ClassLoadingTestSupport.wasInitialized(ClassLoadingTestNonAssignable.class)).isFalse();
    }

    @Test
    public void testTwoArgNewInstanceDoesNotInitializeAtLoadTime()
    {
        // The 2-arg overload performs no type check, but it must still load without running <clinit>.
        // ClassLoadingTestNonAssignable has no usable constructor, so instantiation fails -- but only after the
        // class has been loaded; loading must not have run its static initializer.
        ClassLoadingTestSupport.assertNotInitialized(ClassLoadingTestNonAssignable.class);
        ParameterizedClass parameterizedClass = new ParameterizedClass(ClassLoadingTestNonAssignable.class.getName());

        assertThatThrownBy(() -> ParameterizedClass.newInstance(parameterizedClass, null))
        .isInstanceOf(ConfigurationException.class);
        assertThat(ClassLoadingTestSupport.wasInitialized(ClassLoadingTestNonAssignable.class)).isFalse();
    }

    @Test
    public void testNewInstanceSearchPackageFallThroughDoesNotAbortOnWrongType()
    {
        // org.apache.cassandra.utils.ClassLoadingSearchProbe (wrong type, not a Runnable) resolves under the earlier
        // search package, while org.apache.cassandra.config.ClassLoadingSearchProbe (correct type, a Runnable)
        // resolves under the later one. A wrong-type match under the earlier package must not abort the search.
        ClassLoadingTestSupport.assertNotInitialized(org.apache.cassandra.utils.ClassLoadingSearchProbe.class);
        ParameterizedClass parameterizedClass = new ParameterizedClass("ClassLoadingSearchProbe");

        Runnable instance = ParameterizedClass.newInstance(parameterizedClass,
                                                           List.of("org.apache.cassandra.utils",
                                                                   "org.apache.cassandra.config"),
                                                           Runnable.class);
        assertNotNull(instance);
        assertThat(instance).isInstanceOf(org.apache.cassandra.config.ClassLoadingSearchProbe.class);
        // The wrong-type class that was probed under the earlier package must not have been initialized.
        assertThat(ClassLoadingTestSupport.wasInitialized(org.apache.cassandra.utils.ClassLoadingSearchProbe.class)).isFalse();
    }
}
