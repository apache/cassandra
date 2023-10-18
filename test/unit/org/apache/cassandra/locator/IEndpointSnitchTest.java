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

package org.apache.cassandra.locator;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.assertj.core.api.Assertions;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ConfigurationBuilder;

public class IEndpointSnitchTest
{
    // all these must support comparing by endpoing, do not add any here unless that is confirmed!
    private static final Set<Class<? extends IEndpointSnitch>> INIT_REQUIRES_NETWORK = ImmutableSet.of(AlibabaCloudSnitch.class,
                                                                                                       CloudstackSnitch.class,
                                                                                                       Ec2Snitch.class,
                                                                                                       Ec2MultiRegionSnitch.class,
                                                                                                       GoogleCloudSnitch.class,
                                                                                                       AzureSnitch.class);

    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void allSupportEndpoint() throws InvocationTargetException, InstantiationException, IllegalAccessException
    {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                                                  .forPackage("org.apache.cassandra")
                                                  .setScanners(Scanners.SubTypes)
                                                  .setExpandSuperTypes(true));

        for (Class<? extends IEndpointSnitch> klass : reflections.getSubTypesOf(IEndpointSnitch.class))
        {
            if (Modifier.isAbstract(klass.getModifiers())
                || Modifier.isPrivate(klass.getModifiers()) // private can not be created normally, so these are scoped to tests and can be ignored
                || klass.isAnonymousClass()
                || INIT_REQUIRES_NETWORK.contains(klass))
                continue;
            Constructor<? extends IEndpointSnitch> declaredConstructor;
            try
            {
                declaredConstructor = klass.getDeclaredConstructor();
            }
            catch (NoSuchMethodException e)
            {
                // DynamicEndpointSnitch or test snitch... we can not create this normally
                continue;
            }
            if (Modifier.isPrivate(declaredConstructor.getModifiers()))
                continue;
            IEndpointSnitch snitch = declaredConstructor.newInstance();
            Assertions.assertThat(snitch.supportCompareByEndpoint())
                      .describedAs("Snitch %s does not support compare by endpoint!", snitch.getClass())
                      .isTrue();
        }
    }
}