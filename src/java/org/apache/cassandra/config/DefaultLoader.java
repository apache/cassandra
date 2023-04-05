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

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.FieldProperty;
import org.yaml.snakeyaml.introspector.MethodProperty;
import org.yaml.snakeyaml.introspector.Property;

import static org.apache.cassandra.utils.FBUtilities.camelToSnake;

public class DefaultLoader implements Loader
{
    @Override
    public Map<String, Property> getProperties(Class<?> root)
    {
        Map<String, Property> properties = new HashMap<>();
        for (Class<?> c = root; c != null; c = c.getSuperclass())
        {
            for (Field f : c.getDeclaredFields())
            {
                String name = camelToSnake(f.getName());
                int modifiers = f.getModifiers();
                if (!Modifier.isStatic(modifiers)
                    && !f.isAnnotationPresent(JsonIgnore.class)
                    && !Modifier.isTransient(modifiers)
                    && Modifier.isPublic(modifiers)
                    && !properties.containsKey(name))
                    properties.put(name, new FieldProperty(f));
            }
        }
        try
        {
            PropertyDescriptor[] descriptors = Introspector.getBeanInfo(root).getPropertyDescriptors();
            if (descriptors != null)
            {
                for (PropertyDescriptor d : descriptors)
                {
                    String name = camelToSnake(d.getName());
                    Method writeMethod = d.getWriteMethod();
                    // if the property can't be written to, then ignore it
                    if (writeMethod == null || writeMethod.isAnnotationPresent(JsonIgnore.class))
                        continue;
                    // if read method exists, override the field version in case get/set does validation
                    if (properties.containsKey(name) && (d.getReadMethod() == null || d.getReadMethod().isAnnotationPresent(JsonIgnore.class)))
                        continue;
                    d.setName(name);
                    properties.put(name, new MethodPropertyPlus(d));
                }
            }
        }
        catch (IntrospectionException e)
        {
            throw new RuntimeException(e);
        }
        return properties;
    }

    /**
     * .get() acts differently than .set() and doesn't do a good job showing the cause of the failure, this
     * class rewrites to make the errors easier to reason with.
     */
    private static class MethodPropertyPlus extends MethodProperty
    {
        private final Method readMethod;

        public MethodPropertyPlus(PropertyDescriptor property)
        {
            super(property);
            this.readMethod = property.getReadMethod();
        }

        @Override
        public Object get(Object object)
        {
            if (!isReadable())
                throw new YAMLException("No readable property '" + getName() + "' on class: " + object.getClass().getName());

            try
            {
                return readMethod.invoke(object);
            }
            catch (IllegalAccessException e)
            {
                throw new YAMLException("Unable to find getter for property '" + getName() + "' on class " + object.getClass().getName(), e);
            }
            catch (InvocationTargetException e)
            {
                throw new YAMLException("Failed calling getter for property '" + getName() + "' on class " + object.getClass().getName(), e.getCause());
            }
        }
    }
}
