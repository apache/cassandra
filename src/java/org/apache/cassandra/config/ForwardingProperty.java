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

import java.lang.annotation.Annotation;
import java.util.List;

import org.yaml.snakeyaml.introspector.Property;

/**
 * This class delegates all calls of {@link Property} to a {@link #delegate()}, used for cases where a small number of
 * methods want to be overriden from the delegate.
 *
 * This class acts as a decorator to a {@link Property} and allows decorating any method.
 */
public class ForwardingProperty extends Property
{
    private final Property delegate;

    public ForwardingProperty(String name, Property property)
    {
        this(name, property.getType(), property);
    }

    public ForwardingProperty(String name, Class<?> type, Property property)
    {
        super(name, type);
        this.delegate = property;
    }

    protected Property delegate()
    {
        return delegate;
    }

    @Override
    public boolean isWritable()
    {
        return delegate().isWritable();
    }

    @Override
    public boolean isReadable()
    {
        return delegate().isReadable();
    }

    @Override
    public Class<?>[] getActualTypeArguments()
    {
        return delegate().getActualTypeArguments();
    }

    @Override
    public void set(Object o, Object o1) throws Exception
    {
        delegate().set(o, o1);
    }

    @Override
    public Object get(Object o)
    {
        return delegate().get(o);
    }

    @Override
    public List<Annotation> getAnnotations()
    {
        return delegate().getAnnotations();
    }

    @Override
    public <A extends Annotation> A getAnnotation(Class<A> aClass)
    {
        return delegate().getAnnotation(aClass);
    }
}
