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

package org.apache.cassandra.distributed.shared;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Joiner;

import org.apache.cassandra.config.CassandraRelevantProperties;

public final class WithProperties implements AutoCloseable
{
    private final List<Property> properties = new ArrayList<>();

    public WithProperties()
    {
    }

    public WithProperties(String... kvs)
    {
        set(kvs);
    }

    public void set(String... kvs)
    {
        assert kvs.length % 2 == 0 : "Input must have an even amount of inputs but given " + kvs.length;
        for (int i = 0; i <= kvs.length - 2; i = i + 2)
        {
            set(CassandraRelevantProperties.getProperty(kvs[i]), kvs[i + 1]);
        }
    }

    public void set(CassandraRelevantProperties prop, String value)
    {
        properties.add(new Property(prop, prop.setString(value)));
    }

    public void set(CassandraRelevantProperties prop, String... values)
    {
        set(prop, Arrays.asList(values));
    }

    public void set(CassandraRelevantProperties prop, Collection<String> values)
    {
        set(prop, Joiner.on(",").join(values));
    }

    public void set(CassandraRelevantProperties prop, boolean value)
    {
        set(prop, Boolean.toString(value));
    }

    public void set(CassandraRelevantProperties prop, long value)
    {
        set(prop, Long.toString(value));
    }

    @Override
    public void close()
    {
        Collections.reverse(properties);
        properties.forEach(s -> {
            if (s.prevValue == null)
                s.prop.clearValue();
            else
                s.prop.setString(s.prevValue);
        });
        properties.clear();
    }

    private static final class Property
    {
        private final CassandraRelevantProperties prop;
        private final String prevValue;

        private Property(CassandraRelevantProperties prop, String prevValue)
        {
            this.prop = prop;
            this.prevValue = prevValue;
        }
    }
}
