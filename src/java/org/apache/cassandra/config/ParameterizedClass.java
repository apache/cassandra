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

import com.google.common.base.Objects;

public class ParameterizedClass
{
    public static final String CLASS_NAME = "class_name";
    public static final String PARAMETERS = "parameters";

    public String class_name;
    public Map<String, String> parameters;

    public ParameterizedClass(String class_name, Map<String, String> parameters)
    {
        this.class_name = class_name;
        this.parameters = parameters;
    }

    @SuppressWarnings("unchecked")
    public ParameterizedClass(Map<String, ?> p)
    {
        this((String)p.get(CLASS_NAME),
             p.containsKey(PARAMETERS) ? (Map<String, String>)((List<?>)p.get(PARAMETERS)).get(0) : null);
    }

    @Override
    public boolean equals(Object that)
    {
        return that instanceof ParameterizedClass && equals((ParameterizedClass) that);
    }

    public boolean equals(ParameterizedClass that)
    {
        return Objects.equal(class_name, that.class_name) && Objects.equal(parameters, that.parameters);
    }

    @Override
    public String toString()
    {
        return class_name + (parameters == null ? "" : parameters.toString());
    }
}
