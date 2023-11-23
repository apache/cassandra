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

import java.util.Map;

import org.yaml.snakeyaml.introspector.Property;

public class WithReplacementLoader implements Loader
{
    private final Loader delegate;

    public WithReplacementLoader(Loader delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public Map<String, Property> getProperties(Class<?> root)
    {
        Map<String, Property> properties = delegate.getProperties(root);
        // only handling top-level replacements for now, previous logic was only top level so not a regression
        for (Replacement r : Replacements.getReplacements(root))
        {
            Property latest = properties.get(r.newName);
            assert latest != null : "Unable to find replacement new name: " + r.newName;
            Property conflict = properties.put(r.oldName, r.toProperty(latest));
            // some configs kept the same name, but changed the type, if this is detected then rely on the replaced property
            assert conflict == null || r.oldName.equals(r.newName) : String.format("New property %s attempted to replace %s, but this property already exists", latest.getName(), conflict.getName());
        }
        return properties;
    }
}
