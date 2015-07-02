/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import com.google.common.base.Objects;

public final class TriggerMetadata
{
    public static final String CLASS = "class";

    public final String name;

    // For now, the only supported option is 'class'.
    // Proper trigger parametrization will be added later.
    public final String classOption;

    public TriggerMetadata(String name, String classOption)
    {
        this.name = name;
        this.classOption = classOption;
    }

    public static TriggerMetadata create(String name, String classOption)
    {
        return new TriggerMetadata(name, classOption);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof TriggerMetadata))
            return false;

        TriggerMetadata td = (TriggerMetadata) o;

        return name.equals(td.name) && classOption.equals(td.classOption);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, classOption);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add("name", name)
                      .add("class", classOption)
                      .toString();
    }
}
