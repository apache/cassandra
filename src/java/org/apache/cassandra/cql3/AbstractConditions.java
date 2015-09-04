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
package org.apache.cassandra.cql3;

import java.util.Collections;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.functions.Function;

/**
 * Base class for <code>Conditions</code> classes.
 *
 */
abstract class AbstractConditions implements Conditions
{
    public Iterable<Function> getFunctions()
    {
        return Collections.emptyList();
    }

    public Iterable<ColumnDefinition> getColumns()
    {
        return null;
    }

    public boolean isEmpty()
    {
        return false;
    }

    public boolean appliesToStaticColumns()
    {
        return false;
    }

    public boolean appliesToRegularColumns()
    {
        return false;
    }

    public boolean isIfExists()
    {
        return false;
    }

    public boolean isIfNotExists()
    {
        return false;
    }
}
