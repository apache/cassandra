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

package org.apache.cassandra.harry.dsl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.OpSelectors;

class ValueHelper implements ValueOverrides
{
    protected Map<String, OverridingBijection<?>> overrides = new HashMap<>();
    protected Map<String, ValueDescriptorIndexGenerator> descriptorGenerators = new HashMap<>();

    protected final List<ColumnSpec<?>> regularColumns;
    protected final List<ColumnSpec<?>> staticColumns;

    @SuppressWarnings("rawtypes,unchecked")
    public ValueHelper(SchemaSpec orig, OpSelectors.PureRng rng)
    {
        this.regularColumns = new ArrayList<>();
        for (ColumnSpec<?> regular : orig.regularColumns)
        {
            OverridingBijection override = new OverridingBijection<>(regular.generator());
            regular = regular.override(override);
            this.regularColumns.add(regular);
            this.overrides.put(regular.name, override);
            this.descriptorGenerators.put(regular.name, new ValueDescriptorIndexGenerator(regular, rng));
        }

        this.staticColumns = new ArrayList<>();
        for (ColumnSpec<?> static_ : orig.staticColumns)
        {
            OverridingBijection override = new OverridingBijection<>(static_.generator());
            static_ = static_.override(override);
            this.staticColumns.add(static_);
            this.overrides.put(static_.name, override);
            this.descriptorGenerators.put(static_.name, new ValueDescriptorIndexGenerator(static_, rng));
        }
    }

    @Override
    @SuppressWarnings("unchecked,rawtypes")
    public void override(String columnName, int idx, Object override)
    {
        OverridingBijection gen = overrides.get(columnName);
        if (gen == null)
            throw new IllegalStateException(String.format("Overrides for %s are not supported", columnName));
        if (idx == ValueDescriptorIndexGenerator.UNSET)
            throw new IllegalStateException("Can't override an UNSET value");

        gen.override(descriptorGenerators.get(columnName).inflate(idx), override);
    }
}
