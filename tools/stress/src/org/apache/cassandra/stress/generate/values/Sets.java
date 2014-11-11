/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.stress.generate.values;


import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.db.marshal.SetType;

public class Sets extends Generator<Set>
{
    final Generator valueType;

    public Sets(String name, Generator valueType, GeneratorConfig config)
    {
        super(SetType.getInstance(valueType.type, true), config, name, Set.class);
        this.valueType = valueType;
    }

    public void setSeed(long seed)
    {
        super.setSeed(seed);
        valueType.setSeed(seed * 31);
    }

    @Override
    public Set generate()
    {
        final Set set = new HashSet();
        int size = (int) sizeDistribution.next();
        for (int i = 0 ; i < size ; i++)
            set.add(valueType.generate());
        return set;
    }
}
