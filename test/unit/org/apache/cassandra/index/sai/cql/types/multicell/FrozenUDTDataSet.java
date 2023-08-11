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

package org.apache.cassandra.index.sai.cql.types.multicell;

import java.util.Collection;
import java.util.Collections;

import org.apache.cassandra.index.sai.cql.types.DataSet;
import org.apache.cassandra.index.sai.cql.types.QuerySet;

import static org.apache.cassandra.index.sai.cql.types.IndexingTypeSupport.NUMBER_OF_VALUES;

public class FrozenUDTDataSet extends DataSet<Object>
{
    private final DataSet<?>[] elementDataSets;

    private volatile String udt;

    public FrozenUDTDataSet(DataSet<?>... elementDataSets)
    {
        this.elementDataSets = elementDataSets;

        values = new Object[NUMBER_OF_VALUES];
        for (int index = 0; index < NUMBER_OF_VALUES; index++)
        {
            Object[] fields = new Object[elementDataSets.length * 2]; // field name and filed value
            for (int i = 0; i < elementDataSets.length; i++)
            {
                fields[i * 2] = elementDataSets[i].toString();
                fields[i * 2 + 1] = elementDataSets[i].values[getRandom().nextIntBetween(0, elementDataSets[i].values.length - 1)];
            }

            values[index] = userType(fields);
        }
    }

    @Override
    public void init()
    {
        StringBuilder fields = new StringBuilder();
        for (int i = 0; i < elementDataSets.length; i++)
        {
            if (i != 0)
                fields.append(", ");

            fields.append("v_").append(i).append(' ').append(elementDataSets[i]);
        }
        udt = createType(String.format("CREATE TYPE %%s(%s)", fields));
    }

    @Override
    public QuerySet querySet()
    {
        return new QuerySet.FrozenTuple();
    }

    @Override
    public Collection<String> decorateIndexColumn(String column)
    {
        return Collections.singletonList(column);
    }

    public String toString()
    {
        return String.format("frozen<%s>", udt);
    }
}
