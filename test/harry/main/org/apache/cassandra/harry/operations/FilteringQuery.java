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

package org.apache.cassandra.harry.operations;

import java.util.List;

import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.reconciler.Reconciler;
import org.apache.cassandra.harry.util.DescriptorRanges;

public class FilteringQuery extends Query
{
    public FilteringQuery(long pd,
                          boolean reverse,
                          List<Relation> relations,
                          SchemaSpec schemaSpec)
    {
        super(QueryKind.SINGLE_PARTITION, pd, reverse, relations, schemaSpec);
    }

    public boolean match(Reconciler.RowState rowState)
    {
        for (Relation relation : relations)
        {
            switch (relation.columnSpec.kind)
            {
                case CLUSTERING:
                    if (!matchCd(rowState.cd))
                        return false;
                    break;
                case REGULAR:
                    if (!relation.match(rowState.vds[relation.columnSpec.getColumnIndex()]))
                        return false;
                    break;
                case STATIC:
                    if (!relation.match(rowState.partitionState.staticRow().vds[relation.columnSpec.getColumnIndex()]))
                        return false;
                    break;
                case PARTITION_KEY:
                    if (!relation.match(rowState.partitionState.pd))
                        return false;
                    break;
            }
        }
        return true;
    }

    public DescriptorRanges.DescriptorRange toRange(long ts)
    {
        throw new IllegalStateException("not implemented for filtering query");
    }

    @Override
    public String toString()
    {
        return "FilteringQuery{pd=" + pd + ", reverse=" + reverse + ", relations=" + relations + '}';
    }
}
