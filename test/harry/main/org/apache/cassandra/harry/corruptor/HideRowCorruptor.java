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

package org.apache.cassandra.harry.corruptor;

import org.apache.cassandra.harry.data.ResultSetRow;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.operations.DeleteHelper;

public class HideRowCorruptor implements RowCorruptor
{
    private final SchemaSpec schema;
    private final OpSelectors.Clock clock;

    public HideRowCorruptor(SchemaSpec schemaSpec,
                            OpSelectors.Clock clock)
    {
        this.schema = schemaSpec;
        this.clock = clock;
    }

    // Can corrupt any row that has at least one written non-null value
    public boolean canCorrupt(ResultSetRow row)
    {
        return row != null;
    }

    public CompiledStatement corrupt(ResultSetRow row)
    {
        return DeleteHelper.deleteRow(schema, row.pd, row.cd, clock.rts(clock.peek()));
    }
}
