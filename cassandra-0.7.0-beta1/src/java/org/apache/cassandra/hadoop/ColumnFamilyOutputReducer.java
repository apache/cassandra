package org.apache.cassandra.hadoop;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The <code>ColumnFamilyOutputReducer</code> reduces a &lt;key, values&gt;
 * pair, where the value is a generic iterable type, into a list of columns that
 * need to be mutated for that key, where each column corresponds to an element
 * in the value.
 * 
 * <p>
 * The default implementation treats the VALUEIN type to be a
 * {@link ColumnWritable}, in which case this reducer acts as an identity
 * function.
 * 
 * @author Karthick Sankarachary
 * 
 * @param <KEYIN>
 */
public class ColumnFamilyOutputReducer<KEYIN, VALUEIN> extends Reducer<KEYIN, VALUEIN, byte[], List<IColumn>>
{
    public void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException, InterruptedException
    {
        byte[] cfKey = key.toString().getBytes();
        List<IColumn> cfColumns = new ArrayList<IColumn>();
        for (VALUEIN value : values)
        {
            ColumnWritable columnValue = map(value);
            cfColumns.add(new Column(columnValue.getName(), columnValue.getValue()));
        }
        context.write(cfKey, cfColumns);
    }

    protected ColumnWritable map(VALUEIN value)
    {
        return (ColumnWritable) value;
    }
}