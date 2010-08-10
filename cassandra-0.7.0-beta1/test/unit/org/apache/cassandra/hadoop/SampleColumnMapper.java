package org.apache.cassandra.hadoop;

/**
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

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A sample mapper that takes a pair of input &lt;key, value&gt; (writable)
 * integers, and writes them as &lt;key, column&gt; writables.
 */
public class SampleColumnMapper extends Mapper<IntWritable,IntWritable,IntWritable,ColumnWritable>
{
    protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException
    {
        byte[] columnNameAndValue = String.valueOf(value.get()).getBytes();
        context.write(key, new ColumnWritable(columnNameAndValue, columnNameAndValue));
    }
}