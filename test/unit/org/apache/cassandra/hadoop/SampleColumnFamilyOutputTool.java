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
import java.util.SortedMap;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.thrift.TException;

/**
 * The <code>SampleColumnFamilyOutputTool</code> provides a tool interface which
 * runs a {@link SampleColumnMapper} on the &lt;key, value&gt; pairs obtained
 * from a sequence file, and then reduces it through the default
 * {@link ColumnFamilyOutputReducer}.
 * 
 * @author Karthick Sankarachary
 * 
 */
public class SampleColumnFamilyOutputTool extends Configured implements Tool
{
    private Path inputdir;
    
    public SampleColumnFamilyOutputTool(Path inputdir, String columnFamily)
    {
        this.inputdir = inputdir;
    }
    
    public int run(String[] args)
    throws InvalidRequestException, TException, IOException, InterruptedException, ClassNotFoundException
    {
        Job job = new Job(new Configuration());
        
        // In case your job runs out of memory, use this setting 
        // (provided you're on Hadoop 0.20.1 or later)
        // job.getConfiguration().setInt(JobContext.IO_SORT_MB, 1);
        ConfigHelper.setOutputColumnFamily(job.getConfiguration(),
                                     ColumnFamilyOutputFormatTest.KEYSPACE,
                                     ColumnFamilyOutputFormatTest.COLUMN_FAMILY);
        ConfigHelper.setOutputSlicePredicate(job.getConfiguration(), new SlicePredicate());

        SequenceFileInputFormat.addInputPath(job, inputdir);
        
        job.setMapperClass(SampleColumnMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ColumnWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        
        job.setReducerClass(ColumnFamilyOutputReducer.class);
        job.setOutputKeyClass(byte[].class);
        job.setOutputValueClass(SortedMap.class);
        job.setOutputFormatClass(ColumnFamilyOutputFormat.class);
        
        job.waitForCompletion(true);
        return 0;
    }
}