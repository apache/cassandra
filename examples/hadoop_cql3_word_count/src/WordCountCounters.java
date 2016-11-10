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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.datastax.driver.core.Row;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.utils.ByteBufferUtil;


/**
 * This sums the word count stored in the input_words_count ColumnFamily for the key "sum".
 *
 * Output is written to a text file.
 */
public class WordCountCounters extends Configured implements Tool
{
    private static final Logger logger = LoggerFactory.getLogger(WordCountCounters.class);

    static final String INPUT_MAPPER_VAR = "input_mapper";
    static final String COUNTER_COLUMN_FAMILY = "input_words_count";
    private static final String OUTPUT_PATH_PREFIX = "/tmp/word_count_counters";

    public static void main(String[] args) throws Exception
    {
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new WordCountCounters(), args);
        System.exit(0);
    }

    public static class SumNativeMapper extends Mapper<Long, Row, Text, LongWritable>
    {
        long sum = -1;
        public void map(Long key, Row row, Context context) throws IOException, InterruptedException
        {   
            if (sum < 0)
                sum = 0;

            logger.debug("read " + key + ":count_num from " + context.getInputSplit());
            sum += Long.valueOf(row.getString("count_num"));
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (sum > 0)
                context.write(new Text("total_count"), new LongWritable(sum));
        }
    }

    public static class SumMapper extends Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, LongWritable>
    {
        long sum = -1;

        public void map(Map<String, ByteBuffer> key, Map<String, ByteBuffer> columns, Context context) throws IOException, InterruptedException
        {   
            if (sum < 0)
                sum = 0;

            logger.debug("read " + toString(key) + ":count_num from " + context.getInputSplit());
            sum += Long.valueOf(ByteBufferUtil.string(columns.get("count_num")));
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (sum > 0)
                context.write(new Text("total_count"), new LongWritable(sum));
        }

        private String toString(Map<String, ByteBuffer> keys)
        {
            String result = "";
            try
            {
                for (ByteBuffer key : keys.values())
                    result = result + ByteBufferUtil.string(key) + ":";
            }
            catch (CharacterCodingException e)
            {
                logger.error("Failed to print keys", e);
            }
            return result;
        }
    }

    public static class ReducerToFilesystem extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        long sum = 0;

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
        {
            for (LongWritable val : values)
                sum += val.get();
            context.write(key, new LongWritable(sum));
        }
    }

    public int run(String[] args) throws Exception
    {
        String inputMapperType = "native";
        if (args != null && args[0].startsWith(INPUT_MAPPER_VAR))
        {
            String[] arg0 = args[0].split("=");
            if (arg0 != null && arg0.length == 2)
                inputMapperType = arg0[1];
        }
        Job job = new Job(getConf(), "wordcountcounters");

        job.setCombinerClass(ReducerToFilesystem.class);
        job.setReducerClass(ReducerToFilesystem.class);
        job.setJarByClass(WordCountCounters.class); 

        ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), WordCount.KEYSPACE, WordCount.OUTPUT_COLUMN_FAMILY);

        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3");
        if ("native".equals(inputMapperType))
        {
            job.setMapperClass(SumNativeMapper.class);
            job.setInputFormatClass(CqlInputFormat.class);
            CqlConfigHelper.setInputCql(job.getConfiguration(), "select * from " + WordCount.OUTPUT_COLUMN_FAMILY + " where token(word) > ? and token(word) <= ? allow filtering");
        }
        else
        {
            job.setMapperClass(SumMapper.class);
            job.setInputFormatClass(CqlInputFormat.class);
            ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX));
        job.waitForCompletion(true);
        return 0;
    }
}
