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
import java.util.Arrays;
import java.util.SortedMap;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This counts the occurrences of words in ColumnFamily Standard1, that has a single column (that we care about)
 * "text" containing a sequence of words.
 *
 * For each word, we output the total number of occurrences across all texts.
 */
public class WordCount extends Configured implements Tool
{
    private static final Logger logger = Logger.getLogger(WordCount.class);

    static final String KEYSPACE = "Keyspace1";
    static final String COLUMN_FAMILY = "Standard1";
    private static final String CONF_COLUMN_NAME = "columnname";
    private static final String OUTPUT_PATH_PREFIX = "/tmp/word_count";
    static final int RING_DELAY = 3000; // this is enough for testing a single server node; may need more for a real cluster

    public static void main(String[] args) throws Exception
    {
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(0);
    }

    public static class TokenizerMapper extends Mapper<String, SortedMap<byte[], IColumn>, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String columnName;

        public void map(String key, SortedMap<byte[], IColumn> columns, Context context) throws IOException, InterruptedException
        {
            IColumn column = columns.get(columnName.getBytes());
            if (column == null)
                return;
            String value = new String(column.value());
            logger.debug("read " + key + ":" + value + " from " + context.getInputSplit());

            StringTokenizer itr = new StringTokenizer(value);
            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException
        {
            this.columnName = context.getConfiguration().get(CONF_COLUMN_NAME);
        }
        
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();

        for (int i = 0; i < WordCountSetup.TEST_COUNT; i++)
        {
            String columnName = "text" + i;
            conf.set(CONF_COLUMN_NAME, columnName);
            Job job = new Job(conf, "wordcount");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setInputFormatClass(ColumnFamilyInputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX + i));

            ConfigHelper.setThriftContact(conf, "localhost",  9160);
            ConfigHelper.setColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY, "BytesType", "RandomPartitioner");
            SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(columnName.getBytes()));
            ConfigHelper.setSlicePredicate(job.getConfiguration(), predicate);

            job.waitForCompletion(true);
        }
        return 0;
    }
}
