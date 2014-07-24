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
import java.util.*;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.utils.ByteBufferUtil;
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
 *
 * When outputting to Cassandra, we write the word counts as a {word, count} column/value pair,
 * with a row key equal to the name of the source column we read the words from.
 */
public class WordCount extends Configured implements Tool
{
    private static final Logger logger = LoggerFactory.getLogger(WordCount.class);

    static final String KEYSPACE = "wordcount";
    static final String COLUMN_FAMILY = "input_words";

    static final String OUTPUT_REDUCER_VAR = "output_reducer";
    static final String OUTPUT_COLUMN_FAMILY = "output_words";
    private static final String OUTPUT_PATH_PREFIX = "/tmp/word_count";

    private static final String CONF_COLUMN_NAME = "columnname";

    public static void main(String[] args) throws Exception
    {
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(0);
    }

    public static class TokenizerMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, Cell>, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private ByteBuffer sourceColumn;

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
        {
        }

        public void map(ByteBuffer key, SortedMap<ByteBuffer, Cell> columns, Context context) throws IOException, InterruptedException
        {
            for (Cell cell : columns.values())
            {
                String name  = ByteBufferUtil.string(cell.name().toByteBuffer());
                String value = null;
                
                if (name.contains("int"))
                    value = String.valueOf(ByteBufferUtil.toInt(cell.value()));
                else
                    value = ByteBufferUtil.string(cell.value());
                               
                logger.debug("read {}:{}={} from {}",
                             new Object[] {ByteBufferUtil.string(key), name, value, context.getInputSplit()});

                StringTokenizer itr = new StringTokenizer(value);
                while (itr.hasMoreTokens())
                {
                    word.set(itr.nextToken());
                    context.write(word, one);
                }
            }
        }
    }

    public static class ReducerToFilesystem extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            context.write(key, new IntWritable(sum));
        }
    }

    public static class ReducerToCassandra extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>>
    {
        private ByteBuffer outputKey;

        protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
        throws IOException, InterruptedException
        {
            outputKey = ByteBufferUtil.bytes(context.getConfiguration().get(CONF_COLUMN_NAME));
        }

        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            context.write(outputKey, Collections.singletonList(getMutation(word, sum)));
        }

        private static Mutation getMutation(Text word, int sum)
        {
            org.apache.cassandra.thrift.Column c = new org.apache.cassandra.thrift.Column();
            c.setName(Arrays.copyOf(word.getBytes(), word.getLength()));
            c.setValue(ByteBufferUtil.bytes(sum));
            c.setTimestamp(System.currentTimeMillis());

            Mutation m = new Mutation();
            m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
            m.column_or_supercolumn.setColumn(c);
            return m;
        }
    }

    public int run(String[] args) throws Exception
    {
        String outputReducerType = "filesystem";
        if (args != null && args[0].startsWith(OUTPUT_REDUCER_VAR))
        {
            String[] s = args[0].split("=");
            if (s != null && s.length == 2)
                outputReducerType = s[1];
        }
        logger.info("output reducer type: " + outputReducerType);

        // use a smaller page size that doesn't divide the row count evenly to exercise the paging logic better
        ConfigHelper.setRangeBatchSize(getConf(), 99);

        for (int i = 0; i < WordCountSetup.TEST_COUNT; i++)
        {
            String columnName = "text" + i;

            Job job = new Job(getConf(), "wordcount");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(TokenizerMapper.class);

            if (outputReducerType.equalsIgnoreCase("filesystem"))
            {
                job.setCombinerClass(ReducerToFilesystem.class);
                job.setReducerClass(ReducerToFilesystem.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX + i));
            }
            else
            {
                job.setReducerClass(ReducerToCassandra.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(IntWritable.class);
                job.setOutputKeyClass(ByteBuffer.class);
                job.setOutputValueClass(List.class);

                job.setOutputFormatClass(ColumnFamilyOutputFormat.class);

                ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);
                job.getConfiguration().set(CONF_COLUMN_NAME, "sum");
            }

            job.setInputFormatClass(ColumnFamilyInputFormat.class);

            ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
            ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
            ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
            ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
            SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(columnName)));
            ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

            if (i == 4)
            {
                IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("int4"), IndexOperator.EQ, ByteBufferUtil.bytes(0));
                ConfigHelper.setInputRange(job.getConfiguration(), Arrays.asList(expr));
            }

            if (i == 5)
            {
                // this will cause the predicate to be ignored in favor of scanning everything as a wide row
                ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY, true);
            }

            ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "localhost");
            ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");

            job.waitForCompletion(true);
        }
        return 0;
    }
}
