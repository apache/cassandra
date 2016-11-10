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
import java.util.Map.Entry;

import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
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
import com.datastax.driver.core.Row;

/**
 * This counts the occurrences of words in ColumnFamily
 *   cql3_wordcount ( id uuid,
 *                   line  text,
 *                   PRIMARY KEY (id))
 *
 * For each word, we output the total number of occurrences across all body texts.
 *
 * When outputting to Cassandra, we write the word counts to column family
 *  output_words ( word text,
 *                 count_num text,
 *                 PRIMARY KEY (word))
 * as a {word, count} to columns: word, count_num with a row key of "word sum"
 */
public class WordCount extends Configured implements Tool
{
    private static final Logger logger = LoggerFactory.getLogger(WordCount.class);
    static final String INPUT_MAPPER_VAR = "input_mapper";
    static final String KEYSPACE = "cql3_wordcount";
    static final String COLUMN_FAMILY = "inputs";

    static final String OUTPUT_REDUCER_VAR = "output_reducer";
    static final String OUTPUT_COLUMN_FAMILY = "output_words";

    private static final String OUTPUT_PATH_PREFIX = "/tmp/word_count";
    private static final String PRIMARY_KEY = "row_key";

    public static void main(String[] args) throws Exception
    {
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(0);
    }

    public static class TokenizerMapper extends Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private ByteBuffer sourceColumn;

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
        {
        }

        public void map(Map<String, ByteBuffer> keys, Map<String, ByteBuffer> columns, Context context) throws IOException, InterruptedException
        {
            for (Entry<String, ByteBuffer> column : columns.entrySet())
            {
                if (!"line".equalsIgnoreCase(column.getKey()))
                    continue;

                String value = ByteBufferUtil.string(column.getValue());

                StringTokenizer itr = new StringTokenizer(value);
                while (itr.hasMoreTokens())
                {
                    word.set(itr.nextToken());
                    context.write(word, one);
                }
            }
        }
    }

    public static class NativeTokenizerMapper extends Mapper<Long, Row, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private ByteBuffer sourceColumn;

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
        {
        }

        public void map(Long key, Row row, Context context) throws IOException, InterruptedException
        {
            String value = row.getString("line");
            logger.debug("read {}:{}={} from {}", key, "line", value, context.getInputSplit());
            StringTokenizer itr = new StringTokenizer(value);
            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                context.write(word, one);
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

    public static class ReducerToCassandra extends Reducer<Text, IntWritable, Map<String, ByteBuffer>, List<ByteBuffer>>
    {
        private Map<String, ByteBuffer> keys;
        private ByteBuffer key;
        protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
        throws IOException, InterruptedException
        {
            keys = new LinkedHashMap<String, ByteBuffer>();
        }

        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            keys.put("word", ByteBufferUtil.bytes(word.toString()));
            context.write(keys, getBindVariables(word, sum));
        }

        private List<ByteBuffer> getBindVariables(Text word, int sum)
        {
            List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
            variables.add(ByteBufferUtil.bytes(String.valueOf(sum)));         
            return variables;
        }
    }

    public int run(String[] args) throws Exception
    {
        String outputReducerType = "filesystem";
        String inputMapperType = "native";
        String outputReducer = null;
        String inputMapper = null;

        if (args != null)
        {
            if(args[0].startsWith(OUTPUT_REDUCER_VAR))
                outputReducer = args[0];
            if(args[0].startsWith(INPUT_MAPPER_VAR))
                inputMapper = args[0];
            
            if (args.length == 2)
            {
                if(args[1].startsWith(OUTPUT_REDUCER_VAR))
                    outputReducer = args[1];
                if(args[1].startsWith(INPUT_MAPPER_VAR))
                    inputMapper = args[1]; 
            }
        }

        if (outputReducer != null)
        {
            String[] s = outputReducer.split("=");
            if (s != null && s.length == 2)
                outputReducerType = s[1];
        }
        logger.info("output reducer type: " + outputReducerType);
        if (inputMapper != null)
        {
            String[] s = inputMapper.split("=");
            if (s != null && s.length == 2)
                inputMapperType = s[1];
        }
        Job job = new Job(getConf(), "wordcount");
        job.setJarByClass(WordCount.class);

        if (outputReducerType.equalsIgnoreCase("filesystem"))
        {
            job.setCombinerClass(ReducerToFilesystem.class);
            job.setReducerClass(ReducerToFilesystem.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX));
        }
        else
        {
            job.setReducerClass(ReducerToCassandra.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Map.class);
            job.setOutputValueClass(List.class);

            job.setOutputFormatClass(CqlOutputFormat.class);

            ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);
            job.getConfiguration().set(PRIMARY_KEY, "word,sum");
            String query = "UPDATE " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY +
                           " SET count_num = ? ";
            CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
            ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "localhost");
            ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
        }

        if (inputMapperType.equalsIgnoreCase("native"))
        {
            job.setMapperClass(NativeTokenizerMapper.class);
            job.setInputFormatClass(CqlInputFormat.class);
            CqlConfigHelper.setInputCql(job.getConfiguration(), "select * from " + COLUMN_FAMILY + " where token(id) > ? and token(id) <= ? allow filtering");
        }
        else
        {
            job.setMapperClass(TokenizerMapper.class);
            job.setInputFormatClass(CqlInputFormat.class);
            ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
        }

        ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");

        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3");
        job.waitForCompletion(true);
        return 0;
    }
}
