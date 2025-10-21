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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.Generators;
import org.assertj.core.api.Assertions;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.apache.cassandra.utils.FailingConsumer.orFail;
import static org.quicktheories.QuickTheory.qt;

public class JMXToolTest
{
    @Test
    public void jsonSerde()
    {
        serde(JMXTool.Dump.Format.json, JMXTool.Diff.Format.json);
    }

    @Test
    public void yamlSerde()
    {
        serde(JMXTool.Dump.Format.yaml, JMXTool.Diff.Format.yaml);
    }

    @Test
    public void cliHelp()
    {
        ToolResult result = jmxtool();
        result.assertOnCleanExit();

        Assertions.assertThat(result.getStdout())
                  .isEqualTo("Usage: jmxtool [-hV] [COMMAND]\n" +
                             "JMX tool for Apache Cassandra\n" +
                             "  -h, --help      Show this help message and exit.\n" +
                             "  -V, --version   Print version information and exit.\n" +
                             "Commands:\n" +
                             "  help  Display help information about the specified command.\n" +
                             "  dump  Dump the Apache Cassandra JMX objects and metadata.\n" +
                             "  diff  Diff two jmx dump files and report their differences\n");
    }

    @Test
    public void cliHelpDiff()
    {
        ToolResult result = jmxtool("help", "diff");
        result.assertOnCleanExit();

        Assertions.assertThat(result.getStdout())
                  .isEqualTo("Usage: jmxtool diff [-hV] [--ignore-missing-on-left]\n" +
                             "                    [--ignore-missing-on-right] [-f=format]\n" +
                             "                    [--exclude-attribute=exclude_attributes]...\n" +
                             "                    [--exclude-object=exclude_objects]...\n" +
                             "                    [--exclude-operation=exclud_operations]... [<files>...]\n" +
                             "Diff two jmx dump files and report their differences\n" +
                             "      [<files>...]      Files to diff\n" +
                             "      --exclude-attribute=exclude_attributes\n" +
                             "                        Ignores processing specific attributes. Each usage\n" +
                             "                          should take a single attribute, but can use this flag\n" +
                             "                          multiple times.\n" +
                             "      --exclude-object=exclude_objects\n" +
                             "                        Ignores processing specific objects. Each usage should\n" +
                             "                          take a single object, but can use this flag multiple\n" +
                             "                          times.\n" +
                             "      --exclude-operation=exclud_operations\n" +
                             "                        Ignores processing specific operations. Each usage\n" +
                             "                          should take a single operation, but can use this flag\n" +
                             "                          multiple times.\n" +
                             "  -f, --format=format   What format the files are in; only support json and\n" +
                             "                          yaml as format\n" +
                             "  -h, --help            Show this help message and exit.\n" +
                             "      --ignore-missing-on-left\n" +
                             "                        Ignore results missing on the left\n" +
                             "      --ignore-missing-on-right\n" +
                             "                        Ignore results missing on the right\n" +
                             "  -V, --version         Print version information and exit.\n");
    }

    @Test
    public void cliHelpDump()
    {
        ToolResult result = jmxtool("help", "dump");
        result.assertOnCleanExit();

        Assertions.assertThat(result.getStdout())
                  .isEqualTo("Usage: jmxtool dump [-hV] [-f=format] [-u=url]\n" +
                             "Dump the Apache Cassandra JMX objects and metadata.\n" +
                             "  -f, --format=format   What format to dump content as; supported values are\n" +
                             "                          console (default), json, and yaml\n" +
                             "  -h, --help            Show this help message and exit.\n" +
                             "  -u, --url=url         JMX url to target\n" +
                             "  -V, --version         Print version information and exit.\n");
    }

    private static ToolResult jmxtool(String... args)
    {
        List<String> cmd = new ArrayList<>(1 + args.length);
        cmd.add("tools/bin/jmxtool");
        cmd.addAll(Arrays.asList(args));
        return ToolRunner.invoke(cmd);
    }

    private void serde(JMXTool.Dump.Format serializer, JMXTool.Diff.Format deserializer)
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().withShrinkCycles(0).forAll(gen()).checkAssert(orFail(map -> serde(serializer, deserializer, buffer, map)));
    }

    private void serde(JMXTool.Dump.Format serializer,
                       JMXTool.Diff.Format deserializer,
                       DataOutputBuffer buffer,
                       Map<String, JMXTool.Info> map) throws IOException
    {
        buffer.clear();
        serializer.dump(buffer, map);
        Map<String, JMXTool.Info> read = deserializer.load(new DataInputBuffer(buffer.buffer(), false));
        Assertions.assertThat(read)
                  .as("property deserialize(serialize(value)) == value failed")
                  .isEqualTo(map);
    }

    private static final Gen<JMXTool.Attribute> attributeGen = Generators.IDENTIFIER_GEN.zip(Generators.IDENTIFIER_GEN, Generators.IDENTIFIER_GEN, JMXTool.Attribute::new);
    private static final Gen<JMXTool.Parameter> parameterGen = Generators.IDENTIFIER_GEN.zip(Generators.IDENTIFIER_GEN, JMXTool.Parameter::new);
    private static final Gen<JMXTool.Operation> operationGen = Generators.IDENTIFIER_GEN.zip(SourceDSL.arrays().ofClass(parameterGen, JMXTool.Parameter.class).withLengthBetween(0, 10), Generators.IDENTIFIER_GEN, JMXTool.Operation::new);
    private static final Gen<JMXTool.Info> infoGen = SourceDSL.arrays().ofClass(attributeGen, JMXTool.Attribute.class).withLengthBetween(0, 10).zip(SourceDSL.arrays().ofClass(operationGen, JMXTool.Operation.class).withLengthBetween(0, 10), JMXTool.Info::new);

    private static Gen<Map<String, JMXTool.Info>> gen()
    {
        return SourceDSL.maps().of(Generators.IDENTIFIER_GEN, infoGen).ofSizeBetween(0, 10);
    }
}
