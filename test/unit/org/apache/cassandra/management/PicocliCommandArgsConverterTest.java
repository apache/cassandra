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

package org.apache.cassandra.management;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.api.CommandMetadata;
import org.apache.cassandra.management.api.OptionMetadata;
import org.apache.cassandra.management.picocli.PicocliCommandArgsConverter;
import org.apache.cassandra.management.picocli.PicocliCommandMetadata;
import org.apache.cassandra.service.AsyncProfilerService.AsyncProfilerEvent;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.AbstractCommand;
import org.apache.cassandra.tools.nodetool.AsyncProfileCommandGroup.AsyncProfileStartCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static org.assertj.core.api.Assertions.assertThat;

public class PicocliCommandArgsConverterTest
{
    @Test
    public void testFromCommandDefaultValueHandling()
    {
        CommandExecutionArgs annotationDefaultArgs = PicocliCommandArgsConverter.fromCommand(new CommandWithAnnotationDefaults());
        assertThat(findOptionValue(annotationDefaultArgs, "--name")).isEqualTo("default-name");
        assertThat(findOptionValue(annotationDefaultArgs, "--count")).isEqualTo(1);
        assertThat(findOptionValue(annotationDefaultArgs, "--items")).isEqualTo(List.of());
        assertThat(findOptionValue(annotationDefaultArgs, "--flag")).isNull();
        assertThat(annotationDefaultArgs.parameters()).isEmpty();

        CommandWithAnnotationDefaults overridden = new CommandWithAnnotationDefaults();
        overridden.flag = true;
        overridden.name = "cassandra-node-1";
        overridden.count = 42;
        overridden.items = List.of("a", "b", "c");
        overridden.target = "node1";

        CommandExecutionArgs overriddenArgs = PicocliCommandArgsConverter.fromCommand(overridden);
        assertThat(findOptionValue(overriddenArgs, "--flag")).isEqualTo(Boolean.TRUE);
        assertThat(findOptionValue(overriddenArgs, "--name")).isEqualTo("cassandra-node-1");
        assertThat(findOptionValue(overriddenArgs, "--count")).isEqualTo(42);
        assertThat(findOptionValue(overriddenArgs, "--items")).isEqualTo(List.of("a", "b", "c"));
        assertThat(overriddenArgs.parameters()).hasSize(1);
        assertThat(overriddenArgs.parameters().values().iterator().next()).isEqualTo("node1");

        CommandExecutionArgs javaDefaultArgs = PicocliCommandArgsConverter.fromCommand(new CommandWithJavaDefaults());
        assertThat(findOptionValue(javaDefaultArgs, "--name")).isEqualTo("default-name");
        assertThat(findOptionValue(javaDefaultArgs, "--count")).isEqualTo(0);
        assertThat(findOptionValue(javaDefaultArgs, "--flag")).isNull();
        assertThat(javaDefaultArgs.parameters()).isEmpty();
    }

    @Test
    public void testRoundTripSimpleCommand()
    {
        CommandWithJavaDefaults source = new CommandWithJavaDefaults();
        source.flag = true;
        source.name = "round-trip";
        source.count = 7;
        source.items = List.of("p", "q");
        source.target = "node42";

        CommandExecutionArgs args = PicocliCommandArgsConverter.fromCommand(source);

        CommandWithJavaDefaults target = new CommandWithJavaDefaults();
        PicocliCommandArgsConverter.toCommand(args, target);

        assertThat(target.flag).isTrue();
        assertThat(target.name).isEqualTo("round-trip");
        assertThat(target.count).isEqualTo(7);
        assertThat(target.items).containsExactly("p", "q");
        assertThat(target.target).isEqualTo("node42");
    }

    @Test
    public void testRoundTripMixinCommand()
    {
        CommandExecutionArgs defaultArgs = PicocliCommandArgsConverter.fromCommand(new CommandWithMixin());
        assertThat(findOptionValue(defaultArgs, "--verbose")).isNull();

        CommandWithMixin source = new CommandWithMixin();
        source.verboseMixin.verbose = true;
        source.host = "node-dc2";

        CommandExecutionArgs args = PicocliCommandArgsConverter.fromCommand(source);

        CommandWithMixin target = new CommandWithMixin();
        PicocliCommandArgsConverter.toCommand(args, target);

        assertThat(target.verboseMixin.verbose).isTrue();
        assertThat(target.host).isEqualTo("node-dc2");
    }

    @Test
    public void testSetOfEnumElementsAreConverted()
    {
        CollectionElementCommand command = new CollectionElementCommand();
        CommandMetadata metadata = PicocliCommandMetadata.from(command);

        Map<String, Object> raw = Map.of("colorSet", List.of("green", "red"));
        CommandExecutionArgs args = CommandExecutionArgsSerde.fromMap(raw, metadata);

        PicocliCommandArgsConverter.toCommand(args, command);

        String joined = command.colorSet.stream().map(Enum::name).collect(Collectors.joining(","));
        assertThat(joined).contains("green").contains("red");
        assertThat(command.colorSet).containsExactlyInAnyOrder(Color.green, Color.red);
    }

    @Test
    public void testArrayOfEnumElementsAreConverted()
    {
        CollectionElementCommand command = new CollectionElementCommand();
        CommandMetadata metadata = PicocliCommandMetadata.from(command);

        Map<String, Object> raw = Map.of("colorArray", List.of("red", "green"));
        CommandExecutionArgs args = CommandExecutionArgsSerde.fromMap(raw, metadata);

        PicocliCommandArgsConverter.toCommand(args, command);

        assertThat(command.colorArray).containsExactly(Color.red, Color.green);
    }

    @Test
    public void testAsyncProfilerStartEventListIsConverted()
    {
        AsyncProfileStartCommand command = new AsyncProfileStartCommand();
        CommandMetadata metadata = PicocliCommandMetadata.from(command);

        // The grammar parses "cpu,alloc" into a list of String literals.
        Map<String, Object> raw = Map.of("event", List.of("cpu", "alloc"));
        CommandExecutionArgs args = CommandExecutionArgsSerde.fromMap(raw, metadata);

        PicocliCommandArgsConverter.toCommand(args, command);

        assertThat(command.event).containsExactly(AsyncProfilerEvent.cpu, AsyncProfilerEvent.alloc);

        String joined = command.event.stream().map(Enum::name).collect(Collectors.joining(","));
        assertThat(joined).isEqualTo("cpu,alloc");
    }

    private static Object findOptionValue(CommandExecutionArgs args, String optionName)
    {
        for (java.util.Map.Entry<OptionMetadata, Object> entry : args.options().entrySet())
        {
            if (List.of(entry.getKey().names()).contains(optionName))
                return entry.getValue();
        }
        return null;
    }

    @Command(name = "command-with-annotation-defaults")
    static class CommandWithAnnotationDefaults extends AbstractCommand
    {
        @Option(names = { "--flag", "-f" }, description = "A boolean flag")
        boolean flag = false;

        @Option(names = { "--name", "-n" }, description = "A string option", paramLabel = "name", defaultValue = "default-name")
        String name = "default-name";

        @Option(names = { "--count", "-c" }, description = "An integer option", paramLabel = "count", defaultValue = "1")
        int count = 1;

        @Option(names = { "--items" }, description = "A list option", paramLabel = "items")
        List<String> items = List.of();

        @Parameters(index = "0", paramLabel = "target", description = "A positional parameter", arity = "0..1")
        String target = null;

        @Override
        protected void execute(NodeProbe probe)
        {
        }
    }

    @Command(name = "command-with-java-defaults")
    static class CommandWithJavaDefaults extends AbstractCommand
    {
        @Option(names = { "--flag", "-f" }, description = "A boolean flag")
        boolean flag = false;

        @Option(names = { "--name", "-n" }, description = "A string option", paramLabel = "name")
        String name = "default-name";

        @Option(names = { "--count", "-c" }, description = "An integer option", paramLabel = "count")
        int count = 0;

        @Option(names = { "--items" }, description = "A list option", paramLabel = "items")
        List<String> items = List.of();

        @Parameters(index = "0", paramLabel = "target", description = "A positional parameter", arity = "0..1")
        String target = null;

        @Override
        protected void execute(NodeProbe probe)
        {
        }
    }

    static class VerboseMixin
    {
        @Option(names = { "--verbose", "-v" }, description = "Enable verbose output")
        boolean verbose = false;
    }

    @Command(name = "mixin-command")
    static class CommandWithMixin extends AbstractCommand
    {
        @Mixin
        VerboseMixin verboseMixin = new VerboseMixin();

        @Option(names = { "--host" }, description = "Target host",
        paramLabel = "host", defaultValue = "localhost")
        String host = "localhost";

        @Override
        protected void execute(NodeProbe probe)
        {
        }
    }

    enum Color
    {
        red, green, blue
    }

    @Command(name = "collection-element-command")
    static class CollectionElementCommand extends AbstractCommand
    {
        @Option(names = { "--colorSet", "-s" }, paramLabel = "colorSet", description = "A set of enum values")
        public Set<Color> colorSet = Set.of();

        @Option(names = { "--colorArray", "-a" }, paramLabel = "colorArray", description = "An array of enum values")
        public Color[] colorArray = new Color[0];

        @Override
        protected void execute(NodeProbe probe)
        {
        }
    }
}