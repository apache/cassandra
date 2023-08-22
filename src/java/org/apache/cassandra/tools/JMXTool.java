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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import com.fasterxml.jackson.core.type.TypeReference;
import io.airlift.airline.Arguments;
import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.utils.JsonUtils;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

public class JMXTool
{
    private static final List<String> METRIC_PACKAGES = Arrays.asList("org.apache.cassandra.metrics",
                                                                      "org.apache.cassandra.db",
                                                                      "org.apache.cassandra.hints",
                                                                      "org.apache.cassandra.internal",
                                                                      "org.apache.cassandra.net",
                                                                      "org.apache.cassandra.request",
                                                                      "org.apache.cassandra.service");

    private static final Comparator<MBeanOperationInfo> OPERATOR_COMPARATOR = (a, b) -> {
        int rc = a.getName().compareTo(b.getName());
        if (rc != 0)
            return rc;
        String[] aSig = Stream.of(a.getSignature()).map(MBeanParameterInfo::getName).toArray(String[]::new);
        String[] bSig = Stream.of(b.getSignature()).map(MBeanParameterInfo::getName).toArray(String[]::new);
        rc = Integer.compare(aSig.length, bSig.length);
        if (rc != 0)
            return rc;
        for (int i = 0; i < aSig.length; i++)
        {
            rc = aSig[i].compareTo(bSig[i]);
            if (rc != 0)
                return rc;
        }
        return rc;
    };

    @Command(name = "dump", description = "Dump the Apache Cassandra JMX objects and metadata.")
    public static final class Dump implements Callable<Void>
    {
        @Inject
        private HelpOption helpOption;

        @Option(title = "url", name = { "-u", "--url" }, description = "JMX url to target")
        private String targetUrl = "service:jmx:rmi:///jndi/rmi://localhost:7199/jmxrmi";

        @Option(title = "format", name = { "-f", "--format" }, description = "What format to dump content as; supported values are console (default), json, and yaml")
        private Format format = Format.console;

        public Void call() throws Exception
        {
            Map<String, Info> map = load(new JMXServiceURL(targetUrl));
            format.dump(System.out, map);
            return null;
        }

        public enum Format
        {
            console
            {
                void dump(OutputStream output, Map<String, Info> map)
                {
                    // output should be released by caller
                    PrintStream out = toPrintStream(output);
                    for (Map.Entry<String, Info> e : map.entrySet())
                    {
                        String name = e.getKey();
                        Info info = e.getValue();

                        out.println(name);
                        out.println("\tAttributes");
                        Stream.of(info.attributes).forEach(a -> printRow(out, a.name, a.type, a.access));
                        out.println("\tOperations");
                        Stream.of(info.operations).forEach(o -> {
                            String args = Stream.of(o.parameters)
                                                .map(i -> i.name + ": " + i.type)
                                                .collect(Collectors.joining(",", "(", ")"));
                            printRow(out, o.name, o.returnType, args);
                        });
                    }
                }
            },
            json
            {
                void dump(OutputStream output, Map<String, Info> map) throws IOException
                {
                    JsonUtils.JSON_OBJECT_PRETTY_WRITER.writeValue(output, map);
                }
            },
            yaml
            {
                void dump(OutputStream output, Map<String, Info> map) throws IOException
                {
                    Representer representer = new Representer();
                    representer.addClassTag(Info.class, Tag.MAP); // avoid the auto added tag
                    Yaml yaml = new Yaml(representer);
                    yaml.dump(map, new OutputStreamWriter(output));
                }
            };

            private static PrintStream toPrintStream(OutputStream output)
            {
                try
                {
                    return output instanceof PrintStream ? (PrintStream) output : new PrintStream(output, true, "UTF-8");
                }
                catch (UnsupportedEncodingException e)
                {
                    throw new AssertionError(e); // utf-8 is a required charset for the JVM
                }
            }

            abstract void dump(OutputStream output, Map<String, Info> map) throws IOException;
        }
    }

    @Command(name = "diff", description = "Diff two jmx dump files and report their differences")
    public static final class Diff implements Callable<Void>
    {
        @Inject
        private HelpOption helpOption;

        @Arguments(title = "files", usage = "<left> <right>", description = "Files to diff")
        private List<File> files;

        @Option(title = "format", name = { "-f", "--format" }, description = "What format the files are in; only support json and yaml as format")
        private Format format = Format.yaml;

        @Option(title = "ignore left", name = { "--ignore-missing-on-left" }, description = "Ignore results missing on the left")
        private boolean ignoreMissingLeft;

        @Option(title = "ignore right", name = { "--ignore-missing-on-right" }, description = "Ignore results missing on the right")
        private boolean ignoreMissingRight;

        @Option(title = "exclude objects", name = "--exclude-object", description
                                                                      = "Ignores processing specific objects. " +
                                                                        "Each usage should take a single object, " +
                                                                        "but can use this flag multiple times.")
        private List<CliPattern> excludeObjects = new ArrayList<>();

        @Option(title = "exclude attributes", name = "--exclude-attribute", description
                                                                            = "Ignores processing specific attributes. " +
                                                                              "Each usage should take a single attribute, " +
                                                                              "but can use this flag multiple times.")
        private List<CliPattern> excludeAttributes = new ArrayList<>();

        @Option(title = "exclude operations", name = "--exclude-operation", description
                                                                            = "Ignores processing specific operations. " +
                                                                              "Each usage should take a single operation, " +
                                                                              "but can use this flag multiple times.")
        private List<CliPattern> excludeOperations = new ArrayList<>();

        public Void call() throws Exception
        {
            Preconditions.checkArgument(files.size() == 2, "files requires 2 arguments but given %s", files);
            Map<String, Info> left;
            Map<String, Info> right;
            try (FileInputStreamPlus leftStream = new FileInputStreamPlus(files.get(0));
                 FileInputStreamPlus rightStream = new FileInputStreamPlus(files.get(1)))
            {
                left = format.load(leftStream);
                right = format.load(rightStream);
            }

            diff(left, right);
            return null;
        }

        private void diff(Map<String, Info> left, Map<String, Info> right)
        {
            DiffResult<String> objectNames = diff(left.keySet(), right.keySet(), name -> {
                for (CliPattern p : excludeObjects)
                {
                    if (p.pattern.matcher(name).matches())
                        return false;
                }
                return true;
            });

            if (!ignoreMissingRight && !objectNames.notInRight.isEmpty())
            {
                System.out.println("Objects not in right:");
                printSet(0, objectNames.notInRight);
            }
            if (!ignoreMissingLeft && !objectNames.notInLeft.isEmpty())
            {
                System.out.println("Objects not in left: ");
                printSet(0, objectNames.notInLeft);
            }
            Runnable printHeader = new Runnable()
            {
                boolean printedHeader = false;

                public void run()
                {
                    if (!printedHeader)
                    {
                        System.out.println("Difference found in attribute or operation");
                        printedHeader = true;
                    }
                }
            };

            for (String key : objectNames.shared)
            {
                Info leftInfo = left.get(key);
                Info rightInfo = right.get(key);
                DiffResult<Attribute> attributes = diff(leftInfo.attributeSet(), rightInfo.attributeSet(), attribute -> {
                    for (CliPattern p : excludeAttributes)
                    {
                        if (p.pattern.matcher(attribute.name).matches())
                            return false;
                    }
                    return true;
                });
                if (!ignoreMissingRight && !attributes.notInRight.isEmpty())
                {
                    printHeader.run();
                    System.out.println(key + "\tattribute not in right:");
                    printSet(1, attributes.notInRight);
                }
                if (!ignoreMissingLeft && !attributes.notInLeft.isEmpty())
                {
                    printHeader.run();
                    System.out.println(key + "\tattribute not in left:");
                    printSet(1, attributes.notInLeft);
                }

                DiffResult<Operation> operations = diff(leftInfo.operationSet(), rightInfo.operationSet(), operation -> {
                    for (CliPattern p : excludeOperations)
                    {
                        if (p.pattern.matcher(operation.name).matches() ||
                            p.pattern.matcher(operation.toString().replaceAll(" +", "")).matches())
                            return false;
                    }
                    return true;
                });
                if (!ignoreMissingRight && !operations.notInRight.isEmpty())
                {
                    printHeader.run();
                    System.out.println(key + "\toperation not in right:");
                    printSet(1, operations.notInRight, (sb, o) ->
                                                       rightInfo.getOperation(o.name).ifPresent(match ->
                                                                                                sb.append("\t").append("similar in right: ").append(match)));
                }
                if (!ignoreMissingLeft && !operations.notInLeft.isEmpty())
                {
                    printHeader.run();
                    System.out.println(key + "\toperation not in left:");
                    printSet(1, operations.notInLeft, (sb, o) ->
                                                      leftInfo.getOperation(o.name).ifPresent(match ->
                                                                                              sb.append("\t").append("similar in left: ").append(match)));
                }
            }
        }

        private static <T extends Comparable<T>> void printSet(int indent, Set<T> set)
        {
            printSet(indent, set, (i1, i2) -> {});
        }

        private static <T extends Comparable<T>> void printSet(int indent, Set<T> set, BiConsumer<StringBuilder, T> fn)
        {
            StringBuilder sb = new StringBuilder();
            for (T t : new TreeSet<>(set))
            {
                sb.setLength(0);
                for (int i = 0; i < indent; i++)
                    sb.append('\t');
                sb.append(t);
                fn.accept(sb, t);
                System.out.println(sb);
            }
        }

        private static <T> DiffResult<T> diff(Set<T> left, Set<T> right, Predicate<T> fn)
        {
            left = Sets.filter(left, fn);
            right = Sets.filter(right, fn);
            return new DiffResult<>(Sets.difference(left, right), Sets.difference(right, left), Sets.intersection(left, right));
        }

        private static final class DiffResult<T>
        {
            private final SetView<T> notInRight;
            private final SetView<T> notInLeft;
            private final SetView<T> shared;

            private DiffResult(SetView<T> notInRight, SetView<T> notInLeft, SetView<T> shared)
            {
                this.notInRight = notInRight;
                this.notInLeft = notInLeft;
                this.shared = shared;
            }
        }

        public enum Format
        {
            json
            {
                Map<String, Info> load(InputStream input) throws IOException
                {
                    return JsonUtils.JSON_OBJECT_MAPPER.readValue(input, new TypeReference<Map<String, Info>>() {});
                }
            },
            yaml
            {
                Map<String, Info> load(InputStream input) throws IOException
                {
                    Yaml yaml = new Yaml(new CustomConstructor());
                    return (Map<String, Info>) yaml.load(input);
                }
            };

            abstract Map<String, Info> load(InputStream input) throws IOException;
        }

        private static final class CustomConstructor extends Constructor
        {
            private static final String ROOT = "__root__";
            private static final TypeDescription INFO_TYPE = new TypeDescription(Info.class);

            public CustomConstructor()
            {
                this.rootTag = new Tag(ROOT);
                this.addTypeDescription(INFO_TYPE);
            }

            protected Object constructObject(Node node)
            {
                if (ROOT.equals(node.getTag().getValue()) && node instanceof MappingNode)
                {
                    MappingNode mn = (MappingNode) node;
                    return mn.getValue().stream()
                                .collect(Collectors.toMap(t -> super.constructObject(t.getKeyNode()),
                                                          t -> {
                                                              Node child = t.getValueNode();
                                                              child.setType(INFO_TYPE.getType());
                                                              return super.constructObject(child);
                                                          }));
                }
                else
                {
                    return super.constructObject(node);
                }
            }
        }
    }

    private static Map<String, Info> load(JMXServiceURL url) throws IOException, MalformedObjectNameException, IntrospectionException, InstanceNotFoundException, ReflectionException
    {
        try (JMXConnector jmxc = JMXConnectorFactory.connect(url, null))
        {
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            Map<String, Info> map = new TreeMap<>();
            for (String pkg : new TreeSet<>(METRIC_PACKAGES))
            {
                Set<ObjectName> metricNames = new TreeSet<>(mbsc.queryNames(new ObjectName(pkg + ":*"), null));
                for (ObjectName name : metricNames)
                {
                    if (mbsc.isRegistered(name))
                    {
                        MBeanInfo info = mbsc.getMBeanInfo(name);
                        map.put(name.toString(), Info.from(info));
                    }
                }
            }
            return map;
        }
    }

    private static String getAccess(MBeanAttributeInfo a)
    {
        String access;
        if (a.isReadable())
        {
            if (a.isWritable())
                access = "read/write";
            else
                access = "read-only";
        }
        else if (a.isWritable())
            access = "write-only";
        else
            access = "no-access";
        return access;
    }

    private static String normalizeType(String type)
    {
        switch (type)
        {
            case "[Z":
                return "boolean[]";
            case "[B":
                return "byte[]";
            case "[S":
                return "short[]";
            case "[I":
                return "int[]";
            case "[J":
                return "long[]";
            case "[F":
                return "float[]";
            case "[D":
                return "double[]";
            case "[C":
                return "char[]";
        }
        if (type.startsWith("[L"))
            return type.substring(2, type.length() - 1) + "[]"; // -1 will remove the ; at the end
        return type;
    }

    private static final StringBuilder ROW_BUFFER = new StringBuilder();

    private static void printRow(PrintStream out, String... args)
    {
        ROW_BUFFER.setLength(0);
        ROW_BUFFER.append("\t\t");
        for (String a : args)
            ROW_BUFFER.append(a).append("\t");
        out.println(ROW_BUFFER);
    }

    public static final class Info
    {
        private Attribute[] attributes;
        private Operation[] operations;

        public Info()
        {
        }

        public Info(Attribute[] attributes, Operation[] operations)
        {
            this.attributes = attributes;
            this.operations = operations;
        }

        private static Info from(MBeanInfo info)
        {
            Attribute[] attributes = Stream.of(info.getAttributes())
                                           .sorted(Comparator.comparing(MBeanFeatureInfo::getName))
                                           .map(Attribute::from)
                                           .toArray(Attribute[]::new);

            Operation[] operations = Stream.of(info.getOperations())
                                           .sorted(OPERATOR_COMPARATOR)
                                           .map(Operation::from)
                                           .toArray(Operation[]::new);
            return new Info(attributes, operations);
        }

        public Attribute[] getAttributes()
        {
            return attributes;
        }

        public void setAttributes(Attribute[] attributes)
        {
            this.attributes = attributes;
        }

        public Set<String> attributeNames()
        {
            return Stream.of(attributes).map(a -> a.name).collect(Collectors.toSet());
        }

        public Set<Attribute> attributeSet()
        {
            return new HashSet<>(Arrays.asList(attributes));
        }

        public Operation[] getOperations()
        {
            return operations;
        }

        public void setOperations(Operation[] operations)
        {
            this.operations = operations;
        }

        public Set<String> operationNames()
        {
            return Stream.of(operations).map(o -> o.name).collect(Collectors.toSet());
        }

        public Set<Operation> operationSet()
        {
            return new HashSet<>(Arrays.asList(operations));
        }

        public Optional<Attribute> getAttribute(String name)
        {
            return Stream.of(attributes).filter(a -> a.name.equals(name)).findFirst();
        }

        public Attribute getAttributePresent(String name)
        {
            return getAttribute(name).orElseThrow(AssertionError::new);
        }

        public Optional<Operation> getOperation(String name)
        {
            return Stream.of(operations).filter(o -> o.name.equals(name)).findFirst();
        }

        public Operation getOperationPresent(String name)
        {
            return getOperation(name).orElseThrow(AssertionError::new);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Info info = (Info) o;
            return Arrays.equals(attributes, info.attributes) &&
                   Arrays.equals(operations, info.operations);
        }

        @Override
        public int hashCode()
        {
            int result = Arrays.hashCode(attributes);
            result = 31 * result + Arrays.hashCode(operations);
            return result;
        }
    }

    public static final class Attribute implements Comparable<Attribute>
    {
        private String name;
        private String type;
        private String access;

        public Attribute()
        {
        }

        public Attribute(String name, String type, String access)
        {
            this.name = name;
            this.type = type;
            this.access = access;
        }

        private static Attribute from(MBeanAttributeInfo info)
        {
            return new Attribute(info.getName(), normalizeType(info.getType()), JMXTool.getAccess(info));
        }

        public String getName()
        {
            return name;
        }

        public void setName(String name)
        {
            this.name = name;
        }

        public String getType()
        {
            return type;
        }

        public void setType(String type)
        {
            this.type = type;
        }

        public String getAccess()
        {
            return access;
        }

        public void setAccess(String access)
        {
            this.access = access;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Attribute attribute = (Attribute) o;
            return Objects.equals(name, attribute.name) &&
                   Objects.equals(type, attribute.type);
        }

        public int hashCode()
        {
            return Objects.hash(name, type);
        }

        public String toString()
        {
            return name + ": " + type;
        }

        public int compareTo(Attribute o)
        {
            int rc = name.compareTo(o.name);
            if (rc != 0)
                return rc;
            return type.compareTo(o.type);
        }
    }

    public static final class Operation implements Comparable<Operation>
    {
        private String name;
        private Parameter[] parameters;
        private String returnType;

        public Operation()
        {
        }

        public Operation(String name, Parameter[] parameters, String returnType)
        {
            this.name = name;
            this.parameters = parameters;
            this.returnType = returnType;
        }

        private static Operation from(MBeanOperationInfo info)
        {
            Parameter[] params = Stream.of(info.getSignature()).map(Parameter::from).toArray(Parameter[]::new);
            return new Operation(info.getName(), params, normalizeType(info.getReturnType()));
        }

        public String getName()
        {
            return name;
        }

        public void setName(String name)
        {
            this.name = name;
        }

        public Parameter[] getParameters()
        {
            return parameters;
        }

        public void setParameters(Parameter[] parameters)
        {
            this.parameters = parameters;
        }

        public List<String> parameterTypes()
        {
            return Stream.of(parameters).map(p -> p.type).collect(Collectors.toList());
        }

        public String getReturnType()
        {
            return returnType;
        }

        public void setReturnType(String returnType)
        {
            this.returnType = returnType;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Operation operation = (Operation) o;
            return Objects.equals(name, operation.name) &&
                   Arrays.equals(parameters, operation.parameters) &&
                   Objects.equals(returnType, operation.returnType);
        }

        public int hashCode()
        {
            int result = Objects.hash(name, returnType);
            result = 31 * result + Arrays.hashCode(parameters);
            return result;
        }

        public String toString()
        {
            return name + Stream.of(parameters).map(Parameter::toString).collect(Collectors.joining(", ", "(", ")")) + ": " + returnType;
        }

        public int compareTo(Operation o)
        {
            int rc = name.compareTo(o.name);
            if (rc != 0)
                return rc;
            rc = Integer.compare(parameters.length, o.parameters.length);
            if (rc != 0)
                return rc;
            for (int i = 0; i < parameters.length; i++)
            {
                rc = parameters[i].type.compareTo(o.parameters[i].type);
                if (rc != 0)
                    return rc;
            }
            return returnType.compareTo(o.returnType);
        }
    }

    public static final class Parameter
    {
        private String name;
        private String type;

        public Parameter()
        {
        }

        public Parameter(String name, String type)
        {
            this.name = name;
            this.type = type;
        }

        private static Parameter from(MBeanParameterInfo info)
        {
            return new Parameter(info.getName(), normalizeType(info.getType()));
        }

        public String getName()
        {
            return name;
        }

        public void setName(String name)
        {
            this.name = name;
        }

        public String getType()
        {
            return type;
        }

        public void setType(String type)
        {
            this.type = type;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Parameter parameter = (Parameter) o;
            return Objects.equals(type, parameter.type);
        }

        public int hashCode()
        {
            return Objects.hash(type);
        }

        public String toString()
        {
            return name + ": " + type;
        }
    }

    public static final class CliPattern
    {
        private final Pattern pattern;

        public CliPattern(String pattern)
        {
            this.pattern = Pattern.compile(pattern);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Cli.CliBuilder<Callable<Void>> builder = Cli.builder("jmxtool");
        builder.withDefaultCommand(Help.class);
        builder.withCommands(Help.class, Dump.class, Diff.class);

        Cli<Callable<Void>> parser = builder.build();
        Callable<Void> command = parser.parse(args);
        command.call();
    }
}
