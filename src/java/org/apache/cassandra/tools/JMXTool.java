package org.apache.cassandra.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.airline.Arguments;
import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

public class JMXTool
{
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
        @Option(title = "url", name = { "-u", "--url" }, description = "JMX url to target")
        private String targetUrl = "service:jmx:rmi:///jndi/rmi://localhost:7199/jmxrmi";

        @Option(title = "format", name = { "-f", "--format" }, description = "What format to dump content as")
        private Format format = Format.console;

        public Void call() throws Exception
        {
            Map<String, Info> map = load(new JMXServiceURL(targetUrl));
            format.dump(map);
            return null;
        }

        public enum Format
        {
            console
            {
                void dump(Map<String, Info> map)
                {
                    for (Map.Entry<String, Info> e : map.entrySet())
                    {
                        String name = e.getKey();
                        Info info = e.getValue();

                        System.out.println(name);
                        System.out.println("\tAttributes");
                        Stream.of(info.attributes).forEach(a -> printRow(a.name, a.type, a.access));
                        System.out.println("\tOperations");
                        Stream.of(info.operations).forEach(o -> {
                            String args = Stream.of(o.parameters)
                                                .map(i -> i.name + ": " + i.type)
                                                .collect(Collectors.joining(",", "(", ")"));
                            printRow(o.name, o.returnType, args);
                        });
                    }
                }
            },
            json
            {
                void dump(Map<String, Info> map) throws IOException
                {
                    ObjectMapper mapper = new ObjectMapper();
                    System.out.println(mapper.writeValueAsString(map));
                }
            },
            yaml
            {
                void dump(Map<String, Info> map) throws IOException
                {
                    Representer representer = new Representer();
                    representer.addClassTag(Info.class, Tag.MAP); // avoid the auto added tag
                    Yaml yaml = new Yaml(representer);
                    System.out.println(yaml.dump(map));
                }
            };

            abstract void dump(Map<String, Info> map) throws IOException;
        }
    }

    @Command(name = "diff", description = "Diff two jmx dump files and report their differences")
    public static final class Diff implements Callable<Void>
    {
        @Arguments(title = "files", usage = "<left> <right>", description = "Files to diff")
        private List<File> files;

        @Option(title = "format", name = { "-f", "--format" }, description = "What format the files are in")
        private Format format = Format.yaml;

        @Option(title = "ignore left", name = { "--ignore-left" }, description = "Ignore results missing on the left")
        private boolean ignoreMissingLeft;

        @Option(title = "ignore right", name = { "--ignore-right" }, description = "Ignore results missing on the right")
        private boolean ignoreMissingRight;

        public Void call() throws Exception
        {
            if (files.size() != 2)
                throw new IllegalArgumentException("files requires 2 arguments but given " + files);
            Map<String, Info> left = format.load(files.get(0));
            Map<String, Info> right = format.load(files.get(1));

            DiffResult<String> keys = diff(left.keySet(), right.keySet());

            if (!ignoreMissingRight && !keys.notInRight.isEmpty())
            {
                System.out.println("Keys not in right:");
                printSet(0, keys.notInRight);
            }
            if (!ignoreMissingLeft && !keys.notInLeft.isEmpty())
            {
                System.out.println("Keys not in left: ");
                printSet(0, keys.notInLeft);
            }
            for (String key : keys.shared)
            {
                Info leftInfo = left.get(key);
                Info rightInfo = right.get(key);
                DiffResult<String> attributes = diff(leftInfo.attributeNames(), rightInfo.attributeNames());
                if (!ignoreMissingRight && !attributes.notInRight.isEmpty())
                {
                    System.out.println(key + "\tattribute not in right:");
                    printSet(1, attributes.notInRight);
                }
                if (!ignoreMissingLeft && !attributes.notInLeft.isEmpty())
                {
                    System.out.println(key + "\tattribute not in left:");
                    printSet(1, attributes.notInLeft);
                }
                //TODO check type

                DiffResult<String> operations = diff(leftInfo.operationNames(), rightInfo.operationNames());
                if (!ignoreMissingRight && !operations.notInRight.isEmpty())
                {
                    System.out.println(key + "\toperation not in right:");
                    printSet(1, operations.notInRight);
                }
                if (!ignoreMissingLeft && !operations.notInLeft.isEmpty())
                {
                    System.out.println(key + "\toperation not in left:");
                    printSet(1, operations.notInLeft);
                }
            }
            return null;
        }

        private static void printSet(int indent, Set<String> set)
        {
            StringBuilder sb = new StringBuilder();
            for (String s : set)
            {
                sb.setLength(0);
                for (int i = 0; i < indent; i++)
                    sb.append('\t');
                sb.append(s);
                System.out.println(sb);
            }
        }

        private static <T> DiffResult<T> diff(Set<T> left, Set<T> right)
        {
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
                Map<String, Info> load(File f) throws IOException
                {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.readValue(f, new TypeReference<Map<String, Info>>() {});
                }
            },
            yaml
            {
                Map<String, Info> load(File f) throws IOException
                {
                    Yaml yaml = new Yaml(new CustomConstructor());
                    try (FileInputStream input = new FileInputStream(f))
                    {
                        return (Map<String, Info>) yaml.load(input);
                    }
                }
            };

            abstract Map<String, Info> load(File f) throws IOException;
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
        JMXConnector jmxc = JMXConnectorFactory.connect(url, null);

        MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

        Map<String, Info> map = new TreeMap<>();
        for (String pkg : new TreeSet<>(Arrays.asList("org.apache.cassandra.metrics", "org.apache.cassandra.db")))
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

    private static void printRow(String... args)
    {
        ROW_BUFFER.setLength(0);
        ROW_BUFFER.append("\t\t");
        for (String a : args)
            ROW_BUFFER.append(a).append("\t");
        System.out.println(ROW_BUFFER);
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
    }

    public static final class Attribute
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
    }

    public static final class Operation
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

        public String getReturnType()
        {
            return returnType;
        }

        public void setReturnType(String returnType)
        {
            this.returnType = returnType;
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
    }

    public static void main(String[] args) throws Exception
    {
        Cli.CliBuilder<Callable<Void>> builder = Cli.builder("jmxtool");
        builder.withDefaultCommand(Dump.class);
        builder.withCommand(Dump.class);
        builder.withCommand(Diff.class);

        Cli<Callable<Void>> parser = builder.build();
        Callable<Void> command = parser.parse(args);
        command.call();
    }
}
