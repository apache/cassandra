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
package org.apache.cassandra.config;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.cassandra.distributed.upgrade.ConfigCompatibilityTestGenerate;
import org.yaml.snakeyaml.introspector.Property;

/**
 * To create the test files used by this class, run {@link ConfigCompatibilityTestGenerate}.
 */
public class ConfigCompatibilityTest
{
    private static final Logger logger = LoggerFactory.getLogger(ConfigCompatibilityTest.class);

    public static final String TEST_DIR = "test/data/config";

    // see CASSANDRA-11115
    private static final Set<String> THRIFT = ImmutableSet.of("rpc_server_type",
                                                              "rpc_port",
                                                              "rpc_listen_backlog",
                                                              "start_rpc",
                                                              "thrift_max_message_length_in_mb",
                                                              "rpc_max_threads",
                                                              "rpc_min_threads",
                                                              "rpc_recv_buff_size_in_bytes",
                                                              "rpc_send_buff_size_in_bytes",
                                                              "thrift_framed_transport_size_in_mb",
                                                              "thrift_prepared_statements_cache_size_mb",
                                                              "request_scheduler",
                                                              "request_scheduler_id",
                                                              "request_scheduler_options");
    // see // CASSANDRA-16956
    private static final Set<String> WINDOWS = ImmutableSet.of("windows_timer_interval");

    private static final Set<String> REMOVED_IN_40 = ImmutableSet.<String>builder()
                                                     .addAll(THRIFT)
                                                     .addAll(WINDOWS) // windows was removed later, but support was dropped in 4.0
                                                     .add("encryption_options") // CASSANDRA-10404
                                                     .add("index_interval") // CASSANDRA-10671
                                                     .add("streaming_socket_timeout_in_ms") // CASSANDRA-12229
                                                     .build();

    private static final Set<String> REMOVED_IN_50 = ImmutableSet.<String>builder()
                                                                 .add("commitlog_sync_batch_window_in_ms")
                                                                 .add("native_transport_max_negotiable_protocol_version")
                                                                 .add("concurrent_replicates")
                                                                 .add("commitlog_periodic_queue_size")
                                                                 .build();

    private static final Set<String> ALLOW_LIST = Sets.union(REMOVED_IN_40, REMOVED_IN_50);

    private static final Set<String> EXPECTED_FOR_50 = ImmutableSet.<String>builder()
                                                                   // Switched to a parameterized class that can construct from a bare string
                                                                   .add("internode_authenticator types do not match; org.apache.cassandra.config.ParameterizedClass != java.lang.String")
                                                                   .add("authenticator types do not match; org.apache.cassandra.config.ParameterizedClass != java.lang.String")
                                                                   .add("Property internode_authenticator used to be a value-type, but now is nested type class org.apache.cassandra.config.ParameterizedClass")
                                                                   .add("Property authenticator used to be a value-type, but now is nested type class org.apache.cassandra.config.ParameterizedClass")
                                                                   .build();

    /**
     * Not all converts make sense as backwards compatible as they use things like String to handle the conversion more
     * generically.
     */
    private static final Set<Converters> IGNORED_CONVERTERS = EnumSet.of(Converters.SECONDS_CUSTOM_DURATION);

    @Test
    public void diff_3_0() throws IOException
    {
        diff(TEST_DIR + "/version=3.0.0-alpha1.yml", ALLOW_LIST, EXPECTED_FOR_50);
    }

    @Test
    public void diff_3_11() throws IOException
    {
        diff(TEST_DIR + "/version=3.11.0.yml", ALLOW_LIST, EXPECTED_FOR_50);
    }

    @Test
    public void diff_4_0() throws IOException
    {
        diff(TEST_DIR + "/version=4.0-alpha1.yml", ImmutableSet.<String>builder()
                                                               .addAll(WINDOWS)
                                                               .addAll(ALLOW_LIST)
                                                               .build(), EXPECTED_FOR_50);
    }

    @Test
    public void diff_4_1() throws IOException
    {
        diff(TEST_DIR + "/version=4.1-alpha1.yml", ImmutableSet.<String>builder()
                                                               .addAll(WINDOWS)
                                                               .addAll(ALLOW_LIST)
                                                               .build(), EXPECTED_FOR_50);
    }

    @Test
    public void diff_5_0() throws IOException
    {
        diff(TEST_DIR + "/version=5.0-alpha1.yml", ImmutableSet.<String>builder()
                                                               .build(), ImmutableSet.of());
    }

    private void diff(String original, Set<String> ignore, Set<String> expectedErrors) throws IOException
    {
        Class<Config> type = Config.class;
        ClassTree previous = load(original);
        Loader loader = Properties.defaultLoader();
        Map<Class<?>, Map<String, Replacement>> replacements = Replacements.getNameReplacements(type);
        Set<String> missing = new HashSet<>();
        Set<String> errors = new HashSet<>();
        diff(loader, replacements, previous, type, "", missing, errors);
        missing = Sets.difference(missing, ignore);
        errors = Sets.difference(errors, expectedErrors);
        StringBuilder msg = new StringBuilder();
        if (!missing.isEmpty())
            msg.append(String.format("Unable to find the following properties:\n%s", String.join("\n", new TreeSet<>(missing))));
        if (!errors.isEmpty())
        {
            if (msg.length() > 0)
                msg.append('\n');
            msg.append(String.format("Errors detected:\n%s", String.join("\n", new TreeSet<>(errors))));
        }
        if (msg.length() > 0)
            throw new AssertionError(msg);
    }

    private void diff(Loader loader, Map<Class<?>, Map<String, Replacement>> replacements, ClassTree previous, Class<?> type, String prefix, Set<String> missing, Set<String> errors)
    {
        Map<String, Replacement> replaces = replacements.getOrDefault(type, Collections.emptyMap());
        Map<String, Property> properties = loader.getProperties(type);
        Sets.SetView<String> missingInCurrent = Sets.difference(previous.properties.keySet(), properties.keySet());
        Sets.SetView<String> inBoth = Sets.intersection(previous.properties.keySet(), properties.keySet());
        for (String name : missingInCurrent)
        {
            Replacement replacement = replaces.get(name);
            // can we find the property in @Replaces?
            if (replacement == null)
            {
                missing.add(prefix + name);
            }
            else
            {
                // do types match?
                Node node = previous.properties.get(name);
                if (node instanceof Leaf && replacement.oldType != null)
                    typeCheck(replacement.converter, toString(replacement.oldType), ((Leaf) node).type, name, errors);
            }
        }
        for (String name : inBoth)
        {
            Property prop = properties.get(name);
            Node node = previous.properties.get(name);
            // do types match?
            // if nested, look at sub-fields
            if (node instanceof ClassTree)
            {
                // current is nested type
                diff(loader, replacements, (ClassTree) node, prop.getType(), prefix + name + ".", missing, errors);
            }
            else
            {
                // current is flat type
                Replacement replacement = replaces.get(name);
                if (replacement != null && replacement.oldType != null)
                {
                    typeCheck(replacement.converter, toString(replacement.oldType), ((Leaf) node).type, name, errors);
                }
                else
                {
                    // previous is leaf, is current?
                    Map<String, Property> children = Properties.isPrimitive(prop) || Properties.isCollection(prop) ? Collections.emptyMap() : loader.getProperties(prop.getType());
                    if (!children.isEmpty())
                        errors.add(String.format("Property %s used to be a value-type, but now is nested type %s", name, prop.getType()));
                    typeCheck(null, toString(prop.getType()), ((Leaf) node).type, name, errors);
                }
            }
        }
    }

    private static void typeCheck(Converters converters, String lhs, String rhs, String name, Set<String> errors)
    {
        if (IGNORED_CONVERTERS.contains(converters))
            return;
        if (!lhs.equals(rhs))
            errors.add(String.format("%s types do not match; %s != %s%s", name, lhs, rhs, converters != null ? ", converter " + converters.name() : ""));
    }

    private static ClassTree load(String path) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory()); // checkstyle: permit this instantiation
        return mapper.readValue(new File(path), ClassTree.class);
    }

    public static void dump(ClassTree classTree, String path) throws IOException
    {
        logger.info("Dumping class to {}", path);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory()); // checkstyle: permit this instantiation
        mapper.writeValue(new File(path), classTree);

        // validate that load works as expected
        ClassTree loaded = load(path);
        assert loaded.equals(classTree);
    }

    public static ClassTree toTree(Class<?> klass)
    {
        ClassTree node = new ClassTree(klass);
        addProperties(Properties.defaultLoader(), node, klass);
        return node;
    }

    private static void addProperties(Loader loader, ClassTree node, Class<?> type)
    {
        SortedMap<String, Property> properties = new TreeMap<>(loader.getProperties(type));
        for (Map.Entry<String, Property> e : properties.entrySet())
        {
            Property property = e.getValue();
            Map<String, Property> subProperties = Properties.isPrimitive(property) || Properties.isCollection(property) ? Collections.emptyMap() : loader.getProperties(property.getType());
            Node child;
            if (subProperties.isEmpty())
            {
                child = new Leaf(toString(property.getType()));
            }
            else
            {
                ClassTree subTree = new ClassTree(property.getType());
                addProperties(loader, subTree, property.getType());
                child = subTree;
            }
            node.addProperty(e.getKey(), child);
        }
    }

    private static String toString(Class<?> type)
    {
        return normalize(type).getCanonicalName();
    }

    private static Class<?> normalize(Class<?> type)
    {
        // convert primitives to Number, allowing null in the doamin
        // this means that switching between int to Integer, and Integer to int are seen as the same while diffing; null
        // added/removed from domain is ignored by diff
        if (type.equals(Byte.TYPE))
            return Byte.class;
        else if (type.equals(Short.TYPE))
            return Short.class;
        else if (type.equals(Integer.TYPE))
            return Integer.class;
        else if (type.equals(Long.TYPE))
            return Long.class;
        else if (type.equals(Float.TYPE))
            return Float.class;
        else if (type.equals(Double.TYPE))
            return Double.class;
        else if (type.equals(Boolean.TYPE))
            return Boolean.class;
        else if (type.isArray())
            return List.class;
        return type;
    }

    @JsonSerialize(using = NodeSerializer.class)
    @JsonDeserialize(using = NodeDeserializer.class)
    private interface Node
    {

    }

    private static final class NodeSerializer extends StdSerializer<Node>
    {
        NodeSerializer()
        {
            super(Node.class);
        }

        @Override
        public void serialize(Node node, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException
        {
            if (node instanceof Leaf)
            {
                jsonGenerator.writeString(((Leaf) node).type);
            }
            else if (node instanceof ClassTree)
            {
                ClassTree classTree = (ClassTree) node;
                jsonGenerator.writeStartObject();
                for (Map.Entry<String, Node> e : classTree.properties.entrySet())
                    jsonGenerator.writeObjectField(e.getKey(), e.getValue());
                jsonGenerator.writeEndObject();
            }
        }
    }

    private static final class NodeDeserializer extends StdDeserializer<Node>
    {

        protected NodeDeserializer()
        {
            super(Node.class);
        }

        @Override
        public Node deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException
        {
            return toNode(jsonParser.getCodec().readTree(jsonParser));
        }

        private static Node toNode(TreeNode node)
        {
            if (node.isValueNode())
                return new Leaf(((TextNode) node).textValue());
            Map<String, Node> props = new HashMap<>();
            Iterator<String> it = node.fieldNames();
            while (it.hasNext())
            {
                String name = it.next();
                Node value = toNode(node.get(name));
                Node previous = props.put(name, value);
                if (previous != null)
                    throw new AssertionError("Duplicate properties found: " + name);
            }
            ClassTree classTree = new ClassTree();
            classTree.setProperties(props);
            return classTree;
        }
    }

    private static class ClassTree implements Node
    {
        private Class<?> type = null;
        private Map<String, Node> properties = new HashMap<>();

        ClassTree()
        {

        }

        ClassTree(Class<?> type)
        {
            this.type = type;
        }

        public Map<String, Node> getProperties()
        {
            return properties;
        }

        public void setProperties(Map<String, Node> properties)
        {
            this.properties = properties;
        }

        public void addProperty(String key, Node node)
        {
            Node previous = properties.put(key, node);
            if (previous != null)
                throw new AssertionError("Duplicate property name: " + key);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClassTree classTree = (ClassTree) o;
            return Objects.equals(properties, classTree.properties);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(properties);
        }

        @Override
        public String toString()
        {
            return "Klass{" +
                   "type=" + type +
                   ", properties=" + properties +
                   '}';
        }
    }

    private static class Leaf implements Node
    {
        private final String type;

        public Leaf(String type)
        {
            this.type = type;
        }

        @JsonValue
        public String getType()
        {
            return type;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Leaf leaf = (Leaf) o;
            return Objects.equals(type, leaf.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type);
        }

        @Override
        public String toString()
        {
            return type;
        }
    }
}