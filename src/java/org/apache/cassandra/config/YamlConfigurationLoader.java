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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.composer.Composer;
import org.yaml.snakeyaml.constructor.BaseConstructor;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.constructor.CustomClassLoaderConstructor;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.MissingProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeId;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Represent;
import org.yaml.snakeyaml.representer.Representer;

import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_DUPLICATE_CONFIG_KEYS;
import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_NEW_OLD_CONFIG_KEYS;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;
import static org.apache.cassandra.config.Replacements.getNameReplacements;

public class YamlConfigurationLoader implements ConfigurationLoader
{
    private static final Logger logger = LoggerFactory.getLogger(YamlConfigurationLoader.class);

    /**
     * This is related to {@link Config#PROPERTY_PREFIX} but is different to make sure Config properties updated via
     * system properties do not conflict with other system properties; the name "settings" matches system_views.settings.
     */
    static final String SYSTEM_PROPERTY_PREFIX = "cassandra.settings.";

    /**
     * Inspect the classpath to find storage configuration file
     */
    private static URL getStorageConfigURL() throws ConfigurationException
    {
        String configUrl = CASSANDRA_CONFIG.getString();

        URL url;
        try
        {
            url = new URL(configUrl);
            url.openStream().close(); // catches well-formed but bogus URLs
        }
        catch (Exception e)
        {
            ClassLoader loader = DatabaseDescriptor.class.getClassLoader();
            url = loader.getResource(configUrl);
            if (url == null)
            {
                String required = "file:" + File.pathSeparator() + File.pathSeparator();
                if (!configUrl.startsWith(required))
                    throw new ConfigurationException(String.format(
                        "Expecting URI in variable: [cassandra.config]. Found[%s]. Please prefix the file with [%s%s] for local " +
                        "files and [%s<server>%s] for remote files. If you are executing this from an external tool, it needs " +
                        "to set Config.setClientMode(true) to avoid loading configuration.",
                        configUrl, required, File.pathSeparator(), required, File.pathSeparator()));
                throw new ConfigurationException("Cannot locate " + configUrl + ".  If this is a local file, please confirm you've provided " + required + File.pathSeparator() + " as a URI prefix.");
            }
        }

        logger.info("Configuration location: {}", url);

        return url;
    }

    private static URL storageConfigURL;

    @Override
    public Config loadConfig() throws ConfigurationException
    {
        if (storageConfigURL == null)
            storageConfigURL = getStorageConfigURL();

        return loadConfig(storageConfigURL);
    }

    public Config loadConfig(URL url) throws ConfigurationException
    {
        return loadConfig(url, Config.class, Config::new);
    }

    @VisibleForTesting
    public <T> T loadConfig(URL url, Class<T> root, Supplier<T> factory) throws ConfigurationException
    {
        try
        {
            logger.debug("Loading settings from {}", url);
            byte[] configBytes;
            try (InputStream is = url.openStream())
            {
                configBytes = ByteStreams.toByteArray(is);
            }
            catch (IOException e)
            {
                // getStorageConfigURL should have ruled this out
                throw new AssertionError(e);
            }

            Map<Class<?>, Map<String, Replacement>> replacements = getNameReplacements(root);
            verifyReplacements(replacements, configBytes);
            PropertiesChecker propertiesChecker = new PropertiesChecker();
            Yaml yaml = YamlFactory.instance.newYamlInstance(new ConfigWithValidationConstructor(root, Yaml.class.getClassLoader()), propertiesChecker);
            T result = loadConfig(yaml, configBytes, root, factory);
            propertiesChecker.check();
            maybeAddSystemProperties(result);
            return result;
        }
        catch (YAMLException e)
        {
            throw new ConfigurationException("Invalid yaml: " + url, e);
        }
    }

    private static void maybeAddSystemProperties(Object obj)
    {
        if (CassandraRelevantProperties.CONFIG_ALLOW_SYSTEM_PROPERTIES.getBoolean())
        {
            java.util.Properties props = System.getProperties();
            Map<String, String> map = new HashMap<>();
            for (String name : props.stringPropertyNames())
            {
                if (name.startsWith(SYSTEM_PROPERTY_PREFIX))
                {
                    String value = props.getProperty(name);
                    if (value != null)
                        map.put(name.replace(SYSTEM_PROPERTY_PREFIX, ""), value);
                }
            }
            if (!map.isEmpty())
                updateFromMap(map, false, obj);
        }
    }

    private static void verifyReplacements(Map<Class<?>, Map<String, Replacement>> replacements, Map<String, ?> rawConfig)
    {
        List<String> duplicates = new ArrayList<>();
        for (Map.Entry<Class<?>, Map<String, Replacement>> outerEntry : replacements.entrySet())
        {
            for (Map.Entry<String, Replacement> entry : outerEntry.getValue().entrySet())
            {
                Replacement r = entry.getValue();
                if (!r.isValueFormatReplacement() && rawConfig.containsKey(r.oldName) && rawConfig.containsKey(r.newName))
                {
                    String msg = String.format("[%s -> %s]", r.oldName, r.newName);
                    duplicates.add(msg);
                }
            }
        }

        if (!duplicates.isEmpty())
        {
            String msg = String.format("Config contains both old and new keys for the same configuration parameters, migrate old -> new: %s", String.join(", ", duplicates));
            if (!ALLOW_NEW_OLD_CONFIG_KEYS.getBoolean())
                throw new ConfigurationException(msg);
            else
                logger.warn(msg);
        }
    }

    private static void verifyReplacements(Map<Class<?>, Map<String, Replacement>> replacements, byte[] configBytes)
    {
        LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setAllowDuplicateKeys(ALLOW_DUPLICATE_CONFIG_KEYS.getBoolean());
        Yaml rawYaml = new Yaml(loaderOptions);

        Map<String, Object> rawConfig = rawYaml.load(new ByteArrayInputStream(configBytes));
        if (rawConfig == null)
            rawConfig = new HashMap<>();
        verifyReplacements(replacements, rawConfig);

    }

    @VisibleForTesting
    public static <T> T fromMap(Map<String,Object> map, Class<T> klass)
    {
        return fromMap(map, true, klass);
    }

    @SuppressWarnings("unchecked") //getSingleData returns Object, not T
    public static <T> T fromMap(Map<String,Object> map, boolean shouldCheck, Class<T> klass)
    {
        SafeConstructor constructor = new ConfigWithValidationConstructor(klass, klass.getClassLoader());
        Map<Class<?>, Map<String, Replacement>> replacements = getNameReplacements(Config.class);
        verifyReplacements(replacements, map);
        YamlConfigurationLoader.PropertiesChecker propertiesChecker = new PropertiesChecker();
        Yaml yaml = YamlFactory.instance.newYamlInstance(constructor, propertiesChecker);
        Node node = yaml.represent(map);
        constructor.setComposer(new Composer(null, null)
        {
            @Override
            public Node getSingleNode()
            {
                return node;
            }
        });
        T value = (T) constructor.getSingleData(klass);
        if (shouldCheck)
            propertiesChecker.check();
        maybeAddSystemProperties(value);
        return value;
    }

    public static <T> T updateFromMap(Map<String, ?> map, boolean shouldCheck, T obj)
    {
        Class<T> klass = (Class<T>) obj.getClass();
        SafeConstructor constructor = new ConfigWithValidationConstructor(klass, klass.getClassLoader())
        {
            @Override
            protected Object newInstance(Node node)
            {
                if (node.getType() == obj.getClass())
                    return obj;
                return super.newInstance(node);
            }
        };
        Map<Class<?>, Map<String, Replacement>> replacements = getNameReplacements(Config.class);
        verifyReplacements(replacements, map);
        YamlConfigurationLoader.PropertiesChecker propertiesChecker = new PropertiesChecker();
        Yaml yaml = YamlFactory.instance.newYamlInstance(constructor, propertiesChecker);
        Node node = yaml.represent(map);
        constructor.setComposer(new Composer(null, null)
        {
            @Override
            public Node getSingleNode()
            {
                return node;
            }
        });
        T value = (T) constructor.getSingleData(klass);
        if (shouldCheck)
            propertiesChecker.check();
        return value;
    }

    /**
     * For the configuration properties that are not mentioned in the configuration yaml file, we need additionally
     * trigger the validation methods for the remaining properties to make sure the default values are valid and set.
     * These validation methods are configured with {@link ValidatedBy} for each property. This will create
     * a {@link MappingNode} with default values obtained from the {@link Config} class intance to finalize the
     * configuration instance creation.
     */
    @VisibleForTesting
    static class ConfigWithValidationConstructor extends CustomClassLoaderConstructor
    {
        private final Class<?> theRoot;

        public ConfigWithValidationConstructor(Class<?> theRoot, ClassLoader classLoader)
        {
            super(theRoot, classLoader);
            this.theRoot = theRoot;
            yamlClassConstructors.put(NodeId.mapping, new CassandraMappingConstructor());
        }

        private class CassandraMappingConstructor extends Constructor.ConstructMapping
        {
            @Override
            protected Object constructJavaBean2ndStep(MappingNode loadedYamlNode, Object object)
            {
                Object result = super.constructJavaBean2ndStep(loadedYamlNode, object);

                // If the node is not the root node, we don't need to handle the validation.
                if (!loadedYamlNode.getTag().equals(rootTag))
                    return result;

                assert theRoot.isInstance(result);
                Representer representer = YamlFactory.representer(new PropertyUtils()
                {
                    private final Loader loader = Properties.defaultLoader();

                    @Override
                    protected Map<String, Property> getPropertiesMap(Class<?> type, BeanAccess bAccess)
                    {
                        Map<String, Property> result = loader.getProperties(type);
                        // Filter out properties that are not validated by any method.
                        return result.entrySet()
                                     .stream()
                                     .filter(e -> {
                                         Property property = e.getValue();
                                         return property.getAnnotation(ValidatedBy.class) != null &&
                                                (property.getAnnotation(ValidatedByList.class) == null ||
                                                 property.getAnnotation(ValidatedByList.class).value().length == 0);
                                     })
                                     .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    }
                });

                // Load default values from the Config class instance for the properties that are not mentioned
                // in the configuration yaml file.
                MappingNode allWithDefauls = (MappingNode) new Yaml(representer).represent(object);
                allWithDefauls.setType(theRoot);
                removeIfLoadedFromYaml(loadedYamlNode, allWithDefauls);

                // This will trigger the validation and default values set for the remaining properties
                // that annotated with @ValidatedBy.
                return super.constructJavaBean2ndStep(allWithDefauls, result);
            }

            private void removeIfLoadedFromYaml(MappingNode loadedYamlNode, MappingNode all)
            {
                all.getValue().removeIf(nodeTuple -> {
                    Node valueNode = nodeTuple.getValueNode();
                    String key = mappingKeyHandleReplacements((ScalarNode) nodeTuple.getKeyNode(), all.getType());
                    Node mappingNodeOrig = mappingNodeByKey(loadedYamlNode, key);

                    if (valueNode instanceof MappingNode && mappingNodeOrig instanceof MappingNode)
                        removeIfLoadedFromYaml((MappingNode) mappingNodeOrig, (MappingNode) valueNode);

                    return mappingNodeOrig != null;
                });
            }

            private String mappingKeyHandleReplacements(ScalarNode keyNode, Class<?> rootType)
            {
                keyNode.setType(String.class);
                String key = (String) constructObject(keyNode);
                List<Replacement> replacements = Replacements.getReplacements(rootType);
                for (Replacement replacement : replacements)
                {
                    if (replacement.oldName.equals(key))
                        return replacement.newName;
                }
                return key;
            }

            private Node mappingNodeByKey(MappingNode node, String key)
            {
                return node.getValue().stream()
                           .filter(t -> mappingKeyHandleReplacements((ScalarNode) t.getKeyNode(), node.getType()).equals(key))
                           .findFirst()
                           .map(NodeTuple::getValueNode)
                           .orElse(null);
            }
        }

        @Override
        protected List<Object> createDefaultList(int initSize)
        {
            return Lists.newCopyOnWriteArrayList();
        }

        @Override
        protected Map<Object, Object> createDefaultMap(int initSize)
        {
            return Maps.newConcurrentMap();
        }

        @Override
        protected Set<Object> createDefaultSet(int initSize)
        {
            return Sets.newConcurrentHashSet();
        }
    }

    private static <T> T loadConfig(Yaml yaml, byte[] configBytes, Class<T> root, Supplier<T> factory)
    {
        T config = yaml.loadAs(new ByteArrayInputStream(configBytes), root);
        // If the configuration file is empty yaml will return null. In this case we should use the default
        // configuration to avoid hitting a NPE at a later stage.
        return config == null ? factory.get() : config;
    }

    /**
     * Utility class to check that there are no extra properties and that properties that are not null by default
     * are not set to null.
     */
    @VisibleForTesting
    private static class PropertiesChecker extends PropertyUtils
    {
        private final Loader loader = Properties.withReplacementsLoader(Properties.validatedPropertyLoader());
        private final Set<String> missingProperties = new HashSet<>();

        private final Set<String> deprecationWarnings = new HashSet<>();

        PropertiesChecker()
        {
            setSkipMissingProperties(true);
        }

        @Override
        public Property getProperty(Class<?> type, String name)
        {
            final Property result = getProperty0(type, name);

            if (result instanceof MissingProperty)
            {
                missingProperties.add(result.getName());
            }
            else if (result.getAnnotation(Deprecated.class) != null)
            {
                deprecationWarnings.add(result.getName());
            }
            else if (result instanceof Replacement.ReplacementProperty &&
                     ((Replacement.ReplacementProperty) result).replacement().deprecated)
                deprecationWarnings.add(result.getName());

            return new ForwardingProperty(result.getName(), result)
            {
                boolean allowsNull = result.getAnnotation(Nullable.class) != null;

                @Override
                public void set(Object object, Object value) throws Exception
                {
                    // TODO: CASSANDRA-17785, add @Nullable to all nullable Config properties and remove value == null
                    if (value == null && get(object) != null && !allowsNull)
                        throw new ConfigurationException("Invalid yaml. The property '" + result.getName() + "' can't be null", false);

                    result.set(object, value);
                }

                @Override
                public Object get(Object object)
                {
                    return result.get(object);
                }
            };
        }

        private Property getProperty0(Class<? extends Object> type, String name)
        {
            if (name.contains("."))
                return getNestedProperty(type, name);
            return getFlatProperty(type, name);
        }

        private Property getFlatProperty(Class<?> type, String name)
        {
            Property prop = loader.getProperties(type).get(name);
            return prop == null ? new MissingProperty(name) : prop;
        }

        private Property getNestedProperty(Class<?> type, String name)
        {
            Property root = null;
            for (String s : name.split("\\."))
            {
                Property prop = getFlatProperty(type, s);
                if (prop instanceof MissingProperty)
                {
                    root = null;
                    break;
                }
                root = root == null ? prop : Properties.andThen(root, prop);
                type = root.getType();
            }
            return root != null ? root : new MissingProperty(name);
        }

        public void check() throws ConfigurationException
        {
            if (!missingProperties.isEmpty())
                throw new ConfigurationException("Invalid yaml. Please remove properties " + missingProperties + " from your cassandra.yaml", false);

            if (!deprecationWarnings.isEmpty())
                logger.warn("{} parameters have been deprecated. They have new names and/or value format; For more information, please refer to NEWS.txt", deprecationWarnings);
        }
    }

    /**
     * Creates a YAML instance based on Cassandra's custom configuration classes and types. Yaml factory here is used
     * to {@link org.yaml.snakeyaml.Yaml#dumpAs} and {@link org.yaml.snakeyaml.Yaml#load(String)} given object or
     * string respectively, that in turn is used to serialize and deserialize configuration properties.
     */
    public static class YamlFactory
    {
        private static final List<TypeDescription> scalarCassandraTypes = loadScalarTypeDescriptions();
        private static final List<TypeDescription> javaBeanCassandraTypes = loadJavaBeanTypeDescriptions();
        public static final YamlFactory instance = new YamlFactory();

        private YamlFactory()
        {
        }

        /**
         * Creates a new Yaml instance with the given root class.
         * @param root The class (usually JavaBean) to be constructed.
         * @return A new Yaml instance with the given root class.
         */
        public Yaml newYamlInstance(Class<?> root)
        {
            PropertyUtils propertyUtils = new PropertyUtils();
            propertyUtils.setBeanAccess(BeanAccess.FIELD);
            propertyUtils.setAllowReadOnlyProperties(true);
            return newYamlInstance(new ConfigWithValidationConstructor(root,
                                                                       root.getClassLoader() == null ?
                                                                       Yaml.class.getClassLoader() :
                                                                       root.getClassLoader()),
                                   propertyUtils);
        }

        public Yaml newYamlInstance(BaseConstructor constructor, PropertyUtils propertyUtils)
        {
            return create(constructor, propertyUtils);
        }

        private static Yaml create(BaseConstructor constructor, PropertyUtils propertyUtils)
        {
            constructor.setPropertyUtils(propertyUtils);
            Yaml yaml = new Yaml(constructor, representer(propertyUtils));

            scalarCassandraTypes.forEach(yaml::addTypeDescription);
            javaBeanCassandraTypes.forEach(yaml::addTypeDescription);
            return yaml;
        }

        private static Representer representer(PropertyUtils propertyUtils)
        {
            DumperOptions options = new DumperOptions();
            options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            options.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
            options.setSplitLines(true);
            options.setPrettyFlow(true); // to remove brackets around
            Representer representer = new CassandraRepresenter(scalarCassandraTypes, options);
            representer.setPropertyUtils(propertyUtils);
            return representer;
        }

        private static List<TypeDescription> loadScalarTypeDescriptions()
        {
            // Enum types resolved with a custom overridden handler DefaultRepresentEnum() as a smiple string.
            return ImmutableList.<TypeDescription>builder()
                                .add(createTypeDescription(DataRateSpec.LongBytesPerSecondBound.class))
                                .add(createTypeDescription(DataStorageSpec.IntBytesBound.class))
                                .add(createTypeDescription(DataStorageSpec.IntKibibytesBound.class))
                                .add(createTypeDescription(DataStorageSpec.IntMebibytesBound.class))
                                .add(createTypeDescription(DataStorageSpec.LongBytesBound.class))
                                .add(createTypeDescription(DataStorageSpec.LongMebibytesBound.class))
                                .add(createTypeDescription(DurationSpec.IntMillisecondsBound.class))
                                .add(createTypeDescription(DurationSpec.IntMinutesBound.class))
                                .add(createTypeDescription(DurationSpec.IntSecondsBound.class))
                                .add(createTypeDescription(DurationSpec.LongMillisecondsBound.class))
                                .add(createTypeDescription(DurationSpec.LongNanosecondsBound.class))
                                .add(createTypeDescription(DurationSpec.LongSecondsBound.class))
                                .build();
        }

        private static List<TypeDescription> loadJavaBeanTypeDescriptions()
        {
            TypeDescription seedDesc = new TypeDescription(ParameterizedClass.class, Tag.MAP);
            seedDesc.addPropertyParameters("parameters", String.class, String.class);
            TypeDescription inheritingClass = new TypeDescription(InheritingClass.class, Tag.MAP);
            inheritingClass.addPropertyParameters("parameters", String.class, String.class);
            TypeDescription memtableDesc = new TypeDescription(Config.MemtableOptions.class, Tag.MAP);
            memtableDesc.addPropertyParameters("configurations", String.class, InheritingClass.class);
            return ImmutableList.<TypeDescription>builder()
                                .add(seedDesc, inheritingClass, memtableDesc)
                                .build();
        }

        private static TypeDescription createTypeDescription(Class<?> clazz)
        {
            return new TypeDescription(clazz, new Tag(clazz));
        }

        private static class CassandraRepresenter extends Representer
        {
            public CassandraRepresenter(List<TypeDescription> types, DumperOptions options)
            {
                super(options);
                multiRepresenters.put(Enum.class, new DefaultRepresentEnum());
                for (TypeDescription desc : types)
                    representers.computeIfAbsent(desc.getType(), clazz -> o -> representScalar(Tag.STR, o.toString()));
            }

            @Override
            protected Node representMapping(Tag tag, Map<?, ?> mapping, DumperOptions.FlowStyle flowStyle)
            {
                // Tag.MAP is used to represent with a simple map, without the class name in front.
                return super.representMapping(Tag.MAP, mapping, DumperOptions.FlowStyle.BLOCK);
            }

            /**
             * Represents an enum as a simple string by its key. This is useful for backward compatibility,
             * but if you want to use custom enum representations, you should add them to the {@link #representers}
             * map as scalar representations.
             */
            protected class DefaultRepresentEnum implements Represent
            {
                public Node representData(Object data) {
                    return representScalar(Tag.STR, ((Enum<?>) data).name());
                }
            }
        }
    }
}

