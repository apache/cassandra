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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.composer.Composer;
import org.yaml.snakeyaml.constructor.CustomClassLoaderConstructor;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.MissingProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import org.yaml.snakeyaml.nodes.Node;

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

            SafeConstructor constructor = new CustomConstructor(Config.class, Yaml.class.getClassLoader());
            Map<Class<?>, Map<String, Replacement>> replacements = getNameReplacements(Config.class);
            verifyReplacements(replacements, configBytes);
            PropertiesChecker propertiesChecker = new PropertiesChecker(replacements);
            constructor.setPropertyUtils(propertiesChecker);
            Yaml yaml = new Yaml(constructor);
            Config result = loadConfig(yaml, configBytes);
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
        SafeConstructor constructor = new YamlConfigurationLoader.CustomConstructor(klass, klass.getClassLoader());
        Map<Class<?>, Map<String, Replacement>> replacements = getNameReplacements(Config.class);
        verifyReplacements(replacements, map);
        YamlConfigurationLoader.PropertiesChecker propertiesChecker = new YamlConfigurationLoader.PropertiesChecker(replacements);
        constructor.setPropertyUtils(propertiesChecker);
        Yaml yaml = new Yaml(constructor);
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
        SafeConstructor constructor = new YamlConfigurationLoader.CustomConstructor(klass, klass.getClassLoader())
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
        YamlConfigurationLoader.PropertiesChecker propertiesChecker = new YamlConfigurationLoader.PropertiesChecker(replacements);
        constructor.setPropertyUtils(propertiesChecker);
        Yaml yaml = new Yaml(constructor);
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

    @VisibleForTesting
    static class CustomConstructor extends CustomClassLoaderConstructor
    {
        CustomConstructor(Class<?> theRoot, ClassLoader classLoader)
        {
            super(theRoot, classLoader);

            TypeDescription seedDesc = new TypeDescription(ParameterizedClass.class);
            seedDesc.putMapPropertyType("parameters", String.class, String.class);
            addTypeDescription(seedDesc);

            TypeDescription memtableDesc = new TypeDescription(Config.MemtableOptions.class);
            memtableDesc.addPropertyParameters("configurations", String.class, InheritingClass.class);
            addTypeDescription(memtableDesc);
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

    private static Config loadConfig(Yaml yaml, byte[] configBytes)
    {
        Config config = yaml.loadAs(new ByteArrayInputStream(configBytes), Config.class);
        // If the configuration file is empty yaml will return null. In this case we should use the default
        // configuration to avoid hitting a NPE at a later stage.
        return config == null ? new Config() : config;
    }

    /**
     * Utility class to check that there are no extra properties and that properties that are not null by default
     * are not set to null.
     */
    @VisibleForTesting
    private static class PropertiesChecker extends PropertyUtils
    {
        private final Loader loader = Properties.defaultLoader();
        private final Set<String> missingProperties = new HashSet<>();

        private final Set<String> nullProperties = new HashSet<>();

        private final Set<String> deprecationWarnings = new HashSet<>();

        private final Map<Class<?>, Map<String, Replacement>> replacements;

        PropertiesChecker(Map<Class<?>, Map<String, Replacement>> replacements)
        {
            this.replacements = Objects.requireNonNull(replacements, "Replacements should not be null");
            setSkipMissingProperties(true);
        }

        @Override
        public Property getProperty(Class<?> type, String name)
        {
            final Property result;
            Map<String, Replacement> typeReplacements = replacements.getOrDefault(type, Collections.emptyMap());
            if (typeReplacements.containsKey(name))
            {
                Replacement replacement = typeReplacements.get(name);
                result = replacement.toProperty(getProperty0(type, replacement.newName));
                
                if (replacement.deprecated)
                    deprecationWarnings.add(replacement.oldName);
            }
            else
            {
                result = getProperty0(type, name);
            }

            if (result instanceof MissingProperty)
            {
                missingProperties.add(result.getName());
            }
            else if (result.getAnnotation(Deprecated.class) != null)
            {
                deprecationWarnings.add(result.getName());
            }

            return new ForwardingProperty(result.getName(), result)
            {
                boolean allowsNull = result.getAnnotation(Nullable.class) != null;

                @Override
                public void set(Object object, Object value) throws Exception
                {
                    // TODO: CASSANDRA-17785, add @Nullable to all nullable Config properties and remove value == null
                    if (value == null && get(object) != null && !allowsNull)
                        nullProperties.add(getName());

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
            if (!nullProperties.isEmpty())
                throw new ConfigurationException("Invalid yaml. Those properties " + nullProperties + " are not valid", false);

            if (!missingProperties.isEmpty())
                throw new ConfigurationException("Invalid yaml. Please remove properties " + missingProperties + " from your cassandra.yaml", false);

            if (!deprecationWarnings.isEmpty())
                logger.warn("{} parameters have been deprecated. They have new names and/or value format; For more information, please refer to NEWS.txt", deprecationWarnings);
        }
    }
}

