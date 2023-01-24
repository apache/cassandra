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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

import org.apache.commons.lang3.SystemUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.composer.Composer;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.constructor.CustomClassLoaderConstructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.MissingProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import org.yaml.snakeyaml.nodes.Node;

public class YamlConfigurationLoader implements ConfigurationLoader
{
    private static final Logger logger = LoggerFactory.getLogger(YamlConfigurationLoader.class);

    private final static String DEFAULT_CONFIGURATION = "cassandra.yaml";

    /**
     * Inspect the classpath to find storage configuration file
     */
    private static URL getStorageConfigURL() throws ConfigurationException
    {
        String configUrl = System.getProperty("cassandra.config");
        if (configUrl == null)
            configUrl = DEFAULT_CONFIGURATION;

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
                String required = "file:" + File.separator + File.separator;
                if (!configUrl.startsWith(required))
                    throw new ConfigurationException(String.format(
                        "Expecting URI in variable: [cassandra.config]. Found[%s]. Please prefix the file with [%s%s] for local " +
                        "files and [%s<server>%s] for remote files. If you are executing this from an external tool, it needs " +
                        "to set Config.setClientMode(true) to avoid loading configuration.",
                        configUrl, required, File.separator, required, File.separator));
                throw new ConfigurationException("Cannot locate " + configUrl + ".  If this is a local file, please confirm you've provided " + required + File.separator + " as a URI prefix.");
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
            PropertiesChecker propertiesChecker = new PropertiesChecker(replacements);
            constructor.setPropertyUtils(propertiesChecker);
            Yaml yaml = new Yaml(constructor);
            Config result = loadConfig(yaml, configBytes);
            propertiesChecker.check();
            return result;
        }
        catch (YAMLException e)
        {
            throw new ConfigurationException("Invalid yaml: " + url + SystemUtils.LINE_SEPARATOR
                                             +  " Error: " + e.getMessage(), false);
        }
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

    static class CustomConstructor extends CustomClassLoaderConstructor
    {
        CustomConstructor(Class<?> theRoot, ClassLoader classLoader)
        {
            super(theRoot, classLoader);

            TypeDescription seedDesc = new TypeDescription(ParameterizedClass.class);
            seedDesc.putMapPropertyType("parameters", String.class, String.class);
            addTypeDescription(seedDesc);
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
    private static class PropertiesChecker extends PropertyUtils
    {
        private final Set<String> missingProperties = new HashSet<>();

        private final Set<String> nullProperties = new HashSet<>();

        private final Set<String> deprecationWarnings = new HashSet<>();

        private final Map<Class<?>, Map<String, Replacement>> replacements;

        public PropertiesChecker(Map<Class<?>, Map<String, Replacement>> replacements)
        {
            this.replacements = Objects.requireNonNull(replacements, "Replacements should not be null");
            setSkipMissingProperties(true);
        }

        @Override
        public Property getProperty(Class<? extends Object> type, String name)
        {
            final Property result;
            Map<String, Replacement> typeReplacements = replacements.getOrDefault(type, Collections.emptyMap());
            if (typeReplacements.containsKey(name))
            {
                Replacement replacement = typeReplacements.get(name);
                final Property newProperty = super.getProperty(type, replacement.newName);
                result = new Property(replacement.oldName, newProperty.getType())
                {
                    @Override
                    public Class<?>[] getActualTypeArguments()
                    {
                        return newProperty.getActualTypeArguments();
                    }

                    @Override
                    public void set(Object o, Object o1) throws Exception
                    {
                        newProperty.set(o, o1);
                    }

                    @Override
                    public Object get(Object o)
                    {
                        return newProperty.get(o);
                    }

                    @Override
                    public List<Annotation> getAnnotations()
                    {
                        return null;
                    }

                    @Override
                    public <A extends Annotation> A getAnnotation(Class<A> aClass)
                    {
                        return null;
                    }
                };

                if (replacement.deprecated)
                    deprecationWarnings.add(replacement.oldName);
            }
            else
            {
                result = super.getProperty(type, name);
            }

            if (result instanceof MissingProperty)
            {
                missingProperties.add(result.getName());
            }

            return new Property(result.getName(), result.getType())
            {
                @Override
                public void set(Object object, Object value) throws Exception
                {
                    if (value == null && get(object) != null)
                    {
                        nullProperties.add(getName());
                    }
                    result.set(object, value);
                }

                @Override
                public Class<?>[] getActualTypeArguments()
                {
                    return result.getActualTypeArguments();
                }

                @Override
                public Object get(Object object)
                {
                    return result.get(object);
                }

                @Override
                public List<Annotation> getAnnotations()
                {
                    return Collections.EMPTY_LIST;
                }

                @Override
                public <A extends Annotation> A getAnnotation(Class<A> aClass)
                {
                    return null;
                }
            };
        }

        public void check() throws ConfigurationException
        {
            if (!nullProperties.isEmpty())
                throw new ConfigurationException("Invalid yaml. Those properties " + nullProperties + " are not valid", false);

            if (!missingProperties.isEmpty())
                throw new ConfigurationException("Invalid yaml. Please remove properties " + missingProperties + " from your cassandra.yaml", false);

            if (!deprecationWarnings.isEmpty())
                logger.warn("{} parameters have been deprecated. They have new names; For more information, please refer to NEWS.txt", deprecationWarnings);
        }
    }

    /**
     * @param klass to get replacements for
     * @return map of old names and replacements needed.
     */
    private static Map<Class<?>, Map<String, Replacement>> getNameReplacements(Class<?> klass)
    {
        List<Replacement> replacements = getReplacements(klass);
        Map<Class<?>, Map<String, Replacement>> objectOldNames = new HashMap<>();
        for (Replacement r : replacements)
        {
            Map<String, Replacement> oldNames = objectOldNames.computeIfAbsent(r.parent, ignore -> new HashMap<>());
            if (!oldNames.containsKey(r.oldName))
                oldNames.put(r.oldName, r);
            else
            {
                throw new ConfigurationException("Invalid annotations, you have more than one @Replaces annotation in " +
                                                 "Config class with same old name(" + r.oldName + ") defined.");
            }
        }
        return objectOldNames;
    }

    private static List<Replacement> getReplacements(Class<?> klass)
    {
        List<Replacement> replacements = new ArrayList<>();
        for (Field field : klass.getDeclaredFields())
        {
            String newName = field.getName();
            final ReplacesList[] byType = field.getAnnotationsByType(ReplacesList.class);
            if (byType == null || byType.length == 0)
            {
                Replaces r = field.getAnnotation(Replaces.class);
                if (r != null)
                    addReplacement(klass, replacements, newName, r);
            }
            else
            {
                for (ReplacesList replacesList : byType)
                    for (Replaces r : replacesList.value())
                        addReplacement(klass, replacements, newName, r);
            }
        }
        return replacements.isEmpty() ? Collections.emptyList() : replacements;
    }

    private static void addReplacement(Class<?> klass,
                                       List<Replacement> replacements,
                                       String newName,
                                       Replaces r)
    {
        String oldName = r.oldName();
        boolean deprecated = r.deprecated();

        replacements.add(new Replacement(klass, oldName, newName, deprecated));
    }

    /**
     * Holder for replacements to support backward compatibility between old and new names for configuration parameters
     * backported partially from trunk(CASSANDRA-15234) to support a bug fix/improvement in Cassadra 4.0
     * (CASSANDRA-17141)
     */
    static final class Replacement
    {
        /**
         * Currently we use for Config class
         */
        final Class<?> parent;
        /**
         * Old name of the configuration parameter
         */
        final String oldName;
        /**
         * New name used for the configuration parameter
         */
        final String newName;
        /**
         * A flag to mark whether the old name is deprecated and fire a warning to the user. By default we set it to false.
         */
        final boolean deprecated;

        Replacement(Class<?> parent,
                    String oldName,
                    String newName,
                    boolean deprecated)
        {
            this.parent = Objects.requireNonNull(parent);
            this.oldName = Objects.requireNonNull(oldName);
            this.newName = Objects.requireNonNull(newName);
            // by default deprecated is false
            this.deprecated = deprecated;
        }
    }
}
