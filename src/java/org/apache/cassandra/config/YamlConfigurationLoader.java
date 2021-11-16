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
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

import org.apache.cassandra.io.util.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.composer.Composer;
import org.yaml.snakeyaml.constructor.Constructor;
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
    @VisibleForTesting
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
    private static String content;

    @Override
    public Config loadConfig() throws ConfigurationException
    {
        if (storageConfigURL == null)
            storageConfigURL = getStorageConfigURL();
        content = YamlConfigurationLoader.readStorageConfig(storageConfigURL);
        isConfigFileValid();
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

            Constructor constructor = new CustomConstructor(Config.class, Yaml.class.getClassLoader());
            Map<Class<?>, Map<String, Replacement>> replacements = getReplacements(Config.class);
            PropertiesChecker propertiesChecker = new PropertiesChecker(replacements);
            constructor.setPropertyUtils(propertiesChecker);
            Yaml yaml = new Yaml(constructor);
            Config result = loadConfig(yaml, configBytes);
            propertiesChecker.check();
            return result;
        }
        catch (YAMLException e)
        {
            throw new ConfigurationException("Invalid yaml: " + url, e);
        }
    }

    private static String readStorageConfig(URL url)
    {
        String content = "";

        try
        {
            content = new String (Files.readAllBytes(Paths.get(String.valueOf(url).substring(5))));

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return content;
    }

    private static void isConfigFileValid()
    {
        if (isBlank("commitlog_sync_period"))
            throw new IllegalArgumentException("You should provide a value for commitlog_sync_period or comment it in " +
                                               "order to get a default one");

        if (isBlank("commitlog_sync_group_window"))
            throw new IllegalArgumentException("You should provide a value for commitlog_sync_group_window or comment it in " +
                                               "order to get a default one");
    }

    /**
     * This method helps to preserve the behavior of parameters which were originally of primitive type and
     * without default value in Config.java (CASSANDRA-15234)
     */
    private static boolean isBlank(String property)
    {
        Pattern p = Pattern.compile(String.format("%s%s *: *$", '^', property), Pattern.MULTILINE);
        Matcher m = p.matcher(content);
        return m.find();
    }

    @VisibleForTesting
    public static <T> T fromMap(Map<String,Object> map, Class<T> klass)
    {
        return fromMap(map, true, klass);
    }

    @SuppressWarnings("unchecked") //getSingleData returns Object, not T
    public static <T> T fromMap(Map<String,Object> map, boolean shouldCheck, Class<T> klass)
    {
        Constructor constructor = new YamlConfigurationLoader.CustomConstructor(klass, klass.getClassLoader());
        Map<Class<?>, Map<String, Replacement>> replacements = getReplacements(Config.class);
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
        private final Set<String> missingProperties = new HashSet<>();

        private final Set<String> nullProperties = new HashSet<>();

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
            if(typeReplacements.containsKey(name))
            {
                Replacement replacement = typeReplacements.get(name);
                Converter converter = replacement.converter;

                final Property newProperty = super.getProperty(type, replacement.newName);
                result = new Property(replacement.oldName, replacement.oldType)
                {
                    @Override
                    public Class<?>[] getActualTypeArguments()
                    {
                        return newProperty.getActualTypeArguments();
                    }

                    @Override
                    public void set(Object o, Object o1) throws Exception
                    {
                        Object migratedValue = converter.apply(o1);
                        newProperty.set(o, migratedValue);
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

                if(replacement.deprecated)
                {
                    logger.warn("{} parameter has been deprecated. It has a new name and/or value format; For more information, please refer to NEWS.txt", name);
                }
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
            {
                throw new ConfigurationException("Invalid yaml. Those properties " + nullProperties + " are not valid", false);
            }

            if (!missingProperties.isEmpty())
            {
                throw new ConfigurationException("Invalid yaml. Please remove properties " + missingProperties + " from your cassandra.yaml", false);
            }
        }
    }

    /**
     * @param klass to get replacements for
     * @return map of old names and replacements needed.
     */
    private static Map<Class<? extends Object>, Map<String, Replacement>> getReplacements(Class<? extends Object> klass)
    {
        List<Replacement> replacements = getReplacementsRecursive(klass);
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

    /**
     * @param klass to get replacements for
     * @return map of old names and replacements needed.
     */
    private static List<Replacement> getReplacementsRecursive(Class<?> klass)
    {
        Set<Class<?>> seen = new HashSet<>(); // to make sure not to process the same type twice
        Map<Class<? extends Converter>, Converter> converterCache = new HashMap<>();
        List<Replacement> accum = new ArrayList<>();
        getReplacementsRecursive(seen, converterCache, accum, klass);
        return accum.isEmpty() ? Collections.emptyList() : accum;
    }

    private static void getReplacementsRecursive(Set<Class<?>> seen,
                                                 Map<Class<? extends Converter>, Converter> converterCache,
                                                 List<Replacement> accum,
                                                 Class<?> klass)
    {
        accum.addAll(getReplacements(converterCache, klass));
        for (Field field : klass.getDeclaredFields())
        {
            if (seen.add(field.getType()))
            {
                // first time looking at this type, walk it
                getReplacementsRecursive(seen, converterCache, accum, field.getType());
            }
        }
    }

    private static List<Replacement> getReplacements(Map<Class<? extends Converter>, Converter> converterCache, Class<?> klass)
    {
        List<Replacement> replacements = new ArrayList<>();
        for (Field field : klass.getDeclaredFields())
        {
            String newName = field.getName();
            Class<?> newType = field.getType();
            final ReplacesList[] byType = field.getAnnotationsByType(ReplacesList.class);
            if (byType == null || byType.length == 0)
            {
                Replaces r = field.getAnnotation(Replaces.class);
                if (r != null)
                    addReplacement(converterCache, klass, replacements, newName, newType, r);
            }
            else
            {
                for (ReplacesList replacesList : byType)
                    for (Replaces r : replacesList.value())
                        addReplacement(converterCache, klass, replacements, newName, newType, r);
            }
        }
        return replacements.isEmpty() ? Collections.emptyList() : replacements;
    }

    private static void addReplacement(Map<Class<? extends Converter>, Converter> converterCache,
                                       Class<?> klass,
                                       List<Replacement> replacements,
                                       String newName, Class<?> newType,
                                       Replaces r)
    {
        String oldName = r.oldName();
        Converter converter = converterCache.computeIfAbsent(r.converter(), converterKlass -> {
            try
            {
                return converterKlass.newInstance();
            }
            catch (IllegalAccessException | InstantiationException e)
            {
                throw new RuntimeException("Unable to create converter of type " + converterKlass, e);
            }
        });

        boolean deprecated = r.deprecated();

        Class<?> oldType = converter.getInputType();
        if (oldType == null)
            oldType = newType;

        replacements.add(new Replacement(klass, oldName, oldType, newName, converter, deprecated));
    }

    /**
     * Holder for replacements to support backward compatibility between old and new names and types
     * of configuration parameters (CASSANDRA-15234)
     */
    static final class Replacement
    {
        /**
         * Currently we use Config class
         */
        final Class<?> parent;
        /**
         * Old name of the configuration parameter
         */
        final String oldName;
        /**
         * Old type of the configuration parameter
         */
        final Class<?> oldType;
        /**
         * New name used for the configuration parameter
         */
        final String newName;
        /**
         * Converter to be used according to the old default unit which was provided as a suffix of the configuration
         * parameter
         */
        final Converter converter;
        final boolean deprecated;

        Replacement(Class<?> parent,
                    String oldName, Class<?> oldType,
                    String newName, Converter converter, boolean deprecated)
        {
            this.parent = Objects.requireNonNull(parent);
            this.oldName = Objects.requireNonNull(oldName);
            this.oldType = Objects.requireNonNull(oldType);
            this.newName = Objects.requireNonNull(newName);
            this.converter = Objects.requireNonNull(converter);
            // by default deprecated is false
            this.deprecated = deprecated;
        }
    }
}

