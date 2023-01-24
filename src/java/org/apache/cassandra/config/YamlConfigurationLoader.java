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
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
            PropertiesChecker propertiesChecker = new PropertiesChecker();
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

    public static <T> T fromMap(Map<String,Object> map, Class<T> klass)
    {
        return fromMap(map, true, klass);
    }

    @SuppressWarnings("unchecked") //getSingleData returns Object, not T
    public static <T> T fromMap(Map<String,Object> map, boolean shouldCheck, Class<T> klass)
    {
        SafeConstructor constructor = new YamlConfigurationLoader.CustomConstructor(klass, klass.getClassLoader());
        YamlConfigurationLoader.PropertiesChecker propertiesChecker = new YamlConfigurationLoader.PropertiesChecker();
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

        public PropertiesChecker()
        {
            setSkipMissingProperties(true);
        }

        @Override
        public Property getProperty(Class<? extends Object> type, String name)
        {
            final Property result = super.getProperty(type, name);

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
}
