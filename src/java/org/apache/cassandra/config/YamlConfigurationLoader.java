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

import java.beans.IntrospectionException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.base.Joiner;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.MissingProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

public class YamlConfigurationLoader implements ConfigurationLoader
{
    private static final Logger logger = LoggerFactory.getLogger(YamlConfigurationLoader.class);

    private final static String DEFAULT_CONFIGURATION = "cassandra.yaml";

    /**
     * Inspect the classpath to find storage configuration file
     */
    static URL getStorageConfigURL() throws ConfigurationException
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
                    throw new ConfigurationException("Expecting URI in variable: [cassandra.config].  Please prefix the file with " + required + File.separator +
                            " for local files or " + required + "<server>" + File.separator + " for remote files. Aborting. If you are executing this from an external tool, it needs to set Config.setClientMode(true) to avoid loading configuration.");
                throw new ConfigurationException("Cannot locate " + configUrl + ".  If this is a local file, please confirm you've provided " + required + File.separator + " as a URI prefix.");
            }
        }

        return url;
    }

    public Config loadConfig() throws ConfigurationException
    {
        return loadConfig(getStorageConfigURL());
    }

    public Config loadConfig(URL url) throws ConfigurationException
    {
        InputStream input = null;
        try
        {
            logger.info("Loading settings from {}", url);
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

            logConfig(configBytes);
            
            org.yaml.snakeyaml.constructor.Constructor constructor = new org.yaml.snakeyaml.constructor.Constructor(Config.class);
            TypeDescription seedDesc = new TypeDescription(SeedProviderDef.class);
            seedDesc.putMapPropertyType("parameters", String.class, String.class);
            constructor.addTypeDescription(seedDesc);
            MissingPropertiesChecker propertiesChecker = new MissingPropertiesChecker();
            constructor.setPropertyUtils(propertiesChecker);
            Yaml yaml = new Yaml(constructor);
            Config result = yaml.loadAs(new ByteArrayInputStream(configBytes), Config.class);
            result.configHintedHandoff();
            propertiesChecker.check();
            return result;
        }
        catch (YAMLException e)
        {
            throw new ConfigurationException("Invalid yaml", e);
        }
    }

    private void logConfig(byte[] configBytes)
    {
        Map<Object, Object> configMap = new TreeMap<>((Map<?, ?>) new Yaml().load(new ByteArrayInputStream(configBytes)));
        // these keys contain passwords, don't log them
        for (String sensitiveKey : new String[] { "client_encryption_options", "server_encryption_options" })
        {
            if (configMap.containsKey(sensitiveKey))
            {
                configMap.put(sensitiveKey, "<REDACTED>");
            }
        }
        logger.info("Node configuration:[" + Joiner.on("; ").join(configMap.entrySet()) + "]");
    }

    private static class MissingPropertiesChecker extends PropertyUtils
    {
        private final Set<String> missingProperties = new HashSet<>();

        public MissingPropertiesChecker()
        {
            setSkipMissingProperties(true);
        }

        @Override
        public Property getProperty(Class<? extends Object> type, String name) throws IntrospectionException
        {
            Property result = super.getProperty(type, name);
            if (result instanceof MissingProperty)
            {
                missingProperties.add(result.getName());
            }
            return result;
        }

        public void check() throws ConfigurationException
        {
            if (!missingProperties.isEmpty())
            {
                throw new ConfigurationException("Invalid yaml. Please remove properties " + missingProperties + " from your cassandra.yaml");
            }
        }
    }
}
