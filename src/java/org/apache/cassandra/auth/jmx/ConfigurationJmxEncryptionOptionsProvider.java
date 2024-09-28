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
package org.apache.cassandra.auth.jmx;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.cassandra.config.EncryptionOptions;

public class ConfigurationJmxEncryptionOptionsProvider implements JmxEncryptionOptionsProvider
{
    private static final String encryptionsOptionsClassName = EncryptionOptions.class.getName();
    private static final String jmxEncryptionOptionsClassName = EncryptionOptions.JmxEncryptionOptions.class.getName();

    public static class JmxEncryptionOptionSetter
    {
        public void setValue(EncryptionOptions.JmxEncryptionOptions encryptionOptions, String className, String fieldName, String value)
        {
            try
            {
                Field field = Class.forName(className).getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(encryptionOptions, value);
            }
            catch (NoSuchFieldException | IllegalAccessException | ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static class JmxEncryptionOptionSetterBooleanPrimative extends JmxEncryptionOptionSetter
    {
        public void setValue(EncryptionOptions.JmxEncryptionOptions encryptionOptions, String className, String fieldName, String value)
        {
            try
            {
                Field field = Class.forName(className).getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(encryptionOptions, Boolean.parseBoolean(value));
            }
            catch (NoSuchFieldException | IllegalAccessException | ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static class JmxEncryptionOptionSetterBooleanObject extends JmxEncryptionOptionSetter
    {
        public void setValue(EncryptionOptions.JmxEncryptionOptions encryptionOptions, String className, String fieldName, String value)
        {
            try
            {
                Field field = Class.forName(className).getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(encryptionOptions, Boolean.valueOf(value));
            }
            catch (NoSuchFieldException | IllegalAccessException | ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static class JmxEncryptionOptionSetterListStrings extends JmxEncryptionOptionSetter
    {
        private static final Pattern COMMA_DELIMITED_VALUES_PATTERN = Pattern.compile("\\s*,\\s*");

        public void setValue(EncryptionOptions.JmxEncryptionOptions encryptionOptions, String className, String fieldName, String value)
        {
            int value_len = value.length();
            int start_offset = value.charAt(0) == '[' ? 1 : 0;
            int end_offset = value.charAt(value_len - 1) == ']' ? value_len - 1 : value_len;
            try
            {
                Field field = Class.forName(className).getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(
                    encryptionOptions,
                    Arrays.asList(COMMA_DELIMITED_VALUES_PATTERN.split(value.substring(start_offset, end_offset)))
                );
            }
            catch (NoSuchFieldException | IllegalAccessException | ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    enum JmxEncryptionOption
    {
        enabled("enabled", encryptionsOptionsClassName, "enabled", new JmxEncryptionOptionSetterBooleanObject()),
        keystorePath("keystore", encryptionsOptionsClassName, "keystore", new JmxEncryptionOptionSetter()),
        keystorePassword("keystore_password", encryptionsOptionsClassName, "keystore_password", new JmxEncryptionOptionSetter()),
        keystoreType("keystore_type", jmxEncryptionOptionsClassName, "keystore_type", new JmxEncryptionOptionSetter()),
        truststorePath("truststore", encryptionsOptionsClassName, "truststore", new JmxEncryptionOptionSetter()),
        truststorePassword("truststore_password", encryptionsOptionsClassName, "truststore_password", new JmxEncryptionOptionSetter()),
        truststoreType("truststore_type", jmxEncryptionOptionsClassName, "truststore_type", new JmxEncryptionOptionSetter()),
        requireClientAuth("require_client_auth", encryptionsOptionsClassName, "require_client_auth", new JmxEncryptionOptionSetterBooleanPrimative()),
        cipherSuites("cipher_suites", encryptionsOptionsClassName, "cipher_suites", new JmxEncryptionOptionSetterListStrings()),
        protocols("protocols", encryptionsOptionsClassName, "accepted_protocols", new JmxEncryptionOptionSetterListStrings());

        public final String optionName;
        public final String className;
        public final String fieldName;
        public final JmxEncryptionOptionSetter setter;

        JmxEncryptionOption(String optionName, String className, String fieldName, JmxEncryptionOptionSetter setter)
        {
            this.optionName = optionName;
            this.className = className;
            this.fieldName = fieldName;
            this.setter = setter;
        }
    }

    private final EncryptionOptions.JmxEncryptionOptions jmxEncryptionOptions =
        new EncryptionOptions.JmxEncryptionOptions();

    public ConfigurationJmxEncryptionOptionsProvider(Map<String, String> parameters)
    {
        for (JmxEncryptionOption jeo : JmxEncryptionOption.values())
        {
            String value = (parameters.get(jeo.optionName) == null) ? null : String.valueOf(parameters.get(jeo.optionName));
            if (value != null)
                jeo.setter.setValue(jmxEncryptionOptions, jeo.className, jeo.fieldName, value.trim());
        }
        jmxEncryptionOptions.applyConfig();
    }

    public EncryptionOptions.JmxEncryptionOptions getJmxEncryptionOptions()
    {
        return jmxEncryptionOptions;
    }
}