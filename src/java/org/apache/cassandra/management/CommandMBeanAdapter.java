/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.management;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.ReflectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.management.api.ArgumentMetadata;
import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandMetadata;
import org.apache.cassandra.management.api.OptionMetadata;
import org.apache.cassandra.management.api.ParameterMetadata;
import org.apache.cassandra.utils.JsonUtils;

import static javax.management.MBeanOperationInfo.ACTION;
import static javax.management.MBeanOperationInfo.INFO;
import static org.apache.cassandra.cql3.statements.ExecuteCommandStatement.COMMAND_RESULT_SCHEMA_EXECUTION_ID;
import static org.apache.cassandra.cql3.statements.ExecuteCommandStatement.COMMAND_RESULT_SCHEMA_OUTPUT;
import static org.apache.cassandra.management.api.ParameterMetadata.COMMAND_POSITIONAL_PARAM_PREFIX;
import static org.apache.cassandra.utils.JsonUtils.convertDefaultValue;
import static org.apache.cassandra.utils.JsonUtils.getJsonType;

/**
 * Command MBean exposes a single management command to the JMX interface.
 *
 * <p>Uses JSON-based parameter format:
 * <ul>
 *   <li>Single "invoke" operation with JSON string parameter</li>
 *   <li>JSON format: {"optionName": "value", "param0": "value", ...}</li>
 *   <li>Option names: use option name or any alias (e.g., "concurrent-compactors", "--concurrent-compactors")</li>
 *   <li>Positional parameters: use "param0", "param1", etc. or parameter name</li>
 *   <li>Returns command output as String</li>
 * </ul>
 *
 * <p>
 * Invocation:
 * <pre>
 * // Command: setconcurrentcompactors --concurrent-compactors 4
 * mbean.invoke("invoke",
 *     new Object[]{"{\"concurrent-compactors\": \"4\"}"},
 *     new String[]{ "String" });
 *
 * // Command: getendpoints keyspace table
 * mbean.invoke("invoke",
 *     new Object[]{"{\"param0\": \"mykeyspace\", \"param1\": \"mytable\"}"},
 *     new String[]{ "String" });
 * </pre>
 *
 * <p>The {@code invoke} operation accepts exactly one argument: the JSON string above. Other forms
 * (e.g. name-value pairs spread across multiple arguments) are not supported and are rejected with an
 * {@link IllegalArgumentException}.
 */
public class CommandMBeanAdapter implements DynamicMBean
{
    public static final String INVOKE_METHOD = "invoke";
    public static final String GET_JSON_SCHEMA_METHOD = "getJsonSchema";
    public static final String GET_PARAMETER_INFO_METHOD = "getParameterInfo";

    private static final Logger logger = LoggerFactory.getLogger(CommandMBeanAdapter.class);

    private final String fullCommandName;
    private final CommandMetadata metadata;
    private final CommandInvokerService.Executor executor;

    public CommandMBeanAdapter(String fullCommandName, Command<?> command, CommandInvokerService.Executor executor)
    {
        this.fullCommandName = fullCommandName;
        this.metadata = command.metadata();
        this.executor = executor;
    }

    @Override
    public MBeanInfo getMBeanInfo()
    {
        List<MBeanOperationInfo> operations = new ArrayList<>();

        operations.add(new MBeanOperationInfo(
        INVOKE_METHOD,
            "Execute command with JSON parameters. Format: {\"optionName\": \"value\", \"param0\": \"value\", ...}",
            new MBeanParameterInfo[]{
                new MBeanParameterInfo("jsonParameters",
                                       String.class.getName(),
                                       "JSON object with command parameters. Use getJsonSchema() to see available parameters and types.")
            },
            String.class.getName(),
        ACTION));

        operations.add(new MBeanOperationInfo(
            GET_JSON_SCHEMA_METHOD,
            "Get JSON schema describing all available parameters (name -> type mapping)",
            new MBeanParameterInfo[0],
            String.class.getName(),
            INFO));

        operations.add(new MBeanOperationInfo(
            GET_PARAMETER_INFO_METHOD,
            "Get information about all available parameters (human-readable format)",
            new MBeanParameterInfo[0],
            String.class.getName(),
            INFO));

        return new MBeanInfo(CommandMBeanAdapter.class.getName(),
                             metadata.description() != null ? metadata.description() : "Command: " + metadata.name(),
                             null,
                             null,
                             operations.toArray(new MBeanOperationInfo[0]),
                             null);
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException
    {
        if (INVOKE_METHOD.equals(actionName))
        {
            if (params == null || params.length != 1 || !(params[0] instanceof String))
                throw new IllegalArgumentException("invoke requires exactly one parameter (JSON string)");

            String jsonParams = (String) params[0];
            try
            {
                CommandInvokerService.CommandResult result = executor.execute(fullCommandName,
                                                                              () -> CommandExecutionArgsSerde.fromJson(jsonParams, metadata));

                Map<String, Object> jsonResult = new TreeMap<>();
                jsonResult.put(COMMAND_RESULT_SCHEMA_EXECUTION_ID, result.getExecutionId().toString());
                jsonResult.put(COMMAND_RESULT_SCHEMA_OUTPUT, result.getOutput());
                return JsonUtils.writeAsJsonString(jsonResult);
            }
            catch (CommandAuthorizationException e)
            {
                logger.error("Authorization error executing command: {}", metadata.name(), e);
                throw new SecurityException("Access Denied: " + e.getMessage());
            }
            catch (CommandValidationException e)
            {
                logger.error("Validation error for command: {}", metadata.name(), e);
                throw new IllegalArgumentException(ManagementUtils.causeMessages(e));
            }
            catch (CommandExecutionException e)
            {
                logger.error("Error executing command: {}", metadata.name(), e);
                throw new RuntimeException(ManagementUtils.causeMessages(e));
            }
            catch (Exception e)
            {
                logger.error("Unexpected error executing command: {}", metadata.name(), e);
                throw new RuntimeException("Unexpected error executing command: " + e.getMessage(), e);
            }
        }

        if (GET_JSON_SCHEMA_METHOD.equals(actionName))
            return getJsonSchema();

        if (GET_PARAMETER_INFO_METHOD.equals(actionName))
            return getParameterInfo();

        throw new UnsupportedOperationException("Unknown operation: " + actionName);
    }

    public String getJsonSchema()
    {
        try
        {
            Map<String, Object> schema = new LinkedHashMap<>();
            schema.put("$schema", "http://json-schema.org/draft-07/schema#");
            schema.put("type", "object");
            schema.put("title", metadata.name());
            schema.put("description", metadata.description());

            Map<String, Object> properties = new LinkedHashMap<>();
            List<String> required = new ArrayList<>();

            for (OptionMetadata option : metadata.options())
            {
                String primaryName = option.paramLabel();
                properties.put(primaryName, buildJsonSchemaProperty(option));

                if (option.required())
                    required.add(primaryName);
            }

            List<ParameterMetadata> sortedParams = new ArrayList<>(metadata.parameters());
            sortedParams.sort(Comparator.comparingInt(ParameterMetadata::index));

            for (ParameterMetadata param : sortedParams)
            {
                String paramName = COMMAND_POSITIONAL_PARAM_PREFIX + param.index();
                properties.put(paramName, buildJsonSchemaProperty(param));

                if (param.required())
                    required.add(paramName);
            }

            schema.put("properties", properties);

            if (!required.isEmpty())
                schema.put("required", required);

            return JsonUtils.writeAsPrettyJsonString(schema);
        }
        catch (Exception e)
        {
            logger.error("Error generating JSON schema for command: {}", metadata.name(), e);
            throw new RuntimeException("Failed to generate JSON schema: " + e.getMessage(), e);
        }
    }

    private Map<String, Object> buildJsonSchemaProperty(ArgumentMetadata arg)
    {
        Map<String, Object> prop = new LinkedHashMap<>();
        prop.put("type", getJsonType(arg.type()));

        if (arg.names() != null && arg.names().length > 0)
            prop.put("aliases", Arrays.stream(arg.names())
                                     .filter(name -> !name.equals(arg.paramLabel()))
                                     .collect(Collectors.toList()));

        if (arg.description() != null && !arg.description().isEmpty())
            prop.put("description", arg.description());

        String defaultValue = arg.defaultValue();
        if (defaultValue != null && !defaultValue.isEmpty())
            prop.put("default", convertDefaultValue(defaultValue, arg.type()));

        if (arg.type().isArray() || List.class.isAssignableFrom(arg.type()))
        {
            prop.put("type", "array");
            Map<String, Object> items = new LinkedHashMap<>();
            items.put("type", "string");
            prop.put("items", items);
        }

        if (arg.type().isEnum())
        {
            prop.put("enum", Arrays.stream(arg.type().getEnumConstants())
                                   .map(Object::toString)
                                   .collect(Collectors.toList()));
        }

        return prop;
    }

    private static String getTypeName(Class<?> type)
    {
        return type.getCanonicalName();
    }

    private String getParameterInfo()
    {
        StringBuilder info = new StringBuilder();
        info.append("Command: ").append(metadata.name()).append('\n');
        info.append("Description: ").append(metadata.description()).append("\n\n");

        info.append("Options:\n");
        for (OptionMetadata option : metadata.options())
        {
            info.append("  - ").append(option.paramLabel());
            if (option.names().length > 0)
                info.append(" (aliases: ").append(String.join(", ", option.names())).append(')');
            appendRequiredClause(info, getTypeName(option.type()), option.required(), option.description());
        }

        info.append("\nPositional Parameters:\n");
        List<ParameterMetadata> sortedParams = new ArrayList<>(metadata.parameters());
        sortedParams.sort((a, b) -> Integer.compare(a.index(), b.index()));

        for (ParameterMetadata param : sortedParams)
        {
            info.append("  - param").append(param.index());
            if (param.paramLabel() != null && !param.paramLabel().isEmpty())
                info.append(" (").append(param.paramLabel()).append(')');
            appendRequiredClause(info, getTypeName(param.type()), param.required(), param.description());
        }

        return info.toString();
    }

    private static void appendRequiredClause(StringBuilder info,
                                             String typeName,
                                             boolean required,
                                             String description)
    {
        info.append(" [").append(typeName).append(']');
        if (required)
            info.append(" [REQUIRED]");
        if (description != null && !description.isEmpty())
            info.append("\n    ").append(description);
        info.append('\n');
    }

    @Override
    public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public AttributeList getAttributes(String[] attributes)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
