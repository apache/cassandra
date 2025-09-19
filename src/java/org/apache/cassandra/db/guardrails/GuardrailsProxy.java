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

package org.apache.cassandra.db.guardrails;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.JMX;
import javax.management.ObjectName;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.LocalizeString;
import org.apache.cassandra.utils.MBeanWrapper;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Collections.unmodifiableMap;
import static java.util.Comparator.comparing;

/**
 * This class is used in two occasions.
 * <p>
 * Firstly, it is used when there is an invocation of
 * nodetool's get/setguardrailsconfig command. We pass nodetool's GuardrailsMBean proxy upon initialisation,
 * and we parse the methods. Then, when somebody invokes these methods, actual MBean is invoked
 * which will interact with the server.
 * <p>
 * Secondly, it is used in server itself when we interact with Guardrails get/set methods via CQL. In that case,
 * we do not want to parse all mbean methods upon every mutation or selection all over again.
 * It is required to have this cached somehow, so we just invoke methods backed by MBean which
 * is an object existing during server's lifetime but all parsing was already done when server started.
 * <p>
 * In nodetool's case, because it is run in a different JVM, this proxy is shortlived. The advantage though is that
 * all code is at once place hence we do not duplicate the code, and we have one view on this code from two perspectives.
 * <p>
 * It is important that this class can be instantiated both in server and client context,
 * so it is required this class does not integrate anything server-side to the client runtime.
 */
public class GuardrailsProxy
{
    public static final GuardrailsProxy instance = new GuardrailsProxy();

    /**
     * Special map for methods which do not adhere to camel-case convention precisely.
     * These will be translated manually.
     */
    public static final Map<String, String> toSnakeCaseTranslationMap = Map.of("ZeroTTLOnTWCSEnabled", "zero_ttl_on_twcs_enabled",
                                                                               "ZeroTTLOnTWCSWarned", "zero_ttl_on_twcs_warned",
                                                                               "FieldsPerUDTFailThreshold", "fields_per_udt_fail_threshold",
                                                                               "FieldsPerUDTWarnThreshold", "fields_per_udt_warn_threshold",
                                                                               "FieldsPerUDTThreshold", "fields_per_udt_threshold",
                                                                               "SimpleStrategyEnabled", "simplestrategy_enabled",
                                                                               "NonPartitionRestrictedQueryEnabled", "non_partition_restricted_index_query_enabled");

    private static final Set<String> ignored = Set.of("password_validator_config");
    public static final Pattern CAMEL_PATTERN = Pattern.compile("([a-z])([A-Z])");
    private static final Pattern REPLACE_WARN_PATTERN = Pattern.compile("_warn_");
    private static final Pattern REPLACE_FAIL_PATTERN = Pattern.compile("_fail_");

    private GuardrailsMBean guardrailsMBean;

    private Map<String, Method> setters;
    private Map<String, List<Method>> getters;

    private Map<String, List<Method>> flagsGetters;
    private Map<String, List<Method>> valuesGetters;
    private Map<String, List<Method>> thresholdsGetters;

    private GuardrailsProxy()
    {
    }

    public Object invoke(Method method, Object... args)
    {
        try
        {
            return method.invoke(guardrailsMBean, args);
        }
        catch (Throwable t)
        {
            throw new InvalidRequestException(t.getCause().getMessage());
        }
    }

    public Map<String, List<Method>> getFlagsGetters()
    {
        return flagsGetters;
    }

    public Map<String, List<Method>> getValuesGetters()
    {
        return valuesGetters;
    }

    public Map<String, List<Method>> getThresholdsGetters()
    {
        return thresholdsGetters;
    }

    public Map<String, List<Method>> getAllGetters()
    {
        return getters;
    }

    public boolean isFlag(String key)
    {
        return flagsGetters.containsKey(key);
    }

    public boolean isValue(String key)
    {
        return valuesGetters.containsKey(key);
    }

    public boolean isThreshold(String key)
    {
        return thresholdsGetters.containsKey(key);
    }

    public Method getSetter(String guardrailName)
    {
        return setters.get(guardrailName);
    }

    public synchronized void clientInitialisation(GuardrailsMBean mBean,
                                                  String guardrailName,
                                                  boolean initializeSetters,
                                                  boolean initializeGetters)
    {
        initialize(mBean, guardrailName, initializeSetters, initializeGetters);
    }

    public void serverInitialisation()
    {
        try
        {
            guardrailsMBean = JMX.newMBeanProxy(MBeanWrapper.instance.getMBeanServer(),
                                                new ObjectName(Guardrails.MBEAN_NAME),
                                                GuardrailsMBean.class);

            initialize(guardrailsMBean, null, true, true);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    public void initialize(GuardrailsMBean guardrailsMBean,
                           String guardrailName,
                           boolean initializeSetters,
                           boolean initializeGetters)
    {
        if (!initializeSetters && !initializeGetters)
            return;

        if (setters != null && getters != null)
            return;

        this.guardrailsMBean = guardrailsMBean;

        Method[] guardrailsMethods = guardrailsMBean.getClass().getDeclaredMethods();

        if (setters == null && initializeSetters)
        {
            setters = unmodifiableMap(Arrays.stream(guardrailsMethods)
                                            .filter(method -> method.getName().startsWith("set") && !method.getName().endsWith("CSV"))
                                            .collect(Collectors.toMap(method -> toSnakeCase(method.getName().substring(3)),
                                                                      method -> method))
                                            .entrySet()
                                            .stream()
                                            .filter(p -> !ignored.contains(p.getKey()))
                                            .sorted(Map.Entry.comparingByKey())
                                            .collect(Collectors.toMap(Map.Entry::getKey,
                                                                      Map.Entry::getValue,
                                                                      (e1, e2) -> e1,
                                                                      LinkedHashMap::new)));
        }

        if (getters == null && initializeGetters)
        {
            Map<String, List<Method>> allGetters = stream(guardrailsMethods)
                                                   .filter(method -> method.getName().startsWith("get")
                                                                     && !method.getName().endsWith("CSV")
                                                                     && !(method.getName().endsWith("WarnThreshold") || method.getName().endsWith("FailThreshold")))
                                                   .collect(Collectors.groupingBy(method -> toSnakeCase(method.getName().substring(3))));

            // TODO for now remove custom guardrails
            for (String ignore : ignored)
                allGetters.remove(ignore);

            Map<String, List<Method>> thresholds = stream(guardrailsMethods)
                                                   .filter(method -> method.getName().startsWith("get")
                                                                     && !method.getName().endsWith("CSV")
                                                                     && (method.getName().endsWith("WarnThreshold") || method.getName().endsWith("FailThreshold")))
                                                   .filter(method -> {
                                                       if (guardrailName == null)
                                                           return true;

                                                       String snakeCase = toSnakeCase(method.getName().substring(3));
                                                       String snakeCaseSuccinct = snakeCase.replace("_warn_", "_")
                                                                                           .replace("_fail_", "_");

                                                       return guardrailName.equals(snakeCase) || guardrailName.equals(snakeCaseSuccinct);
                                                   })
                                                   .sorted(comparing(Method::getName))
                                                   .collect(Collectors.groupingBy(method -> {
                                                       String methodName = method.getName().substring(3);
                                                       String snakeCase = toSnakeCase(methodName);

                                                       if (guardrailName == null)
                                                       {
                                                           if (snakeCase.endsWith("warn_threshold"))
                                                               return REPLACE_WARN_PATTERN.matcher(snakeCase).replaceAll("_");
                                                           else
                                                               return REPLACE_FAIL_PATTERN.matcher(snakeCase).replaceAll("_");
                                                       }
                                                       else
                                                       {
                                                           return snakeCase;
                                                       }
                                                   }));

            allGetters.putAll(thresholds);

            getters = unmodifiableMap(allGetters.entrySet()
                                                .stream()
                                                .sorted(Map.Entry.comparingByKey())
                                                .collect(Collectors.toMap(Map.Entry::getKey,
                                                                          Map.Entry::getValue,
                                                                          (e1, e2) -> e1,
                                                                          LinkedHashMap::new)));

            flagsGetters = filterGetters(allGetters, e -> {
                for (Method m : e.getValue())
                {
                    if (!m.getReturnType().equals(Boolean.class) && !m.getReturnType().equals(boolean.class))
                        return false;
                }

                return true;
            });

            valuesGetters = filterGetters(allGetters, e -> {
                for (Method m : e.getValue())
                {
                    if (!m.getReturnType().equals(Set.class))
                        return false;
                }

                return true;
            });

            thresholdsGetters = filterGetters(allGetters, e -> {
                for (Method m : e.getValue())
                {
                    if (!m.getName().endsWith("Threshold"))
                        return false;
                }

                return true;
            });
        }
    }

    private Map<String, List<Method>> filterGetters(Map<String, List<Method>> getters,
                                                    Predicate<Map.Entry<String, List<Method>>> filter)
    {
        return unmodifiableMap(getters.entrySet()
                                      .stream()
                                      .filter(filter)
                                      .sorted(Map.Entry.comparingByKey())
                                      .collect(Collectors.toMap(Map.Entry::getKey,
                                                                Map.Entry::getValue,
                                                                (e1, e2) -> e1,
                                                                LinkedHashMap::new)));
    }

    public Object[] prepareArguments(String[] args, Method method)
    {
        Class<?>[] parameterTypes = method.getParameterTypes();
        Object[] arguments = new Object[args.length];

        if (method.getName().endsWith("Threshold"))
        {
            for (int i = 0; i < args.length; i++)
                arguments[args.length - i - 1] = castType(parameterTypes[i], args[i]);
        }
        else
        {
            for (int i = 0; i < args.length; i++)
                arguments[i] = castType(parameterTypes[i], args[i]);
        }

        return arguments;
    }

    private Object castType(Class<?> targetType, String value) throws IllegalArgumentException
    {
        if (targetType == String.class)
            return value.equals("null") ? "" : value;
        else if (targetType == int.class || targetType == Integer.class)
            return getNumber(value, Integer::parseInt, -1);
        else if (targetType == long.class || targetType == Long.class)
            return getNumber(value, Long::parseLong, -1);
        else if (targetType == boolean.class || targetType == Boolean.class)
        {
            return getNumber(value, (v) -> {
                if (!v.equals("true") && !v.equals("false"))
                    throw new IllegalStateException("Use 'true' or 'false' values for booleans");

                return Boolean.parseBoolean(v);
            }, false);
        }
        else if (targetType == Set.class)
        {
            if (value == null || value.equals("null") || value.equals("[]"))
                return new HashSet<>();
            else
                return new LinkedHashSet<>(Arrays.asList(value.split(",")));
        }
        else
        {
            throw new IllegalArgumentException(format("unsupported type: %s", targetType));
        }
    }

    private <T> T getNumber(String value, Function<String, T> transformer, T defaultValue)
    {
        if (value == null || value.equals("null"))
            return defaultValue;

        try
        {
            return transformer.apply(value);
        }
        catch (NumberFormatException ex)
        {
            throw new IllegalStateException(format("Unable to parse value %s", value), ex);
        }
    }

    private static String toSnakeCase(String camelCase)
    {
        if (camelCase == null || camelCase.isEmpty())
            return camelCase;
        else
        {
            String maybeSnakeCase = toSnakeCaseTranslationMap.get(camelCase);
            if (maybeSnakeCase != null)
                return maybeSnakeCase;

            return LocalizeString.toLowerCaseLocalized(CAMEL_PATTERN.matcher(camelCase).replaceAll("$1_$2"));
        }
    }
}
