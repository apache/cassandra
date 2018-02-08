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

package org.apache.cassandra.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cdc.ICDCHandler;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;

public final class CDCParams
{
    private static final Logger logger = LoggerFactory.getLogger(CDCParams.class);

    public enum Option
    {
        CLASS;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    private abstract class NoOpsHandler implements ICDCHandler {}
    private abstract class UnknownHandler extends NoOpsHandler {}
    private abstract class DefaultHandler extends NoOpsHandler {}

    public static final CDCParams DEFAULT = new CDCParams(DefaultHandler.class,
                                                          Collections.emptyMap());

    private final Class<? extends ICDCHandler> klass;
    private final ImmutableMap<String, String> options;

    private CDCParams(Class<? extends ICDCHandler> klass, Map<String, String> options)
    {
        this.klass = klass;
        this.options = ImmutableMap.copyOf(options);
    }

    public static CDCParams create(Class<? extends ICDCHandler> klass, Map<String, String> options)
    {
        return new CDCParams(klass, options);
    }

    public Map<String, String> options()
    {
        return options;
    }

    public boolean isUnknownHandler()
    {
        return this.klass == UnknownHandler.class;
    }

    public boolean isNoOpsHandler()
    {
        return NoOpsHandler.class.isAssignableFrom(this.klass);
    }

    public boolean isDefaultHandler()
    {
        return this.klass == DefaultHandler.class;
    }

    public Class<? extends ICDCHandler> klass()
    {
        return this.klass;
    }

    public static CDCParams fromMap(Map<String, String> map)
    {
        if (map == null)
            return DEFAULT;

        Map<String, String> options = new HashMap<>(map);

        String className = options.remove(Option.CLASS.toString());
        if (StringUtils.isBlank(className))
            return DEFAULT;

        Class<? extends ICDCHandler> klass;
        try
        {
            klass = classFromName(className);
        }
        catch (ConfigurationException e)
        {
            logger.info(e.getMessage()); // unable to find the class return unknown handler
            options.put(Option.CLASS.toString(), className);
            return create(UnknownHandler.class, options);
        }
        return create(klass, options);
    }

    private static Class<? extends ICDCHandler> classFromName(String name)
    {
        String className = name.contains(".") ? name :
                           "org.apache.cassandra.cdc." + name;
        Class<ICDCHandler> cdcHandlerClass = FBUtilities.classForName(className, "CDC handler");

        if (!ICDCHandler.class.isAssignableFrom(cdcHandlerClass))
        {
            throw new ConfigurationException(format("CDC handler class %s is not derived from ICDCHandler",
                                                    className));
        }
        return cdcHandlerClass;
    }

    public Map<String, String> asMap()
    {
        Map<String, String> map = new HashMap<>(options());
        if (isNoOpsHandler())
            return map;

        map.put(Option.CLASS.toString(), klass.getName());
        return map;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("class", klass.getName())
                          .add("options", options)
                          .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CDCParams))
            return false;

        CDCParams cp = (CDCParams) o;
        return klass.equals(cp.klass) && options.equals(cp.options);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(klass, options);
    }
}
