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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.exceptions.ConfigurationException;

public final class Replacements
{
    private Replacements()
    {
    }

    /**
     * @param klass to get replacements for
     * @return map of old names and replacements needed.
     */
    public static Map<Class<? extends Object>, Map<String, Replacement>> getNameReplacements(Class<? extends Object> klass)
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
        List<Replacement> accum = new ArrayList<>();
        getReplacementsRecursive(seen, accum, klass);
        return accum.isEmpty() ? Collections.emptyList() : accum;
    }

    private static void getReplacementsRecursive(Set<Class<?>> seen,
                                                 List<Replacement> accum,
                                                 Class<?> klass)
    {
        accum.addAll(getReplacements(klass));
        for (Field field : klass.getDeclaredFields())
        {
            if (seen.add(field.getType()))
            {
                // first time looking at this type, walk it
                getReplacementsRecursive(seen, accum, field.getType());
            }
        }
    }

    private static List<Replacement> getReplacements(Class<?> klass)
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
                    addReplacement(klass, replacements, newName, newType, r);
            }
            else
            {
                for (ReplacesList replacesList : byType)
                    for (Replaces r : replacesList.value())
                        addReplacement(klass, replacements, newName, newType, r);
            }
        }
        return replacements.isEmpty() ? Collections.emptyList() : replacements;
    }

    private static void addReplacement(Class<?> klass,
                                       List<Replacement> replacements,
                                       String newName, Class<?> newType,
                                       Replaces r)
    {
        String oldName = r.oldName();

        boolean deprecated = r.deprecated();

        Class<?> oldType = r.converter().getOldType();
        if (oldType == null)
            oldType = newType;
        Class<?> expectedNewType = r.converter().getNewType();
        if (expectedNewType != null)
            assert expectedNewType.equals(newType) : String.format("Converter is expected to return %s but %s#%s expects %s", expectedNewType, klass, newName, newType);

        replacements.add(new Replacement(klass, oldName, oldType, newName, r.converter(), deprecated));
    }
}
