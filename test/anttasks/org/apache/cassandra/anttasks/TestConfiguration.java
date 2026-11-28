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

package org.apache.cassandra.anttasks;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;

/** Reads configuration metadata without loading or initializing the test or its superclasses. */
public final class TestConfiguration
{
    private TestConfiguration()
    {
    }

    /** An empty result means the class has no configuration annotation. */
    public static List<String> declaredConfigurations(InputStream bytecode) throws IOException
    {
        List<String> names = new ArrayList<>();
        boolean[] annotated = { false };
        new ClassReader(bytecode).accept(new ClassVisitor(Opcodes.ASM9)
        {
            @Override
            public AnnotationVisitor visitAnnotation(String descriptor, boolean visible)
            {
                if (!"Lorg/apache/cassandra/TestConfigurations;".equals(descriptor))
                    return null;
                annotated[0] = true;
                return new AnnotationVisitor(Opcodes.ASM9)
                {
                    @Override
                    public AnnotationVisitor visitArray(String name)
                    {
                        return new AnnotationVisitor(Opcodes.ASM9)
                        {
                            @Override
                            public void visit(String name, Object value)
                            {
                                names.add((String) value);
                            }
                        };
                    }
                };
            }
        }, ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
        if (!annotated[0])
            return names;
        if (names.isEmpty() || new HashSet<>(names).size() != names.size())
            throw new IllegalArgumentException("TestConfigurations must contain distinct configuration names");
        names.forEach(TestConfiguration::validateName);
        return names;
    }

    private static void validateName(String name)
    {
        if (!name.matches("[a-z][a-z0-9-]*"))
            throw new IllegalArgumentException("Invalid test configuration: " + name);
    }

    public static Path yaml(Path conf, Path output, String name) throws IOException
    {
        validateName(name);
        Path base = conf.resolve("cassandra.yaml");
        if ("default".equals(name))
            return base;
        List<Path> overlays = "latest".equals(name)
                              ? Arrays.asList(conf.resolve("latest_diff.yaml"), conf.resolve("storage_compatibility_mode_none.yaml"))
                              : Arrays.asList(conf.resolve("configurations").resolve(name + ".yaml"));
        StringBuilder yaml = new StringBuilder(Files.readString(base));
        for (Path overlay : overlays)
            yaml.append('\n').append(Files.readString(overlay));
        Files.createDirectories(output);
        Path target = output.resolve("cassandra." + name + ".yaml");
        Files.writeString(target, yaml, StandardCharsets.UTF_8);
        return target;
    }
}
