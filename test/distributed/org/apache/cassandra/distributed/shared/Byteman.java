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

package org.apache.cassandra.distributed.shared;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.security.CodeSigner;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.StandardSystemProperty;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.cassandra.io.util.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.byteman.agent.Transformer;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_BYTEMAN_TRANSFORMATIONS_DEBUG;

public final class Byteman
{
    private static final Logger logger = LoggerFactory.getLogger(Byteman.class);

    private static final boolean DEBUG_TRANSFORMATIONS = TEST_BYTEMAN_TRANSFORMATIONS_DEBUG.getBoolean();
    private static final Method METHOD;
    private static final URL BYTEMAN;

    static
    {
        try
        {
            Method method = ClassLoader.class.getDeclaredMethod("defineClass",
                                                                String.class, byte[].class, Integer.TYPE, Integer.TYPE,
                                                                ProtectionDomain.class);
            method.setAccessible(true);
            METHOD = method;
        }
        catch (NoSuchMethodException e)
        {
            throw new AssertionError(e);
        }

        try
        {
            // this is just to make it more clear when you inspect a class that it was created by byteman
            // the code source will show it came from byteman:// which isn't a valid java URL (hence the stream handler
            // override)
            BYTEMAN = new URL(null, "byteman://", new URLStreamHandler() {
                protected URLConnection openConnection(URL u)
                {
                    throw new UnsupportedOperationException();
                }
            });
        }
        catch (MalformedURLException e)
        {
            throw new AssertionError(e);
        }
    }

    private final Transformer transformer;
    private final List<KlassDetails> klasses;

    public static Byteman createFromScripts(String... scripts)
    {
        List<String> texts = Stream.of(scripts).map(p -> {
            try
            {
                return Files.toString(new File(p).toJavaIOFile(), StandardCharsets.UTF_8);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }).collect(Collectors.toList());

        return new Byteman(Arrays.asList(scripts), texts, extractClasses(texts));
    }

    public static Byteman createFromText(String text)
    {
        return new Byteman(Arrays.asList("invalid"), Arrays.asList(text), extractClasses(Arrays.asList(text)));
    }

    private Byteman(List<String> scripts, List<String> texts, Set<String> modifiedClassNames)
    {
        klasses = modifiedClassNames.stream().map(fullyQualifiedKlass -> {
            try
            {
                Class<?> klass = Class.forName(fullyQualifiedKlass);
                String klassPath = fullyQualifiedKlass.replace(".", "/");
                byte[] bytes = ByteStreams.toByteArray(Thread.currentThread().getContextClassLoader().getResourceAsStream(klassPath + ".class"));

                return new KlassDetails(klassPath, klass, klass.getProtectionDomain(), bytes);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        try
        {
            this.transformer = new Transformer(null, null, scripts, texts, false);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void install(ClassLoader cl)
    {
        try
        {
            for (KlassDetails details : klasses)
            {
                byte[] newBytes = transformer.transform(cl, details.klassPath, details.klass, details.protectionDomain, details.bytes);
                if (newBytes == null)
                    throw new AssertionError("Unable to transform bytes for " + details.klassPath);

                // inject the bytes into the classloader
                METHOD.invoke(cl, null, newBytes, 0, newBytes.length,
                              new ProtectionDomain(new CodeSource(BYTEMAN, new CodeSigner[0]), details.protectionDomain.getPermissions()));
                if (DEBUG_TRANSFORMATIONS)
                {
                    File f = new File(StandardSystemProperty.JAVA_IO_TMPDIR.value(), "byteman/" + details.klassPath + ".class");
                    f.parent().tryCreateDirectories();
                    File original = new File(f.parent(), "original-" + f.name());
                    logger.info("Writing class file for {} to {}", details.klassPath, f.absolutePath());
                    Files.asByteSink(f.toJavaIOFile()).write(newBytes);
                    Files.asByteSink(original.toJavaIOFile()).write(details.bytes);
                }
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Set<String> extractClasses(List<String> texts)
    {
        Pattern pattern = Pattern.compile("^CLASS (.*)$");
        Set<String> modifiedClassNames = new HashSet<>();
        for (String text : texts)
        {
            for (String line : text.split("\n"))
            {
                Matcher matcher = pattern.matcher(line);
                if (!matcher.find())
                    continue;
                modifiedClassNames.add(matcher.group(1));
            }
        }
        if (modifiedClassNames.isEmpty())
            throw new AssertionError("Unable to find any classes to modify");
        return modifiedClassNames;
    }

    private static final class KlassDetails
    {
        private final String klassPath;
        private final Class<?> klass;
        private final ProtectionDomain protectionDomain;
        private final byte[] bytes;

        public KlassDetails(String klassPath,
                            Class<?> klass, ProtectionDomain protectionDomain, byte[] bytes)
        {
            this.klassPath = klassPath;
            this.klass = klass;
            this.protectionDomain = protectionDomain;
            this.bytes = bytes;
        }
    }
}
