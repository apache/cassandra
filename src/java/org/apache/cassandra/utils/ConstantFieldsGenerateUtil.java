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

package org.apache.cassandra.utils;

import java.io.File; //checkstyle: permit this import
import java.io.FileWriter; //checkstyle: permit this import
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Paths; //checkstyle: permit this import
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Mutable;

/**
 * This class is used to generate constants class of provided class.
 */
public class ConstantFieldsGenerateUtil
{
    private static final Pattern COMPILE = Pattern.compile("\\.");
    private static final String CLASS_POSTFIX = "Fields";
    private static final Class<?> configClazz = Config.class;
    private static final String TAB = "    ";

    public static void main(String[] args) throws Exception
    {
        String destDir;
        if (args.length == 0)
        {
            String configPackage = configClazz.getPackage().getName();
            URL root = configClazz.getProtectionDomain().getCodeSource().getLocation();
            destDir = Paths.get(new File(root.toURI()).getParentFile().getParent(), // checkstyle: permit this instantiation
                                "src/java/",
                                COMPILE.matcher(configPackage).replaceAll("/"),
                                "/").toFile().getAbsolutePath(); // checkstyle: permit this invocation
        }
        else
            destDir = args[0];

        File configFieldsClass = Paths.get(destDir,configClazz.getSimpleName() + CLASS_POSTFIX + ".java").toFile(); // checkstyle: permit this invocation
        System.out.println("File: " + configFieldsClass.getAbsolutePath());
        if (configFieldsClass.getParentFile().mkdirs() && configFieldsClass.createNewFile())
            System.out.println("Created new file: " + configFieldsClass.getAbsolutePath());

        try (FileWriter writer = new FileWriter(configFieldsClass))
        {
            for (String line : generate(configClazz))
            {
                writer.write(line);
                writer.write('\n');
            }
        }
    }

    private static List<String> generate(Class<?> clazz)
    {
        final List<String> code = new ArrayList<>();
        final Set<String> constants = new TreeSet<>();
        code.add("");
        code.add("package " + clazz.getPackage().getName() + ';');
        code.add("");
        code.add("/**");
        code.add(" * This class is created by {@link " + ConstantFieldsGenerateUtil.class.getName() + "} based on provided");
        code.add(" * the {@link " + clazz.getCanonicalName() + "} class. It contains non-private non-static {@code Cofig}'s fields");
        code.add(" * marked with the {@link " + Mutable.class.getCanonicalName() + "} annotation to expose them to public APIs.");
        code.add("");
        code.add(" * @see " + Mutable.class.getCanonicalName());
        code.add(" * @see " + clazz.getCanonicalName());
        code.add(" */");
        code.add("public class " + clazz.getSimpleName() + CLASS_POSTFIX);
        code.add("{");
        Field[] fields = clazz.getDeclaredFields();
        Arrays.stream(fields)
              .filter(f -> !Modifier.isStatic(f.getModifiers()))
              .filter(f -> !Modifier.isPrivate(f.getModifiers()))
              .filter(f -> f.isAnnotationPresent(Mutable.class))
              .forEach(f -> constants.add(TAB + "public static final String " + f.getName().toUpperCase() + " = \"" + f.getName() + "\";"));

        code.addAll(constants);
        code.add("}");
        addLicense(code);
        return code;
    }

    /**
     * Add Apache License Header to the source colleciton of strings.
     *
     * @param code Source code.
     */
    public static void addLicense(List<String> code)
    {
        List<String> lic = new ArrayList<>();
        lic.add("/*");
        lic.add(" * Licensed to the Apache Software Foundation (ASF) under one");
        lic.add(" * or more contributor license agreements.  See the NOTICE file");
        lic.add(" * distributed with this work for additional information");
        lic.add(" * regarding copyright ownership.  The ASF licenses this file");
        lic.add(" * to you under the Apache License, Version 2.0 (the");
        lic.add(" * \"License\"); you may not use this file except in compliance");
        lic.add(" * with the License.  You may obtain a copy of the License at");
        lic.add(" *");
        lic.add(" *     http://www.apache.org/licenses/LICENSE-2.0");
        lic.add(" *");
        lic.add(" * Unless required by applicable law or agreed to in writing, software");
        lic.add(" * distributed under the License is distributed on an \"AS IS\" BASIS,");
        lic.add(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
        lic.add(" * See the License for the specific language governing permissions and");
        lic.add(" * limitations under the License.");
        lic.add(" */");
        code.addAll(0, lic);
    }
}
