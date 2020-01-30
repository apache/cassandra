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

package org.apache.cassandra.distributed.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.junit.Test;

public class TestLocator
{
    private static final String defaultOutputFileName = "run-jvm-dtests";
    private static final String testPackage = "org.apache.cassandra.distributed.test";
    private static final String testCommandFormat = "ant testsome -Dtest.name=%s -Dtest.methods=%s";

    public static void main(String[] args) throws Throwable
    {
        String outputFileName = defaultOutputFileName;
        if (args.length >= 1)
        {
            outputFileName = args[0];
        }
        String testPackage = TestLocator.testPackage;
        if (args.length == 2)
            testPackage = args[1];
        try (FileWriter fileWriter = new FileWriter(outputFileName);
             PrintWriter printWriter = new PrintWriter(fileWriter))
        {
            printWriter.println("#!/bin/bash");
            printWriter.println("ret=0");
            for (Class testClass : locateClasses(testPackage))
            {
                for (Method method : testClass.getMethods())
                {
                    if (method.getAnnotation(Test.class) == null)
                        continue;

                    printWriter.println(String.format(testCommandFormat,
                                                      testClass.getName(),
                                                      method.getName()));
                    printWriter.println("if [ $? -ne 0 ]; then ret=1; fi");
                }
            }
            printWriter.println("exit $ret");
        }
    }

    private static List<Class> locateClasses(String packageName) throws ClassNotFoundException, IOException
    {
        ClassLoader classLoader = TestLocator.class.getClassLoader();

        Enumeration<URL> resources = classLoader.getResources(packageName.replace('.', '/'));
        List<Class> classes = new ArrayList<>();
        while (resources.hasMoreElements())
        {
            URL resource = resources.nextElement();
            loadClassesRecursively(new File(resource.getFile()), packageName, classes);
        }

        return classes;
    }


    private static void loadClassesRecursively(File directory, String packageName, List<Class> classes) throws ClassNotFoundException
    {
        for (File file : directory.listFiles())
        {
            if (file.isDirectory())
                loadClassesRecursively(file, packageName + "." + file.getName(), classes);
            else if (file.getName().endsWith(".class"))
            {
                classes.add(Class.forName(packageName + '.' + file.getName().substring(0, file.getName().length() - 6)));
            }
        }
    }
}