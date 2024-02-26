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

package org.apache.cassandra.db.virtual.proc;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.ExecutableType;
import javax.lang.model.type.TypeKind;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Generates {@link RowWalker} implementations for {@link Column} annotated class methods. */
public class SystemViewAnnotationProcessor extends AbstractProcessor
{
    private static final Set<String> SYS_METHODS = new HashSet<>(Arrays.asList("equals", "hashCode", "toString",
            "getClass"));
    private static final String TAB = "    ";
    private static final String WALKER_SUFFIX = "Walker";
    private static final Pattern DOLLAR_PATTERN = Pattern.compile("\\$");
    private static final Map<String, Class<?>> namePrimitiveMap = new HashMap<>();
    private static final Map<Class<?>, Class<?>> primitiveWrapperMap = new HashMap<>();
    private static final Pattern CAMEL_SNAKE_PATTERN = Pattern.compile("\\B([A-Z])");

    static
    {
        namePrimitiveMap.put("boolean", Boolean.TYPE);
        namePrimitiveMap.put("byte", Byte.TYPE);
        namePrimitiveMap.put("char", Character.TYPE);
        namePrimitiveMap.put("short", Short.TYPE);
        namePrimitiveMap.put("int", Integer.TYPE);
        namePrimitiveMap.put("long", Long.TYPE);
        namePrimitiveMap.put("double", Double.TYPE);
        namePrimitiveMap.put("float", Float.TYPE);
        namePrimitiveMap.put("void", Void.TYPE);
        primitiveWrapperMap.put(Boolean.TYPE, Boolean.class);
        primitiveWrapperMap.put(Byte.TYPE, Byte.class);
        primitiveWrapperMap.put(Character.TYPE, Character.class);
        primitiveWrapperMap.put(Short.TYPE, Short.class);
        primitiveWrapperMap.put(Integer.TYPE, Integer.class);
        primitiveWrapperMap.put(Long.TYPE, Long.class);
        primitiveWrapperMap.put(Double.TYPE, Double.class);
        primitiveWrapperMap.put(Float.TYPE, Float.class);
        primitiveWrapperMap.put(Void.TYPE, Void.TYPE);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv)
    {
        for (TypeElement annotation : annotations)
        {
            Set<? extends Element> annotatedElements = roundEnv.getElementsAnnotatedWith(annotation);

            List<Element> getters = annotatedElements.stream()
                    .filter(element -> !SYS_METHODS.contains(element.getSimpleName().toString()))
                    .filter(element -> element.getModifiers().contains(javax.lang.model.element.Modifier.PUBLIC))
                    .filter(element -> ((ExecutableType) element.asType()).getReturnType().getKind() != TypeKind.VOID)
                    .filter(element -> ((ExecutableType) element.asType()).getParameterTypes().isEmpty())
                    .collect(Collectors.toList());

            if (getters.isEmpty())
                continue;

            if (getters.size() != annotatedElements.size())
            {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                        "@Column must be applied to a method without an argument and can't have void type");
                return false;
            }

            Map<String, List<Element>> gettersByClass = getters.stream()
                    .collect(Collectors.groupingBy(element -> ((TypeElement) element.getEnclosingElement()).getQualifiedName().toString()));


            for (Map.Entry<String, List<Element>> classEntry : gettersByClass.entrySet())
            {
                String className = classEntry.getKey();
                Collection<String> code = generate(className, classEntry.getValue());
                try
                {
                    JavaFileObject builderFile = processingEnv.getFiler().createSourceFile(className + WALKER_SUFFIX);
                    try (PrintWriter writer = new PrintWriter(builderFile.openWriter()))
                    {
                        for (String line : code)
                        {
                            writer.write(line);
                            writer.write('\n');
                        }
                    }
                } catch (IOException e)
                {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage());
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Generates {@link RowWalker} implementation.
     */
    private Collection<String> generate(String className, List<Element> columns)
    {
        final List<String> code = new ArrayList<>();
        final Set<String> imports = new TreeSet<>(Comparator.comparing(s -> s.replace(";", "")));
        String packageName =  className.substring(0,  className.lastIndexOf('.'));
        String simpleClassName = className.substring(className.lastIndexOf('.') + 1);

        addImport(imports, Column.class.getName());
        addImport(imports, RowWalker.class.getName());
        addImport(imports, className);

        code.add("package " + packageName + ';');
        code.add("");
        code.add("");
        code.add("/**");
        code.add(" * Generated by {@code " + SystemViewAnnotationProcessor.class.getName() + "}.");
        code.add(" * {@link " + simpleClassName + "} row metadata and data walker.");
        code.add(" * ");
        code.add(" * @see " + simpleClassName);
        code.add(" */");
        code.add("public class " + simpleClassName + WALKER_SUFFIX + " implements RowWalker<" + simpleClassName + '>');
        code.add("{");
        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public void visitMeta(RowWalker.MetadataVisitor visitor)");
        code.add(TAB + '{');

        forEachColumn(columns, (method, annotation) -> {
            String name = method.getSimpleName().toString();
            String returnType = ((ExecutableType) method.asType()).getReturnType().toString();

            if (!isPrimitive(returnType) && !returnType.startsWith("java.lang"))
                addImport(imports, returnType);

            String line = TAB + TAB +
                    "visitor.accept(" + innerClassName(Column.Type.class.getName()) + '.' + annotation.type() + ", \"" + camelToSnake(name) + "\", " +
                    getPrimitiveWrapperClass(returnType) +
                    (isPrimitive(returnType) ? ".TYPE);" : ".class);");

            code.add(line);
        });

        code.add(TAB + '}');
        code.add("");
        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public void visitRow(" + simpleClassName + " row, RowWalker.RowMetadataVisitor visitor)");
        code.add(TAB + '{');

        forEachColumn(columns, (method, annotation) -> {
            String name = method.getSimpleName().toString();
            String returnType = ((ExecutableType) method.asType()).getReturnType().toString();
            String line = TAB + TAB +
                    "visitor.accept(" + innerClassName(Column.Type.class.getName()) + '.' + annotation.type() + ", \"" + camelToSnake(name) + "\", " +
                    getPrimitiveWrapperClass(returnType) +
                    (isPrimitive(returnType) ? ".TYPE, row::" : ".class, row::") +
                    name + ");";
            code.add(line);
        });

        code.add(TAB + '}');
        code.add("");

        Map<Column.Type, AtomicInteger> countsMap = Arrays.stream(Column.Type.values())
                .collect(Collectors.toMap(e -> e, e -> new AtomicInteger()));
        forEachColumn(columns, (method, annotation) -> countsMap.get(annotation.type()).incrementAndGet());

        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override");
        code.add(TAB + "public int count(" + innerClassName(Column.Type.class.getName()) + " type)");
        code.add(TAB + '{');
        code.add(TAB + TAB + "switch (type)");
        code.add(TAB + TAB + '{');
        countsMap.forEach((key, value) -> code.add(TAB + TAB + TAB + "case " + key + ": return " + value + ';'));
        code.add(TAB + TAB + TAB +"default: throw new IllegalStateException(\"Unknown column type: \" + type);");
        code.add(TAB + TAB + '}');
        code.add(TAB + '}');
        code.add("}");

        code.addAll(2, imports);

        addLicenseHeader(code);

        return code;
    }

    private static String innerClassName(String className)
    {
        String classNameDotted = DOLLAR_PATTERN.matcher(className).replaceAll(".");
        String basicClassName = classNameDotted.substring(0, classNameDotted.lastIndexOf('.'));
        return classNameDotted.substring(basicClassName.lastIndexOf('.') + 1);
    }

    private void addImport(Set<String> imports, String className)
    {
        imports.add("import " + DOLLAR_PATTERN.matcher(className).replaceAll(".") + ';');
    }

    private void addLicenseHeader(List<String> code)
    {
        List<String> license = new ArrayList<>();

        license.add("/*");
        license.add(" * Licensed to the Apache Software Foundation (ASF) under one or more");
        license.add(" * contributor license agreements.  See the NOTICE file distributed with");
        license.add(" * this work for additional information regarding copyright ownership.");
        license.add(" * The ASF licenses this file to You under the Apache License, Version 2.0");
        license.add(" * (the \"License\"); you may not use this file except in compliance with");
        license.add(" * the License.  You may obtain a copy of the License at");
        license.add(" *");
        license.add(" *      https://www.apache.org/licenses/LICENSE-2.0");
        license.add(" *");
        license.add(" * Unless required by applicable law or agreed to in writing, software");
        license.add(" * distributed under the License is distributed on an \"AS IS\" BASIS,");
        license.add(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
        license.add(" * See the License for the specific language governing permissions and");
        license.add(" * limitations under the License.");
        license.add(" */");
        license.add("");
        code.addAll(0, license);
    }

    /**
     * Iterates each over the {@code columns} and consumes {@code method} for it.
     */
    private static void forEachColumn(List<Element> columns, BiConsumer<Element, Column> consumer)
    {
        // sort columns by type (partition key, clustering, regular) and then by column name
        columns.stream()
                .sorted(new ColumnAnnotationComparator())
                .forEach(method -> consumer.accept(method, method.getAnnotation(Column.class)));
    }

    private static class ColumnAnnotationComparator implements Comparator<Element> {
        @Override
        public int compare(Element m1, Element m2) {
            Column.Type type1 = getColumnType(m1);
            Column.Type type2 = getColumnType(m2);

            if (type1 == type2)
                return methodName(m1).compareTo(methodName(m2));

            return type1.compareTo(type2);
        }

        private static String methodName(Element method)
        {
            return method.getSimpleName().toString().toLowerCase(Locale.US);
        }

        private static Column.Type getColumnType(Element method) {
            return method.getAnnotation(Column.class).type();
        }
    }

    public static boolean isPrimitive(String className)
    {
        return namePrimitiveMap.containsKey(className);
    }

    public static String getPrimitiveWrapperClass(String className)
    {
        return isPrimitive(className) ?
                primitiveWrapperMap.get(namePrimitiveMap.get(className)).getSimpleName() : className;
    }

    private static String camelToSnake(String camelCase)
    {
        return CAMEL_SNAKE_PATTERN.matcher(camelCase).replaceAll("_$1").toLowerCase();
    }

    @Override
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.latest();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes()
    {
        return Collections.singleton("org.apache.cassandra.db.virtual.proc.Column");
    }
}
