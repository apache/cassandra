package org.apache.cassandra;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import org.junit.Ignore;
import org.junit.runners.model.FrameworkMethod;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.reflect.Method;
import java.util.Comparator;

public class MethodComparator<T> implements Comparator<T>
{
    private static final char[] METHOD_SEPARATORS = {1, 7};

    private MethodComparator()
    {
    }

    public static MethodComparator<FrameworkMethod> getFrameworkMethodComparatorForJUnit4()
    {
        return new MethodComparator<FrameworkMethod>();
    }

    @Override
    public int compare(T o1, T o2)
    {
        final MethodPosition methodPosition1 = this.getIndexOfMethodPosition(o1);
        final MethodPosition methodPosition2 = this.getIndexOfMethodPosition(o2);
        return methodPosition1.compareTo(methodPosition2);
    }

    private MethodPosition getIndexOfMethodPosition(final Object method)
    {
        if (method instanceof FrameworkMethod)
        {
            return this.getIndexOfMethodPosition((FrameworkMethod) method);
        }
        else if (method instanceof Method)
        {
            return this.getIndexOfMethodPosition((Method) method);
        }
        else
        {
            return new NullMethodPosition();
        }
    }

    private MethodPosition getIndexOfMethodPosition(final FrameworkMethod frameworkMethod)
    {
        return getIndexOfMethodPosition(frameworkMethod.getMethod());
    }

    private MethodPosition getIndexOfMethodPosition(final Method method)
    {
        if (method.getAnnotation(Ignore.class) == null)
        {
            final Class<?> aClass = method.getDeclaringClass();
            return getIndexOfMethodPosition(aClass, method.getName());
        }
        else
        {
            return new NullMethodPosition();
        }
    }

    private MethodPosition getIndexOfMethodPosition(final Class<?> aClass, final String methodName)
    {
        MethodPosition methodPosition;
        for (final char methodSeparator : METHOD_SEPARATORS)
        {
            methodPosition = getIndexOfMethodPosition(aClass, methodName, methodSeparator);
            if (!(methodPosition instanceof NullMethodPosition))
            {
                return methodPosition;
            }
        }
        return new NullMethodPosition();
    }

    private MethodPosition getIndexOfMethodPosition(final Class<?> aClass, final String methodName, final char methodSeparator)
    {
        final InputStream inputStream = aClass.getResourceAsStream(aClass.getSimpleName() + ".class");
        final LineNumberReader lineNumberReader = new LineNumberReader(new InputStreamReader(inputStream));
        final String methodNameWithSeparator = methodName + methodSeparator;
        try
        {
            try
            {
                String line;
                while ((line = lineNumberReader.readLine()) != null)
                {
                    if (line.contains(methodNameWithSeparator))
                    {
                        return new MethodPosition(lineNumberReader.getLineNumber(), line.indexOf(methodNameWithSeparator));
                    }
                }
            }
            finally
            {
                lineNumberReader.close();
            }
        }
        catch (IOException e)
        {
            return new NullMethodPosition();
        }
        return new NullMethodPosition();
    }

    private static class MethodPosition implements Comparable<MethodPosition>
    {
        private final Integer lineNumber;
        private final Integer indexInLine;

        public MethodPosition(int lineNumber, int indexInLine)
        {
            this.lineNumber = lineNumber;
            this.indexInLine = indexInLine;
        }

        @Override
        public int compareTo(MethodPosition o)
        {

            // If line numbers are equal, then compare by indexes in this line.
            if (this.lineNumber.equals(o.lineNumber))
            {
                return this.indexInLine.compareTo(o.indexInLine);
            }
            else
            {
                return this.lineNumber.compareTo(o.lineNumber);
            }
        }
    }

    private static class NullMethodPosition extends MethodPosition
    {
        public NullMethodPosition()
        {
            super(-1, -1);
        }
    }
}
