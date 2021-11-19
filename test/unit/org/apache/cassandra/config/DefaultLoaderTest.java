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

import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.yaml.snakeyaml.introspector.FieldProperty;
import org.yaml.snakeyaml.introspector.MethodProperty;
import org.yaml.snakeyaml.introspector.Property;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultLoaderTest
{
    private static final DefaultLoader LOADER = new DefaultLoader();

    // field, no getter, no setter -> Example0
    // field, no/ignored getter, setter -> Example1 && Example5
    // field, getter, no/ignored setter -> Example3 && Example4
    // field, getter, setter -> Example2

    // no field, no getter, setter -> Example9 && Example10
    // no field, getter, no/ignored setter -> Example7 && Example8
    // no field, getter, setter -> Example6

    @Test
    public void fieldPresentWithoutGetterOrSetter()
    {
        // when the field is present and no getter, then the field property is used
        assertThat(get(Example0.class, "first")).isInstanceOf(FieldProperty.class);
    }

    @Test
    public void fieldPresentWithoutGetter()
    {
        // when the field is present and no getter, then the field property is used
        assertThat(get(Example1.class, "first")).isInstanceOf(FieldProperty.class);
        assertThat(get(Example5.class, "first")).isInstanceOf(FieldProperty.class);
    }

    @Test
    public void fieldPresentWithGetter()
    {
        // if setter/getter present, methods take precendents over field
        assertThat(get(Example2.class, "first")).isInstanceOf(MethodProperty.class);
    }

    @Test
    public void fieldPresentWithoutSetter()
    {
        assertThat(get(Example3.class, "first")).isInstanceOf(FieldProperty.class);
        assertThat(get(Example4.class, "first")).isInstanceOf(FieldProperty.class);
    }

    @Test
    public void noFieldWithGetterAndSetter()
    {
        assertThat(get(Example6.class, "first")).isInstanceOf(MethodProperty.class);
    }

    @Test
    public void noFieldWithGetterAndNoSetter()
    {
        assertThat(get(Example7.class, "first")).isNull();
        assertThat(get(Example8.class, "first")).isNull();
    }

    @Test
    public void noFieldWithoutGetterAndWithSetter()
    {
        assertThat(get(Example9.class, "first")).isInstanceOf(MethodProperty.class);
        assertThat(get(Example10.class, "first")).isInstanceOf(MethodProperty.class);
    }

    private static Property get(Class<?> klass, String name)
    {
        return LOADER.getProperties(klass).get(name);
    }

    public static class Example0
    {
        public int first;
    }

    public static class Example1
    {
        public int first;

        public void setFirst(int first)
        {
            this.first = first;
        }
    }

    public static class Example2
    {
        public int first;

        public void setFirst(int first)
        {
            this.first = first;
        }

        public int getFirst()
        {
            return first;
        }
    }

    public static class Example3
    {
        public int first;

        public int getFirst()
        {
            return first;
        }
    }

    public static class Example4
    {
        public int first;

        @JsonIgnore
        public void setFirst(int first)
        {
            this.first = first;
        }

        public int getFirst()
        {
            return first;
        }
    }

    public static class Example5
    {
        public int first;

        public void setFirst(int first)
        {
            this.first = first;
        }

        @JsonIgnore
        public int getFirst()
        {
            return first;
        }
    }

    public static class Example6
    {
        public int getFirst()
        {
            return 42;
        }

        public void setFirst(int first)
        {
            // no-op
        }
    }

    public static class Example7
    {
        public int getFirst()
        {
            return 42;
        }
    }

    public static class Example8
    {
        public int getFirst()
        {
            return 42;
        }

        @JsonIgnore
        public void setFirst(int first)
        {
            // no-op
        }
    }

    public static class Example9
    {
        public void setFirst(int first)
        {
            // no-op
        }
    }

    public static class Example10
    {
        @JsonIgnore
        public int getFirst()
        {
            return 42;
        }

        public void setFirst(int first)
        {
            // no-op
        }
    }
}