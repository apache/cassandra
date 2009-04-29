/**
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
package org.apache.cassandra.cql.common;

import java.util.ArrayList;

//Note: This class is CQL related work in progress.
public class CType
{
    public static interface Type
    {
        String toString(); 
    };

    public static class IntegerType implements Type
    {
        public String toString() { return "Integer"; };
    }

    public static class StringType implements Type
    {
        public String toString() { return "String"; };
    }

    public static class RowType implements Type
    {
        ArrayList<Type> types_;
        public RowType(ArrayList<Type> types)
        {
            types_ = types;
        }

        public String toString()
        {
            StringBuffer sb = new StringBuffer("<");
            for (int idx = types_.size(); idx > 0; idx--)
            {
                sb.append(types_.toString());
                if (idx != 1)
                {
                    sb.append(", ");
                }
            }
            sb.append(">");
            return sb.toString();
        }
    }

    public static class ArrayType
    {
        Type elementType_;
        public ArrayType(Type elementType)
        {
            elementType_ = elementType;
        }

        public String toString()
        {
            return "Array(" + elementType_.toString() + ")";
        }
    }
}
