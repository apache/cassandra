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
package org.apache.cassandra.cql3;


public abstract class Relation {

    protected Type relationType;

    public static enum Type
    {
        EQ, LT, LTE, GTE, GT, IN, CONTAINS, CONTAINS_KEY, NEQ;

        public boolean allowsIndexQuery()
        {
            switch (this)
            {
                case EQ:
                case CONTAINS:
                case CONTAINS_KEY:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public String toString()
        {
            switch (this)
            {
                case EQ:
                    return "=";
                case LT:
                    return "<";
                case LTE:
                    return "<=";
                case GT:
                    return ">";
                case GTE:
                    return ">=";
                case NEQ:
                    return "!=";
                default:
                    return this.name();
            }
        }
    }

    public Type operator()
    {
        return relationType;
    }

    public abstract boolean isMultiColumn();
}
