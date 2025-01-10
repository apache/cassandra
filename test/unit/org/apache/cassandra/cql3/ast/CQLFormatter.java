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

package org.apache.cassandra.cql3.ast;

import static org.apache.cassandra.cql3.ast.Elements.newLine;

public interface CQLFormatter
{
    void group(StringBuilder sb);
    void endgroup(StringBuilder sb);

    void subgroup(StringBuilder sb);
    void endsubgroup(StringBuilder sb);

    void section(StringBuilder sb);
    void element(StringBuilder sb);

    class PrettyPrint implements CQLFormatter
    {
        private static final int SPACE_PER_GROUP = 2;
        private int indent;
        private int subgroup = -1;

        @Override
        public void group(StringBuilder sb)
        {
            indent += SPACE_PER_GROUP;
        }

        @Override
        public void endgroup(StringBuilder sb)
        {
            if (indent == 0)
                throw new IllegalStateException("Unable to end group; more endgroup calls than group calls");
            indent -= SPACE_PER_GROUP;
        }

        @Override
        public void subgroup(StringBuilder sb)
        {
            if (subgroup != -1)
                throw new IllegalStateException("Can not have nested subgroups");
            int offset = sb.lastIndexOf("\n");
            if (offset == -1)
                throw new IllegalStateException("subgroup called without a previous section");
            // offset is before the current indent, so this group will already account for indent
            subgroup = indent;
            indent = sb.length() - offset - 1;
        }

        @Override
        public void endsubgroup(StringBuilder sb)
        {
            // this is for Txn.LET
            indent = subgroup;
            subgroup = -1;
        }

        @Override
        public void section(StringBuilder sb)
        {
            newLine(sb, indent);
        }

        @Override
        public void element(StringBuilder sb)
        {
            newLine(sb, indent);
        }
    }

    enum None implements CQLFormatter
    {
        instance;

        @Override
        public void group(StringBuilder sb)
        {

        }

        @Override
        public void endgroup(StringBuilder sb)
        {

        }

        @Override
        public void subgroup(StringBuilder sb)
        {

        }

        @Override
        public void endsubgroup(StringBuilder sb)
        {

        }

        @Override
        public void section(StringBuilder sb)
        {
            sb.append(' ');
        }

        @Override
        public void element(StringBuilder sb)
        {
            sb.append(' ');
        }
    }
}
