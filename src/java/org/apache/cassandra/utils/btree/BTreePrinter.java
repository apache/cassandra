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

package org.apache.cassandra.utils.btree;

import static org.apache.cassandra.utils.btree.BTree.height;
import static org.apache.cassandra.utils.btree.BTree.isEmpty;
import static org.apache.cassandra.utils.btree.BTree.isLeaf;
import static org.apache.cassandra.utils.btree.BTree.shallowSizeOfBranch;
import static org.apache.cassandra.utils.btree.BTree.size;
import static org.apache.cassandra.utils.btree.BTree.sizeMap;
import static org.apache.cassandra.utils.btree.BTree.sizeOfLeaf;

// A small utility for debugging / printing B-Tree / IntervalBTrees
//
// Prints B-Tree in the following format:
//
//       * (branch): [ 9,10:9 | 15,16:15 ]
//       ├─ [-∞]
//       │    * (branch): [ 3,4:3 | 6,7:6 ]
//       │    ├─ [-∞]
//       │    │      (leaf): [ 0,1:0, 1,2:1, 2,3:2 ]
//       │    ├─ [3,4:3]
//       │    │      (leaf): [ 4,5:4, 5,6:5 ]
//       │    └─ [6,7:6]
//       │           (leaf): [ 7,8:7, 8,9:8 ]
//       ├─ [9,10:9]
//       │    * (branch): [ 12,13:12 ]
//       │    ├─ [-∞]
//       │    │      (leaf): [ 10,11:10, 11,12:11 ]
//       │    └─ [12,13:12]
//       │           (leaf): [ 13,14:13, 14,15:14 ]
//       └─ [15,16:15]
//            * (branch): [ 18,19:18 ]
//            ├─ [-∞]
//            │      (leaf): [ 16,17:16, 17,18:17 ]
//            └─ [18,19:18]
//                   (leaf): [ 19,20:19 ]
@SuppressWarnings("unused")
public class BTreePrinter
{
    private static boolean PRINT_SIZE_MAP = false;

    public static String print(Object[] btree)
    {
        if (isEmpty(btree))
            return "empty";

        StringBuilder sb = new StringBuilder();
        sb.append("(size=").append(size(btree))
          .append(", height=").append(height(btree))
          .append("):\n");
        printNode(sb, btree, 0, "");
        return sb.toString();
    }

    private static void printNode(StringBuilder sb, Object[] node, int level, String prefix)
    {
        String indent = "  ".repeat(level);

        if (isLeaf(node))
        {
            int leafSize = sizeOfLeaf(node);
            sb.append(prefix).append(indent)
              .append("(leaf): [ ");
            for (int i = 0; i < leafSize; i++)
            {
                if (i > 0) sb.append(", ");
                sb.append(node[i]);
            }
            sb.append(" ]\n");
        }
        else
        {
            int keyCount = shallowSizeOfBranch(node);
            int childCount = keyCount + 1;
            int[] sizeMap = sizeMap(node);

            sb.append(prefix)
              .append(indent)
              .append("* (branch): [ ");

            for (int i = 0; i < keyCount; i++)
            {
                if (i > 0) sb.append(" | ");
                sb.append(node[i]);
            }
            sb.append(" ]\n");

            if (PRINT_SIZE_MAP)
            {
                sb.append(prefix).append(indent).append("├─ sizeMap: ");
                for (int i = 0; i < sizeMap.length; i++)
                {
                    if (i > 0) sb.append(", ");
                    sb.append(sizeMap[i]);
                }
                sb.append("\n");
            }

            for (int i = 0; i < childCount; i++)
            {
                Object[] child = (Object[]) node[keyCount + i];
                String childPrefix = prefix + indent;
                String verticalLine = (i == childCount - 1) ? "└─ " : "├─ ";
                String nextPrefix = (i == childCount - 1) ? "   " : "│  ";

                sb.append(childPrefix).append(verticalLine)
                  .append("[")
                  .append(i == 0 ? "-∞" : node[i - 1])
                  .append("]\n");

                // Recursing here should be fine, as we do not expect depth to be too large
                printNode(sb, child, level + 1, prefix + indent + nextPrefix);
            }
        }
    }
}
