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

package org.apache.cassandra.cql.compiler.sem;

import java.util.Map;

import org.antlr.runtime.tree.CommonTree;

import org.apache.cassandra.cql.common.*;
import org.apache.cassandra.cql.compiler.common.*;
import org.apache.cassandra.cql.compiler.parse.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql.common.ColumnMapExpr;
import org.apache.cassandra.cql.common.ColumnRangeQueryRSD;
import org.apache.cassandra.cql.common.ConstantOperand;
import org.apache.cassandra.cql.common.ExplainPlan;
import org.apache.cassandra.cql.common.OperandDef;
import org.apache.cassandra.cql.common.Pair;
import org.apache.cassandra.cql.common.Plan;
import org.apache.cassandra.cql.common.QueryPlan;
import org.apache.cassandra.cql.common.RowSourceDef;
import org.apache.cassandra.cql.common.SetColumnMap;
import org.apache.cassandra.cql.common.SetSuperColumnMap;
import org.apache.cassandra.cql.common.SetUniqueKey;
import org.apache.cassandra.cql.common.SuperColumnMapExpr;
import org.apache.cassandra.cql.common.SuperColumnRangeQueryRSD;
import org.apache.cassandra.cql.common.UniqueKeyQueryRSD;
import org.apache.cassandra.cql.common.Utils;
import org.apache.cassandra.cql.compiler.common.CompilerErrorMsg;
import org.apache.cassandra.cql.compiler.parse.CqlParser;
import org.apache.log4j.Logger;

//
// Note: This class is CQL related work in progress.
//
// Currently, this phase combines both semantic analysis and code-gen.
// I expect that as my ideas get refined/cleared up, I'll be drawing
// a more clear distinction between semantic analysis phase and code-gen.
//
public class SemanticPhase
{
    private final static Logger logger_ = Logger.getLogger(SemanticPhase.class);    

    // Current code-gen also happens in this phase!
    public static Plan doSemanticAnalysis(CommonTree ast) throws SemanticException
    {
        Plan plan = null;

        logger_.debug("AST: " + ast.toStringTree());

        switch (ast.getType())
        {
        case CqlParser.A_GET:
            plan = compileGet(ast);
            break;
        case CqlParser.A_SET:
            plan = compileSet(ast);
            break;
        case CqlParser.A_DELETE:
            compileDelete(ast);
            break;
        case CqlParser.A_SELECT:
            compileSelect(ast);
            break;
        case CqlParser.A_EXPLAIN_PLAN:
            // Case: EXPLAN PLAN <stmt>
            // first, generate a plan for <stmt>
            // and then, wrapper it with a special ExplainPlan plan
            // whose execution will result in an explain plan rather
            // than a normal execution of the statement.
            plan = doSemanticAnalysis((CommonTree)(ast.getChild(0)));
            plan = new ExplainPlan(plan);
            break;
        default:
            // Unhandled AST node. Raise an internal error. 
            throw new SemanticException(CompilerErrorMsg.INTERNAL_ERROR.getMsg(ast, "Unknown Node Type: " + ast.getType()));
        }
        return plan;
    }

    /** 
     * Given a CommonTree AST node of type, A_COLUMN_ACCESS related functions, do semantic
     * checking to ensure table name, column family name, and number of key dimensions
     * specified are all valid. 
     */
    private static CFMetaData getColumnFamilyInfo(CommonTree ast) throws SemanticException
    {
        assert(ast.getType() == CqlParser.A_COLUMN_ACCESS);

        CommonTree columnFamilyNode = (CommonTree)(ast.getChild(1)); 
        CommonTree tableNode = (CommonTree)(ast.getChild(0));

        String columnFamily = columnFamilyNode.getText();
        String table = tableNode.getText();

        Map<String, CFMetaData> columnFamilies = DatabaseDescriptor.getTableMetaData(table);
        if (columnFamilies == null)
        {
            throw new SemanticException(CompilerErrorMsg.INVALID_TABLE.getMsg(ast, table));
        }

        CFMetaData cfMetaData = columnFamilies.get(columnFamily);
        if (cfMetaData == null)
        {
            throw new SemanticException(CompilerErrorMsg.INVALID_COLUMN_FAMILY.getMsg(ast, columnFamily, table));
        }

        // Once you have drilled down to a row using a rowKey, a super column
        // map can be indexed only 2 further levels deep; and a column map may
        // be indexed up to 1 level deep.
        int dimensions = numColumnDimensions(ast);
        if (("Super".equals(cfMetaData.columnType) && (dimensions > 2)) ||
            ("Standard".equals(cfMetaData.columnType) && dimensions > 1))
        {
            throw new SemanticException(CompilerErrorMsg.TOO_MANY_DIMENSIONS.getMsg(ast, cfMetaData.columnType));
        }

        return cfMetaData; 
    }

    private static String getRowKey(CommonTree ast)
    {
        assert(ast.getType() == CqlParser.A_COLUMN_ACCESS);
        return Utils.unescapeSQLString(ast.getChild(2).getText());
    }

    private static int numColumnDimensions(CommonTree ast)
    {
        // Skip over table name, column family and rowKey
        return ast.getChildCount() - 3;
    }

    // Returns the pos'th (0-based index) column specifier in the astNode
    private static String getColumn(CommonTree ast, int pos)
    {
        // Skip over table name, column family and rowKey
        return Utils.unescapeSQLString(ast.getChild(pos + 3).getText()); 
    }

    // Compile a GET statement
    private static Plan compileGet(CommonTree ast) throws SemanticException
    {
        int childCount = ast.getChildCount();
        assert(childCount == 1);

        CommonTree columnFamilySpec = (CommonTree)ast.getChild(0);
        assert(columnFamilySpec.getType() == CqlParser.A_COLUMN_ACCESS);

        CFMetaData cfMetaData = getColumnFamilyInfo(columnFamilySpec);
        ConstantOperand rowKey = new ConstantOperand(getRowKey(columnFamilySpec));
        int dimensionCnt = numColumnDimensions(columnFamilySpec);

        RowSourceDef rwsDef;
        if ("Super".equals(cfMetaData.columnType))
        {
            if (dimensionCnt > 2)
            {
                // We don't expect this case to arise, since Cql.g grammar disallows this.
                // therefore, raise this case as an "internal error".
                throw new SemanticException(CompilerErrorMsg.INTERNAL_ERROR.getMsg(columnFamilySpec));
            }

            if (dimensionCnt == 2)
            {
                // Case: table.super_cf[<rowKey>][<superColumnKey>][<columnKey>]
                ConstantOperand superColumnKey = new ConstantOperand(getColumn(columnFamilySpec, 0));                
                ConstantOperand columnKey = new ConstantOperand(getColumn(columnFamilySpec, 1));
                rwsDef = new UniqueKeyQueryRSD(cfMetaData, rowKey, superColumnKey, columnKey);
            }
            else if (dimensionCnt == 1)
            {
                // Case: table.super_cf[<rowKey>][<superColumnKey>]
                ConstantOperand superColumnKey = new ConstantOperand(getColumn(columnFamilySpec, 0));                
                rwsDef = new ColumnRangeQueryRSD(cfMetaData, rowKey, superColumnKey, -1, Integer.MAX_VALUE);
            }
            else
            {
                // Case: table.super_cf[<rowKey>]             
                rwsDef = new SuperColumnRangeQueryRSD(cfMetaData, rowKey, -1, Integer.MAX_VALUE);
            }
        }
        else  // Standard Column Family
        {
            if (dimensionCnt == 1)
            {
                // Case: table.standard_cf[<rowKey>][<columnKey>]
                ConstantOperand columnKey = new ConstantOperand(getColumn(columnFamilySpec, 0));
                rwsDef = new UniqueKeyQueryRSD(cfMetaData, rowKey, columnKey);
            }
            else
            {
                // Case: table.standard_cf[<rowKey>]
                logger_.assertLog((dimensionCnt == 0), "invalid dimensionCnt: " + dimensionCnt);
                rwsDef = new ColumnRangeQueryRSD(cfMetaData, rowKey, -1, Integer.MAX_VALUE);
            }
        }
        return new QueryPlan(rwsDef);
    }
    
    private static OperandDef  getSimpleExpr(CommonTree ast) throws SemanticException
    {
        int type = ast.getType();

        // for now, the only simple expressions support are of string type
        if (type != CqlParser.StringLiteral)
        {
            throw new SemanticException(CompilerErrorMsg.INVALID_TYPE.getMsg(ast));
        }
        return new ConstantOperand(Utils.unescapeSQLString(ast.getText()));
    }

    private static ColumnMapExpr getColumnMapExpr(CommonTree ast) throws SemanticException
    {
        int type = ast.getType();
        if (type != CqlParser.A_COLUMN_MAP_VALUE)
        {
            throw new SemanticException(CompilerErrorMsg.INVALID_TYPE.getMsg(ast));
        }
        
        int size = ast.getChildCount();
        ColumnMapExpr result = new ColumnMapExpr();
        for (int idx = 0; idx < size; idx++)
        {
            CommonTree entryNode = (CommonTree)(ast.getChild(idx));
            OperandDef columnKey   = getSimpleExpr((CommonTree)(entryNode.getChild(0)));
            OperandDef columnValue = getSimpleExpr((CommonTree)(entryNode.getChild(1)));            

            Pair<OperandDef, OperandDef> entry = new Pair<OperandDef, OperandDef>(columnKey, columnValue);
            result.add(entry);
        }
        return result;
    }

    private static SuperColumnMapExpr getSuperColumnMapExpr(CommonTree ast) throws SemanticException
    {
        int type = ast.getType();        
        if (type != CqlParser.A_SUPERCOLUMN_MAP_VALUE)
        {
            throw new SemanticException(CompilerErrorMsg.INVALID_TYPE.getMsg(ast));
        }
        int size = ast.getChildCount();
        SuperColumnMapExpr result = new SuperColumnMapExpr();
        for (int idx = 0; idx < size; idx++)
        {
            CommonTree entryNode = (CommonTree)(ast.getChild(idx));
            OperandDef    superColumnKey = getSimpleExpr((CommonTree)(entryNode.getChild(0)));
            ColumnMapExpr columnMapExpr  = getColumnMapExpr((CommonTree)(entryNode.getChild(1)));            

            Pair<OperandDef, ColumnMapExpr> entry = new Pair<OperandDef, ColumnMapExpr>(superColumnKey, columnMapExpr);
            result.add(entry);
        }
        return result;
    }

    // compile a SET statement
    private static Plan compileSet(CommonTree ast) throws SemanticException
    {
        int childCount = ast.getChildCount();
        assert(childCount == 2);

        CommonTree columnFamilySpec = (CommonTree)ast.getChild(0);
        assert(columnFamilySpec.getType() == CqlParser.A_COLUMN_ACCESS);

        CFMetaData cfMetaData = getColumnFamilyInfo(columnFamilySpec);
        ConstantOperand rowKey = new ConstantOperand(getRowKey(columnFamilySpec));
        int dimensionCnt = numColumnDimensions(columnFamilySpec);

        CommonTree  valueNode = (CommonTree)(ast.getChild(1));

        Plan plan = null;
        if ("Super".equals(cfMetaData.columnType))
        {
            if (dimensionCnt == 2)
            {
                // Case: set table.super_cf['key']['supercolumn']['column'] = 'value'
                OperandDef value = getSimpleExpr(valueNode);
                ConstantOperand superColumnKey = new ConstantOperand(getColumn(columnFamilySpec, 0));
                ConstantOperand columnKey = new ConstantOperand(getColumn(columnFamilySpec, 1));
                plan = new SetUniqueKey(cfMetaData, rowKey, superColumnKey, columnKey, value);
            }
            else if (dimensionCnt == 1)
            {
                // Case: set table.super_cf['key']['supercolumn'] = <column_map>;
                ColumnMapExpr columnMapExpr = getColumnMapExpr(valueNode);                
                ConstantOperand superColumnKey = new ConstantOperand(getColumn(columnFamilySpec, 0));
                plan = new SetColumnMap(cfMetaData, rowKey, superColumnKey, columnMapExpr);
            }
            else
            {
                // Case: set table.super_cf['key'] = <super_column_map>;
                logger_.assertLog(dimensionCnt == 0, "invalid dimensionCnt: " + dimensionCnt);
                SuperColumnMapExpr superColumnMapExpr = getSuperColumnMapExpr(valueNode);                
                plan = new SetSuperColumnMap(cfMetaData, rowKey, superColumnMapExpr);
            }
        }
        else  // Standard column family
        {
            if (dimensionCnt == 1)
            {
                // Case: set table.standard_cf['key']['column'] = 'value'
                OperandDef value = getSimpleExpr(valueNode);                
                ConstantOperand columnKey = new ConstantOperand(getColumn(columnFamilySpec, 0));
                plan = new SetUniqueKey(cfMetaData, rowKey, columnKey, value);
            } 
            else
            {
                // Case: set table.standard_cf['key'] = <column_map>;
                logger_.assertLog(dimensionCnt == 0, "invalid dimensionCnt: " + dimensionCnt);
                ColumnMapExpr columnMapExpr = getColumnMapExpr(valueNode);                
                plan = new SetColumnMap(cfMetaData, rowKey, columnMapExpr);
            }
        }
        return plan;
    }

    private static void compileSelect(CommonTree ast) throws SemanticException
    {
        // stub; tbd.
    }
    private static void compileDelete(CommonTree ast) throws SemanticException
    {
        // stub; tbd.
    }
}