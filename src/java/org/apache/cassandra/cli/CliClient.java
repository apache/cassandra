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
package org.apache.cassandra.cli;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.commons.lang.StringUtils;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;

import org.antlr.runtime.tree.Tree;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.thrift.TBaseHelper;
import org.apache.thrift.TException;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.Loader;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;

// Cli Client Side Library
public class CliClient
{
    /**
     * Available value conversion functions
     * Used by convertValueByFunction(Tree functionCall) method
     */
    public enum Function
    {
        BYTES         (BytesType.instance),
        INTEGER       (IntegerType.instance),
        LONG          (LongType.instance),
        INT           (Int32Type.instance),
        LEXICALUUID   (LexicalUUIDType.instance),
        TIMEUUID      (TimeUUIDType.instance),
        UTF8          (UTF8Type.instance),
        ASCII         (AsciiType.instance),
        COUNTERCOLUMN (CounterColumnType.instance);

        private AbstractType validator;

        Function(AbstractType validator)
        {
            this.validator = validator;
        }

        public AbstractType getValidator()
        {
            return this.validator;
        }

        public static String getFunctionNames()
        {
            Function[] functions = Function.values();
            StringBuilder functionNames = new StringBuilder();

            for (int i = 0; i < functions.length; i++)
            {
                StringBuilder currentName = new StringBuilder(functions[i].name().toLowerCase());
                functionNames.append(currentName.append(((i != functions.length-1) ? ", " : ".")));
            }

            return functionNames.toString();
        }
    }

    /*
     * the <i>add keyspace</i> command requires a list of arguments,
     *  this enum defines which arguments are valid
     */
    private enum AddKeyspaceArgument {
        PLACEMENT_STRATEGY,
        STRATEGY_OPTIONS,
        DURABLE_WRITES
    }

    /*
        * the <i>add column family</i> command requires a list of arguments,
        *  this enum defines which arguments are valid.
        */
    protected enum ColumnFamilyArgument
    {
        COLUMN_TYPE,
        COMPARATOR,
        SUBCOMPARATOR,
        COMMENT,
        ROWS_CACHED,
        ROW_CACHE_SAVE_PERIOD,
        ROW_CACHE_KEYS_TO_SAVE,
        KEYS_CACHED,
        KEY_CACHE_SAVE_PERIOD,
        READ_REPAIR_CHANCE,
        GC_GRACE,
        COLUMN_METADATA,
        MEMTABLE_OPERATIONS,
        MEMTABLE_THROUGHPUT,
        DEFAULT_VALIDATION_CLASS,
        MIN_COMPACTION_THRESHOLD,
        MAX_COMPACTION_THRESHOLD,
        REPLICATE_ON_WRITE,
        ROW_CACHE_PROVIDER,
        KEY_VALIDATION_CLASS,
        COMPACTION_STRATEGY,
        COMPACTION_STRATEGY_OPTIONS,
        COMPRESSION_OPTIONS,
    }

    private static final String DEFAULT_PLACEMENT_STRATEGY = "org.apache.cassandra.locator.NetworkTopologyStrategy";
    private final String NEWLINE = System.getProperty("line.separator");
    private final String TAB = "  ";

    private Cassandra.Client thriftClient = null;
    private CliSessionState sessionState  = null;
    private String keySpace = null;
    private String username = null;
    private Map<String, KsDef> keyspacesMap = new HashMap<String, KsDef>();
    private Map<String, AbstractType> cfKeysComparators;
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
    private CliUserHelp help;
    public CliClient(CliSessionState cliSessionState, Cassandra.Client thriftClient)
    {
        this.sessionState = cliSessionState;
        this.thriftClient = thriftClient;
        this.cfKeysComparators = new HashMap<String, AbstractType>();
    }

    private CliUserHelp getHelp()
    {
        if (help == null)
            help = loadHelp();
        return help;
    }

    private CliUserHelp loadHelp()
    {
        final InputStream is = CliClient.class.getClassLoader().getResourceAsStream("org/apache/cassandra/cli/CliHelp.yaml");
        assert is != null;

        try
        {
            final Constructor constructor = new Constructor(CliUserHelp.class);
            TypeDescription desc = new TypeDescription(CliUserHelp.class);
            desc.putListPropertyType("commands", CliCommandHelp.class);
            final Yaml yaml = new Yaml(new Loader(constructor));
            return (CliUserHelp) yaml.load(is);
        }
        finally
        {
            FileUtils.closeQuietly(is);
        }
    }

    public void printBanner()
    {
        sessionState.out.println("Welcome to Cassandra CLI version " + FBUtilities.getReleaseVersionString() + "\n");
        sessionState.out.println(getHelp().banner);
    }

    // Execute a CLI Statement 
    public void executeCLIStatement(String statement) throws CharacterCodingException, TException, TimedOutException, NotFoundException, NoSuchFieldException, InvalidRequestException, UnavailableException, InstantiationException, IllegalAccessException, ClassNotFoundException, SchemaDisagreementException
    {
        Tree tree = CliCompiler.compileQuery(statement);
        try
        {
            switch (tree.getType())
            {
                case CliParser.NODE_EXIT:
                    cleanupAndExit();
                    break;
                case CliParser.NODE_THRIFT_GET:
                    executeGet(tree);
                    break;
                case CliParser.NODE_THRIFT_GET_WITH_CONDITIONS:
                    executeGetWithConditions(tree);
                    break;
                case CliParser.NODE_HELP:
                    executeHelp(tree);
                    break;
                case CliParser.NODE_THRIFT_SET:
                    executeSet(tree);
                    break;
                case CliParser.NODE_THRIFT_DEL:
                    executeDelete(tree);
                    break;
                case CliParser.NODE_THRIFT_COUNT:
                    executeCount(tree);
                    break;
                case CliParser.NODE_ADD_KEYSPACE:
                    executeAddKeySpace(tree.getChild(0));
                    break;
                case CliParser.NODE_ADD_COLUMN_FAMILY:
                    executeAddColumnFamily(tree.getChild(0));
                    break;
                case CliParser.NODE_UPDATE_KEYSPACE:
                    executeUpdateKeySpace(tree.getChild(0));
                    break;
                case CliParser.NODE_UPDATE_COLUMN_FAMILY:
                    executeUpdateColumnFamily(tree.getChild(0));
                    break;
                case CliParser.NODE_DEL_COLUMN_FAMILY:
                    executeDelColumnFamily(tree);
                    break;
                case CliParser.NODE_DEL_KEYSPACE:
                    executeDelKeySpace(tree);
                    break;
                case CliParser.NODE_SHOW_CLUSTER_NAME:
                    executeShowClusterName();
                    break;
                case CliParser.NODE_SHOW_VERSION:
                    executeShowVersion();
                    break;
                case CliParser.NODE_SHOW_KEYSPACES:
                    executeShowKeySpaces();
                    break;
                case CliParser.NODE_SHOW_SCHEMA:
                    executeShowSchema(tree);
                    break;
                case CliParser.NODE_DESCRIBE:
                    executeDescribe(tree);
                    break;
                case CliParser.NODE_DESCRIBE_CLUSTER:
                    executeDescribeCluster();
                    break;
                case CliParser.NODE_USE_TABLE:
                    executeUseKeySpace(tree);
                    break;
                case CliParser.NODE_CONNECT:
                    executeConnect(tree);
                    break;
                case CliParser.NODE_LIST:
                    executeList(tree);
                    break;
                case CliParser.NODE_TRUNCATE:
                    executeTruncate(tree.getChild(0).getText());
                    break;
                case CliParser.NODE_ASSUME:
                    executeAssumeStatement(tree);
                    break;
                case CliParser.NODE_CONSISTENCY_LEVEL:
                    executeConsistencyLevelStatement(tree);
                    break;
                case CliParser.NODE_THRIFT_INCR:
                    executeIncr(tree, 1L);
                    break;
                case CliParser.NODE_THRIFT_DECR:
                    executeIncr(tree, -1L);
                    break;
                case CliParser.NODE_DROP_INDEX:
                    executeDropIndex(tree);
                    break;

                case CliParser.NODE_NO_OP:
                    // comment lines come here; they are treated as no ops.
                    break;
                default:
                    sessionState.err.println("Invalid Statement (Type: " + tree.getType() + ")");
                    if (sessionState.batch)
                        System.exit(2);
                    break;
            }
        }
        catch (SchemaDisagreementException e)
        {
            throw new RuntimeException("schema does not match across nodes, (try again later).", e);
        }
    }

    private void cleanupAndExit()
    {
        CliMain.disconnect();
        System.exit(0);
    }
    
    public KsDef getKSMetaData(String keyspace)
            throws NotFoundException, InvalidRequestException, TException
    {
        // Lazily lookup keyspace meta-data.
        if (!(keyspacesMap.containsKey(keyspace)))
            keyspacesMap.put(keyspace, thriftClient.describe_keyspace(keyspace));
        
        return keyspacesMap.get(keyspace);
    }

    private void executeHelp(Tree tree)
    {
        if (tree.getChildCount() > 0)
        {
            String token = tree.getChild(0).getText();
            for (CliCommandHelp ch : getHelp().commands)
            {
                if (token.equals(ch.name))
                {
                    sessionState.out.println(ch.help);
                    break;
                }
            }
        }
        else
        {
            sessionState.out.println(getHelp().help);
        }
    }

    private void executeCount(Tree statement)
            throws TException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        Tree columnFamilySpec = statement.getChild(0);

        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec, keyspacesMap.get(keySpace).cf_defs);
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);
       
        ColumnParent colParent = new ColumnParent(columnFamily).setSuper_column((ByteBuffer) null);
       
        if (columnSpecCnt != 0)
        {
            Tree columnTree = columnFamilySpec.getChild(2);

            byte[] superColumn = (columnTree.getType() == CliParser.FUNCTION_CALL)
                                  ? convertValueByFunction(columnTree, null, null).array()
                                  : columnNameAsByteArray(CliCompiler.getColumn(columnFamilySpec, 0), columnFamily);

            colParent = new ColumnParent(columnFamily).setSuper_column(superColumn);
        }

        SliceRange range = new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE);
        SlicePredicate predicate = new SlicePredicate().setColumn_names(null).setSlice_range(range);

        int count = thriftClient.get_count(getKeyAsBytes(columnFamily, columnFamilySpec.getChild(1)), colParent, predicate, consistencyLevel);
        sessionState.out.printf("%d columns%n", count);
    }
    
    private void executeDelete(Tree statement) 
            throws TException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        Tree columnFamilySpec = statement.getChild(0);

        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec, keyspacesMap.get(keySpace).cf_defs);
        CfDef cfDef = getCfDef(columnFamily);

        ByteBuffer key = getKeyAsBytes(columnFamily, columnFamilySpec.getChild(1));
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);

        byte[] superColumnName = null;
        byte[] columnName = null;
        boolean isSuper = cfDef.column_type.equals("Super");
     
        if ((columnSpecCnt < 0) || (columnSpecCnt > 2))
        {
            sessionState.out.println("Invalid row, super column, or column specification.");
            return;
        }

        Tree columnTree = (columnSpecCnt >= 1)
                           ? columnFamilySpec.getChild(2)
                           : null;

        Tree subColumnTree = (columnSpecCnt == 2)
                              ? columnFamilySpec.getChild(3)
                              : null;

        if (columnSpecCnt == 1)
        {
            assert columnTree != null;

            byte[] columnNameBytes = (columnTree.getType() == CliParser.FUNCTION_CALL)
                                      ? convertValueByFunction(columnTree, null, null).array()
                                      : columnNameAsByteArray(CliCompiler.getColumn(columnFamilySpec, 0), cfDef);


            if (isSuper)
                superColumnName = columnNameBytes;
            else
                columnName = columnNameBytes;
        }
        else if (columnSpecCnt == 2)
        {
            assert columnTree != null;
            assert subColumnTree != null;

            // table.cf['key']['column']['column']
            superColumnName = (columnTree.getType() == CliParser.FUNCTION_CALL)
                                      ? convertValueByFunction(columnTree, null, null).array()
                                      : columnNameAsByteArray(CliCompiler.getColumn(columnFamilySpec, 0), cfDef);

            columnName = (subColumnTree.getType() == CliParser.FUNCTION_CALL)
                                         ? convertValueByFunction(subColumnTree, null, null).array()
                                         : subColumnNameAsByteArray(CliCompiler.getColumn(columnFamilySpec, 1), cfDef);
        }

        ColumnPath path = new ColumnPath(columnFamily);
        if (superColumnName != null)
            path.setSuper_column(superColumnName);

        if (columnName != null)
            path.setColumn(columnName);

        if (isCounterCF(cfDef))
        {
            thriftClient.remove_counter(key, path, consistencyLevel);
        }
        else
        {
            thriftClient.remove(key, path, FBUtilities.timestampMicros(), consistencyLevel);
        }
        sessionState.out.println(String.format("%s removed.", (columnSpecCnt == 0) ? "row" : "column"));
    }

    private void doSlice(String keyspace, ByteBuffer key, String columnFamily, byte[] superColumnName, int limit)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException, IllegalAccessException, NotFoundException, InstantiationException, NoSuchFieldException
    {
        
        long startTime = System.currentTimeMillis();
        ColumnParent parent = new ColumnParent(columnFamily);
        if(superColumnName != null)
            parent.setSuper_column(superColumnName);

        SliceRange range = new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, limit);
        SlicePredicate predicate = new SlicePredicate().setColumn_names(null).setSlice_range(range);

        CfDef cfDef = getCfDef(columnFamily);
        boolean isSuperCF = cfDef.column_type.equals("Super");

        List<ColumnOrSuperColumn> columns = thriftClient.get_slice(key, parent, predicate, consistencyLevel);
        AbstractType validator;

        // Print out super columns or columns.
        for (ColumnOrSuperColumn cosc : columns)
        {
            if (cosc.isSetSuper_column())
            {
                SuperColumn superColumn = cosc.super_column;

                sessionState.out.printf("=> (super_column=%s,", formatColumnName(keyspace, columnFamily, superColumn.name));
                for (Column col : superColumn.getColumns())
                {
                    validator = getValidatorForValue(cfDef, col.getName());
                    sessionState.out.printf("%n     (column=%s, value=%s, timestamp=%d%s)", formatSubcolumnName(keyspace, columnFamily, col.name),
                                                    validator.getString(col.value), col.timestamp,
                                                    col.isSetTtl() ? String.format(", ttl=%d", col.getTtl()) : "");
                }
                
                sessionState.out.println(")");
            }
            else if (cosc.isSetColumn())
            {
                Column column = cosc.column;
                validator = getValidatorForValue(cfDef, column.getName());

                String formattedName = isSuperCF
                                       ? formatSubcolumnName(keyspace, columnFamily, column.name)
                                       : formatColumnName(keyspace, columnFamily, column.name);

                sessionState.out.printf("=> (column=%s, value=%s, timestamp=%d%s)%n",
                                        formattedName,
                                        validator.getString(column.value),
                                        column.timestamp,
                                        column.isSetTtl() ? String.format(", ttl=%d", column.getTtl()) : "");
            }
            else if (cosc.isSetCounter_super_column())
            {
                CounterSuperColumn superColumn = cosc.counter_super_column;

                sessionState.out.printf("=> (super_column=%s,", formatColumnName(keyspace, columnFamily, superColumn.name));
                for (CounterColumn col : superColumn.getColumns())
                {
                    sessionState.out.printf("%n     (counter=%s, value=%s)", formatSubcolumnName(keyspace, columnFamily, col.name), col.value);
                }
                sessionState.out.println(")");
            }
            else // cosc.isSetCounter_column()
            {
                CounterColumn column = cosc.counter_column;
                String formattedName = isSuperCF
                                       ? formatSubcolumnName(keyspace, columnFamily, column.name)
                                       : formatColumnName(keyspace, columnFamily, column.name);

                sessionState.out.printf("=> (counter=%s, value=%s)%n", formattedName, column.value);
            }
        }
        
        sessionState.out.println("Returned " + columns.size() + " results.");
        elapsedTime(startTime);
    }

    private AbstractType getFormatType(String compareWith)
    {
        Function function;

        try
        {
            function = Function.valueOf(compareWith.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            try
            {
                return TypeParser.parse(compareWith);
            }
            catch (ConfigurationException ce)
            {
                StringBuilder errorMessage = new StringBuilder("Unknown comparator '" + compareWith + "'. ");
                errorMessage.append("Available functions: ");
                throw new RuntimeException(errorMessage.append(Function.getFunctionNames()).toString(), e);
            }
        }

        return function.getValidator();
    }

    // Execute GET statement
    private void executeGet(Tree statement)
            throws TException, NotFoundException, InvalidRequestException, UnavailableException, TimedOutException, IllegalAccessException, InstantiationException, ClassNotFoundException, NoSuchFieldException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;
        long startTime = System.currentTimeMillis();
        Tree columnFamilySpec = statement.getChild(0);
        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec, keyspacesMap.get(keySpace).cf_defs);
        ByteBuffer key = getKeyAsBytes(columnFamily, columnFamilySpec.getChild(1));
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);
        CfDef cfDef = getCfDef(columnFamily);
        boolean isSuper = cfDef.column_type.equals("Super");
        
        byte[] superColumnName = null;
        ByteBuffer columnName;

        Tree typeTree = null;
        Tree limitTree = null;

        int limit = 1000000;

        if (statement.getChildCount() >= 2)
        {
            if (statement.getChild(1).getType() == CliParser.CONVERT_TO_TYPE)
            {
                typeTree = statement.getChild(1).getChild(0);
                if (statement.getChildCount() == 3)
                    limitTree = statement.getChild(2).getChild(0);
            }
            else
            {
                limitTree = statement.getChild(1).getChild(0);
            }
        }

        if (limitTree != null)
        {
            limit = Integer.parseInt(limitTree.getText());

            if (limit == 0)
            {
                throw new IllegalArgumentException("LIMIT should be greater than zero.");
            }
        }

        // table.cf['key'] -- row slice
        if (columnSpecCnt == 0)
        {
            doSlice(keySpace, key, columnFamily, superColumnName, limit);
            return;
        }
        // table.cf['key']['column'] -- slice of a super, or get of a standard
        else if (columnSpecCnt == 1)
        {
            columnName = getColumnName(columnFamily, columnFamilySpec.getChild(2));

            if (isSuper)
            {
                superColumnName = columnName.array();
                doSlice(keySpace, key, columnFamily, superColumnName, limit);
                return;
            }
        }
        // table.cf['key']['column']['column'] -- get of a sub-column
        else if (columnSpecCnt == 2)
        {
            superColumnName = getColumnName(columnFamily, columnFamilySpec.getChild(2)).array();
            columnName = getSubColumnName(columnFamily, columnFamilySpec.getChild(3));
        }
        // The parser groks an arbitrary number of these so it is possible to get here.
        else
        {
            sessionState.out.println("Invalid row, super column, or column specification.");
            return;
        }

        AbstractType validator = getValidatorForValue(cfDef, TBaseHelper.byteBufferToByteArray(columnName));

        // Perform a get()
        ColumnPath path = new ColumnPath(columnFamily);
        if(superColumnName != null) path.setSuper_column(superColumnName);
        path.setColumn(columnName);

        if (isCounterCF(cfDef))
        {
            doGetCounter(key, path);
            elapsedTime(startTime);
            return;
        }

        Column column;
        try
        {
            column = thriftClient.get(key, path, consistencyLevel).column;
        }
        catch (NotFoundException e)
        {
            sessionState.out.println("Value was not found");
            elapsedTime(startTime);
            return;
        }

        byte[] columnValue = column.getValue();       
        String valueAsString;

        // we have ^(CONVERT_TO_TYPE <type>) inside of GET statement
        // which means that we should try to represent byte[] value according
        // to specified type
        if (typeTree != null)
        {
            // .getText() will give us <type>
            String typeName = CliUtils.unescapeSQLString(typeTree.getText());
            // building AbstractType from <type>
            AbstractType valueValidator = getFormatType(typeName);

            // setting value for output
            valueAsString = valueValidator.getString(ByteBuffer.wrap(columnValue));
            // updating column value validator class
            updateColumnMetaData(cfDef, columnName, valueValidator.toString());
        }
        else
        {
            valueAsString = (validator == null) ? new String(columnValue, Charsets.UTF_8) : validator.getString(ByteBuffer.wrap(columnValue));
        }

        String formattedColumnName = isSuper
                                     ? formatSubcolumnName(keySpace, columnFamily, column.name)
                                     : formatColumnName(keySpace, columnFamily, column.name);

        // print results
        sessionState.out.printf("=> (column=%s, value=%s, timestamp=%d%s)%n",
                                formattedColumnName,
                                valueAsString,
                                column.timestamp,
                                column.isSetTtl() ? String.format(", ttl=%d", column.getTtl()) : "");
        elapsedTime(startTime);
    }

    private void doGetCounter(ByteBuffer key, ColumnPath path)
            throws TException, NotFoundException, InvalidRequestException, UnavailableException, TimedOutException, IllegalAccessException, InstantiationException, ClassNotFoundException, NoSuchFieldException
    {
        boolean isSuper = path.super_column != null;

        CounterColumn column;
        try
        {
            column = thriftClient.get(key, path, consistencyLevel).counter_column;
        }
        catch (NotFoundException e)
        {
            sessionState.out.println("Value was not found");
            return;
        }

        String formattedColumnName = isSuper
                                     ? formatSubcolumnName(keySpace, path.column_family, column.name)
                                     : formatColumnName(keySpace, path.column_family, column.name);

        // print results
        sessionState.out.printf("=> (counter=%s, value=%d)%n",
                                formattedColumnName,
                                column.value);
    }

    /**
     * Process get operation with conditions (using Thrift get_indexed_slices method)
     * @param statement - tree representation of the current statement
     * Format: ^(NODE_THRIFT_GET_WITH_CONDITIONS cf ^(CONDITIONS ^(CONDITION >= column1 value1) ...) ^(NODE_LIMIT int)*)
     */
    private void executeGetWithConditions(Tree statement)
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        long startTime = System.currentTimeMillis();

        IndexClause clause = new IndexClause();
        String columnFamily = CliCompiler.getColumnFamily(statement, keyspacesMap.get(keySpace).cf_defs);
        // ^(CONDITIONS ^(CONDITION $column $value) ...)
        Tree conditions = statement.getChild(1);
        
        // fetching column family definition
        CfDef columnFamilyDef = getCfDef(columnFamily);

        // fetching all columns
        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]).setFinish(new byte[0]);
        predicate.setSlice_range(sliceRange);

        for (int i = 0; i < conditions.getChildCount(); i++)
        {
            // ^(CONDITION operator $column $value)
            Tree condition = conditions.getChild(i);

            // =, >, >=, <, <=
            String operator = condition.getChild(0).getText();
            String columnNameString  = CliUtils.unescapeSQLString(condition.getChild(1).getText());
            // it could be a basic string or function call
            Tree valueTree = condition.getChild(2);

            try
            {
                ByteBuffer value;
                ByteBuffer columnName = columnNameAsBytes(columnNameString, columnFamily);

                if (valueTree.getType() == CliParser.FUNCTION_CALL)
                {
                    value = convertValueByFunction(valueTree, columnFamilyDef, columnName);
                }
                else
                {
                    String valueString = CliUtils.unescapeSQLString(valueTree.getText());
                    value = columnValueAsBytes(columnName, columnFamily, valueString);
                }

                // index operator from string
                IndexOperator idxOperator = CliUtils.getIndexOperator(operator);
                // adding new index expression into index clause
                clause.addToExpressions(new IndexExpression(columnName, idxOperator, value));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        List<KeySlice> slices;
        clause.setStart_key(new byte[] {});

        // when we have ^(NODE_LIMIT Integer)
        if (statement.getChildCount() == 3)
        {
            Tree limitNode = statement.getChild(2);
            int limitValue = Integer.parseInt(limitNode.getChild(0).getText());

            if (limitValue == 0)
            {
                throw new IllegalArgumentException("LIMIT should be greater than zero.");
            }
            
            clause.setCount(limitValue);    
        }

        try
        {
            ColumnParent parent = new ColumnParent(columnFamily);
            slices = thriftClient.get_indexed_slices(parent, clause, predicate, consistencyLevel);
            printSliceList(columnFamilyDef, slices);
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        elapsedTime(startTime);
    }

    // Execute SET statement
    private void executeSet(Tree statement)
        throws TException, InvalidRequestException, UnavailableException, TimedOutException, NoSuchFieldException, InstantiationException, IllegalAccessException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;
        
        long startTime = System.currentTimeMillis();
        // ^(NODE_COLUMN_ACCESS <cf> <key> <column>)
        Tree columnFamilySpec = statement.getChild(0);
        Tree keyTree = columnFamilySpec.getChild(1); // could be a function or regular text

        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec, keyspacesMap.get(keySpace).cf_defs);
        CfDef cfDef = getCfDef(columnFamily);
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);
        String value = CliUtils.unescapeSQLString(statement.getChild(1).getText());
        Tree valueTree = statement.getChild(1);

        byte[] superColumnName = null;
        ByteBuffer columnName;

        // table.cf['key']
        if (columnSpecCnt == 0)
        {
            sessionState.err.println("No column name specified, (type 'help;' or '?' for help on syntax).");
            return;
        }
        // table.cf['key']['column'] = 'value'
        else if (columnSpecCnt == 1)
        {
            // get the column name
            if (cfDef.column_type.equals("Super"))
            {
                sessionState.out.println("Column family " + columnFamily + " may only contain SuperColumns");
                return;
            }
            columnName = getColumnName(columnFamily, columnFamilySpec.getChild(2));
        }
        // table.cf['key']['super_column']['column'] = 'value'
        else
        {
            assert (columnSpecCnt == 2) : "serious parsing error (this is a bug).";

            superColumnName = getColumnName(columnFamily, columnFamilySpec.getChild(2)).array();
            columnName = getSubColumnName(columnFamily, columnFamilySpec.getChild(3));
        }

        ByteBuffer columnValueInBytes;

        switch (valueTree.getType())
        {
        case CliParser.FUNCTION_CALL:
            columnValueInBytes = convertValueByFunction(valueTree, cfDef, columnName, true);
            break;
        default:
            columnValueInBytes = columnValueAsBytes(columnName, columnFamily, value);
        }

        ColumnParent parent = new ColumnParent(columnFamily);
        if(superColumnName != null)
            parent.setSuper_column(superColumnName);

        Column columnToInsert = new Column(columnName).setValue(columnValueInBytes).setTimestamp(FBUtilities.timestampMicros());
        
        // children count = 3 mean that we have ttl in arguments
        if (statement.getChildCount() == 3)
        {
            String ttl = statement.getChild(2).getText();

            try
            {
                columnToInsert.setTtl(Integer.parseInt(ttl));
            }
            catch (NumberFormatException e)
            {
                sessionState.err.println(String.format("TTL '%s' is invalid, should be a positive integer.", ttl));
                return;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        // do the insert
        thriftClient.insert(getKeyAsBytes(columnFamily, keyTree), parent, columnToInsert, consistencyLevel);
        sessionState.out.println("Value inserted.");
        elapsedTime(startTime);
    }

    // Execute INCR statement
    private void executeIncr(Tree statement, long multiplier)
            throws TException, NotFoundException, InvalidRequestException, UnavailableException, TimedOutException, IllegalAccessException, InstantiationException, ClassNotFoundException, NoSuchFieldException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        Tree columnFamilySpec = statement.getChild(0);

        String columnFamily = CliCompiler.getColumnFamily(columnFamilySpec, keyspacesMap.get(keySpace).cf_defs);
        ByteBuffer key = getKeyAsBytes(columnFamily, columnFamilySpec.getChild(1));
        int columnSpecCnt = CliCompiler.numColumnSpecifiers(columnFamilySpec);
        CfDef cfDef = getCfDef(columnFamily);
        boolean isSuper = cfDef.column_type.equals("Super");
        
        byte[] superColumnName = null;
        ByteBuffer columnName;

        // table.cf['key']['column'] -- incr standard
        if (columnSpecCnt == 1)
        {
            columnName = getColumnName(columnFamily, columnFamilySpec.getChild(2));
        }
        // table.cf['key']['column']['column'] -- incr super
        else if (columnSpecCnt == 2)
        {
            superColumnName = getColumnName(columnFamily, columnFamilySpec.getChild(2)).array();
            columnName = getSubColumnName(columnFamily, columnFamilySpec.getChild(3));
        }
        // The parser groks an arbitrary number of these so it is possible to get here.
        else
        {
            sessionState.out.println("Invalid row, super column, or column specification.");
            return;
        }

        ColumnParent parent = new ColumnParent(columnFamily);
        if(superColumnName != null)
            parent.setSuper_column(superColumnName);

        long value = 1L;

        // children count = 3 mean that we have by in arguments
        if (statement.getChildCount() == 2)
        {
            String byValue = statement.getChild(1).getText();

            try
            {
                value = Long.parseLong(byValue);
            }
            catch (NumberFormatException e)
            {
                sessionState.err.println(String.format("'%s' is an invalid value, should be an integer.", byValue));
                return;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        CounterColumn columnToInsert = new CounterColumn(columnName, multiplier * value);

        // do the insert
        thriftClient.add(key, parent, columnToInsert, consistencyLevel);
        sessionState.out.printf("Value %s%n", multiplier < 0 ? "decremented." : "incremented.");
    }

    private void executeShowClusterName() throws TException
    {
        if (!CliMain.isConnected())
            return;
        
        sessionState.out.println(thriftClient.describe_cluster_name());
    }

    /**
     * Add a keyspace
     * @param statement - a token tree representing current statement
     */
    private void executeAddKeySpace(Tree statement)
    {

        if (!CliMain.isConnected())
            return;
        
        // first value is the keyspace name, after that it is all key=value
        String keyspaceName = statement.getChild(0).getText();
        KsDef ksDef = new KsDef(keyspaceName, DEFAULT_PLACEMENT_STRATEGY, new LinkedList<CfDef>());

        try
        {
            String mySchemaVersion = thriftClient.system_add_keyspace(updateKsDefAttributes(statement, ksDef));
            sessionState.out.println(mySchemaVersion);
            validateSchemaIsSettled(mySchemaVersion);

            keyspacesMap.put(keyspaceName, thriftClient.describe_keyspace(keyspaceName));
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }


    /**
     * Add a column family
     * @param statement - a token tree representing current statement
     */
    private void executeAddColumnFamily(Tree statement)
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        // first value is the column family name, after that it is all key=value
        CfDef cfDef = new CfDef(keySpace, CliUtils.unescapeSQLString(statement.getChild(0).getText()));

        try
        {
            String mySchemaVersion = thriftClient.system_add_column_family(updateCfDefAttributes(statement, cfDef));
            sessionState.out.println(mySchemaVersion);
            validateSchemaIsSettled(mySchemaVersion);
            keyspacesMap.put(keySpace, thriftClient.describe_keyspace(keySpace));
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Update existing keyspace identified by name
     * @param statement - tree represeting statement
     */
    private void executeUpdateKeySpace(Tree statement)
    {
        if (!CliMain.isConnected())
            return;

        try
        {
            String keyspaceName = CliCompiler.getKeySpace(statement, thriftClient.describe_keyspaces());

            KsDef currentKsDef = getKSMetaData(keyspaceName);
            KsDef updatedKsDef = updateKsDefAttributes(statement, currentKsDef);

            String mySchemaVersion = thriftClient.system_update_keyspace(updatedKsDef);
            sessionState.out.println(mySchemaVersion);
            validateSchemaIsSettled(mySchemaVersion);
            keyspacesMap.put(keyspaceName, thriftClient.describe_keyspace(keyspaceName));
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Update existing column family identified by name
     * @param statement - tree represeting statement
     */
    private void executeUpdateColumnFamily(Tree statement)
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        String cfName = CliCompiler.getColumnFamily(statement, keyspacesMap.get(keySpace).cf_defs);

        try
        {
            // request correct cfDef from the server
            CfDef cfDef = getCfDef(thriftClient.describe_keyspace(this.keySpace), cfName);

            if (cfDef == null)
                throw new RuntimeException("Column Family " + cfName + " was not found in the current keyspace.");

            String mySchemaVersion = thriftClient.system_update_column_family(updateCfDefAttributes(statement, cfDef));
            sessionState.out.println(mySchemaVersion);
            validateSchemaIsSettled(mySchemaVersion);
            keyspacesMap.put(keySpace, thriftClient.describe_keyspace(keySpace));
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Used to update keyspace definition attributes
     * @param statement - ANTRL tree representing current statement
     * @param ksDefToUpdate - keyspace definition to update
     * @return ksDef - updated keyspace definition
     */
    private KsDef updateKsDefAttributes(Tree statement, KsDef ksDefToUpdate)
    {
        KsDef ksDef = new KsDef(ksDefToUpdate);
        // server helpfully sets deprecated replication factor when it sends a KsDef back, for older clients.
        // we need to unset that on the new KsDef we create to avoid being treated as a legacy client in return.
        ksDef.unsetReplication_factor();

        // removing all column definitions - thrift system_update_keyspace method requires that 
        ksDef.setCf_defs(new LinkedList<CfDef>());
        
        for(int i = 1; i < statement.getChildCount(); i += 2)
        {
            String currentStatement = statement.getChild(i).getText().toUpperCase();
            AddKeyspaceArgument mArgument = AddKeyspaceArgument.valueOf(currentStatement);
            String mValue = statement.getChild(i + 1).getText();

            switch(mArgument)
            {
            case PLACEMENT_STRATEGY: 
                ksDef.setStrategy_class(CliUtils.unescapeSQLString(mValue));
                break;
            case STRATEGY_OPTIONS:
                ksDef.setStrategy_options(getStrategyOptionsFromTree(statement.getChild(i + 1)));
                break;
            case DURABLE_WRITES:
                ksDef.setDurable_writes(Boolean.parseBoolean(mValue));
                break;
            default:
                //must match one of the above or we'd throw an exception at the valueOf statement above.
                assert(false);
            }
        }

        // using default snitch options if strategy is NetworkTopologyStrategy and no options were set.
        if (ksDef.getStrategy_class().contains(".NetworkTopologyStrategy"))
        {
            Map<String, String> currentStrategyOptions = ksDef.getStrategy_options();

            // adding default data center from SimpleSnitch
            if (currentStrategyOptions == null || currentStrategyOptions.isEmpty())
            {
                SimpleSnitch snitch = new SimpleSnitch();
                Map<String, String> options = new HashMap<String, String>();

                try
                {
                    options.put(snitch.getDatacenter(InetAddress.getLocalHost()), "1");
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }

                ksDef.setStrategy_options(options);
            }
        }

        return ksDef;
    }
    
    /**
     * Update column family definition attributes
     * @param statement - ANTLR tree representing current statement
     * @param cfDefToUpdate - column family definition to apply updates on
     * @return cfDef - updated column family definition
     */
    private CfDef updateCfDefAttributes(Tree statement, CfDef cfDefToUpdate)
    {
        CfDef cfDef = new CfDef(cfDefToUpdate);

        for (int i = 1; i < statement.getChildCount(); i += 2)
        {
            String currentArgument = statement.getChild(i).getText().toUpperCase();
            ColumnFamilyArgument mArgument = ColumnFamilyArgument.valueOf(currentArgument);
            String mValue = statement.getChild(i + 1).getText();

            switch(mArgument)
            {
            case COLUMN_TYPE:
                cfDef.setColumn_type(CliUtils.unescapeSQLString(mValue));
                break;
            case COMPARATOR:
                cfDef.setComparator_type(CliUtils.unescapeSQLString(mValue));
                break;
            case SUBCOMPARATOR:
                cfDef.setSubcomparator_type(CliUtils.unescapeSQLString(mValue));
                break;
            case COMMENT:
                cfDef.setComment(CliUtils.unescapeSQLString(mValue));
                break;
            case ROWS_CACHED:
                cfDef.setRow_cache_size(Double.parseDouble(mValue));
                break;
            case KEYS_CACHED:
                cfDef.setKey_cache_size(Double.parseDouble(mValue));
                break;
            case READ_REPAIR_CHANCE:
                double chance = Double.parseDouble(mValue);

                if (chance < 0 || chance > 1)
                    throw new RuntimeException("Error: read_repair_chance must be between 0 and 1.");

                cfDef.setRead_repair_chance(chance);
                break;
            case GC_GRACE:
                cfDef.setGc_grace_seconds(Integer.parseInt(mValue));
                break;
            case COLUMN_METADATA:
                Tree arrayOfMetaAttributes = statement.getChild(i + 1);
                if (!arrayOfMetaAttributes.getText().equals("ARRAY"))
                    throw new RuntimeException("'column_metadata' format - [{ k:v, k:v, ..}, { ... }, ...]");
                cfDef.setColumn_metadata(getCFColumnMetaFromTree(cfDef, arrayOfMetaAttributes));
                break;
            case MEMTABLE_OPERATIONS:
                break;
            case MEMTABLE_THROUGHPUT:
                break;
            case ROW_CACHE_SAVE_PERIOD:
                cfDef.setRow_cache_save_period_in_seconds(Integer.parseInt(mValue));
                break;
            case KEY_CACHE_SAVE_PERIOD:
                cfDef.setKey_cache_save_period_in_seconds(Integer.parseInt(mValue));
                break;
            case ROW_CACHE_KEYS_TO_SAVE:
                cfDef.setRow_cache_keys_to_save(Integer.parseInt(mValue));
                break;
            case DEFAULT_VALIDATION_CLASS:
                cfDef.setDefault_validation_class(CliUtils.unescapeSQLString(mValue));
                break;
            case MIN_COMPACTION_THRESHOLD:
                cfDef.setMin_compaction_threshold(Integer.parseInt(mValue));
                break;
            case MAX_COMPACTION_THRESHOLD:
                cfDef.setMax_compaction_threshold(Integer.parseInt(mValue));
                break;
            case REPLICATE_ON_WRITE:
                cfDef.setReplicate_on_write(Boolean.parseBoolean(mValue));
                break;
            case ROW_CACHE_PROVIDER:
                cfDef.setRow_cache_provider(CliUtils.unescapeSQLString(mValue));
                break;
            case KEY_VALIDATION_CLASS:
                cfDef.setKey_validation_class(CliUtils.unescapeSQLString(mValue));
                break;
            case COMPACTION_STRATEGY:
                cfDef.setCompaction_strategy(CliUtils.unescapeSQLString(mValue));
                break;
            case COMPACTION_STRATEGY_OPTIONS:
                cfDef.setCompaction_strategy_options(getStrategyOptionsFromTree(statement.getChild(i+1)));
                break;
            case COMPRESSION_OPTIONS:
                cfDef.setCompression_options(getStrategyOptionsFromTree(statement.getChild(i+1)));
                break;
            default:
                //must match one of the above or we'd throw an exception at the valueOf statement above.
                assert(false);

            }
        }

        return cfDef;
    }

    /**
     * Delete a keyspace
     * @param statement - a token tree representing current statement
     * @throws TException - exception
     * @throws InvalidRequestException - exception
     * @throws NotFoundException - exception
     * @throws SchemaDisagreementException 
     */
    private void executeDelKeySpace(Tree statement)
            throws TException, InvalidRequestException, NotFoundException, SchemaDisagreementException
    {
        if (!CliMain.isConnected())
            return;

        String keyspaceName = CliCompiler.getKeySpace(statement, thriftClient.describe_keyspaces());
        String version = thriftClient.system_drop_keyspace(keyspaceName);
        sessionState.out.println(version);
        validateSchemaIsSettled(version);
       
        if (keyspaceName.equals(keySpace)) //we just deleted the keyspace we were authenticated too
            keySpace = null;
    }

    /**
     * Delete a column family
     * @param statement - a token tree representing current statement
     * @throws TException - exception
     * @throws InvalidRequestException - exception
     * @throws NotFoundException - exception
     * @throws SchemaDisagreementException 
     */
    private void executeDelColumnFamily(Tree statement) 
            throws TException, InvalidRequestException, NotFoundException, SchemaDisagreementException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        String cfName = CliCompiler.getColumnFamily(statement, keyspacesMap.get(keySpace).cf_defs);
        String mySchemaVersion = thriftClient.system_drop_column_family(cfName);
        sessionState.out.println(mySchemaVersion);
        validateSchemaIsSettled(mySchemaVersion);
    }

    private void executeList(Tree statement)
            throws TException, InvalidRequestException, NotFoundException, IllegalAccessException, InstantiationException, NoSuchFieldException, UnavailableException, TimedOutException, CharacterCodingException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        long startTime = System.currentTimeMillis();

        // extract column family
        String columnFamily = CliCompiler.getColumnFamily(statement, keyspacesMap.get(keySpace).cf_defs);

        String rawStartKey = "";
        String rawEndKey = "";
        int limitCount = Integer.MAX_VALUE; // will reset to default later if it's not specified

        // optional arguments: key range and limit
        for (int i = 1; i < statement.getChildCount(); i++)
        {
            Tree child = statement.getChild(i);
            if (child.getType() == CliParser.NODE_KEY_RANGE)
            {
                if (child.getChildCount() > 0)
                {
                    rawStartKey = CliUtils.unescapeSQLString(child.getChild(0).getText());
                    if (child.getChildCount() > 1)
                        rawEndKey = CliUtils.unescapeSQLString(child.getChild(1).getText());
                }
            }
            else
            {
                if (child.getChildCount() != 1)
                {
                    sessionState.out.println("Invalid limit clause");
                    return;
                }
                limitCount = Integer.parseInt(child.getChild(0).getText());
                if (limitCount <= 0)
                {
                    sessionState.out.println("Invalid limit " + limitCount);
                    return;
                }
            }
        }

        if (limitCount == Integer.MAX_VALUE)
        {
            limitCount = 100;
            sessionState.out.println("Using default limit of 100");
        }

        CfDef columnFamilyDef = getCfDef(columnFamily);

        // read all columns and superColumns
        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]).setFinish(new byte[0]);
        sliceRange.setCount(Integer.MAX_VALUE);
        predicate.setSlice_range(sliceRange);

        // set the key range
        KeyRange range = new KeyRange(limitCount);
        AbstractType keyComparator = this.cfKeysComparators.get(columnFamily);
        ByteBuffer startKey = rawStartKey.isEmpty() ? ByteBufferUtil.EMPTY_BYTE_BUFFER : getBytesAccordingToType(rawStartKey, keyComparator);
        ByteBuffer endKey = rawEndKey.isEmpty() ? ByteBufferUtil.EMPTY_BYTE_BUFFER : getBytesAccordingToType(rawEndKey, keyComparator);
        range.setStart_key(startKey).setEnd_key(endKey);

        ColumnParent columnParent = new ColumnParent(columnFamily);
        List<KeySlice> keySlices = thriftClient.get_range_slices(columnParent, predicate, range, consistencyLevel);
        printSliceList(columnFamilyDef, keySlices);
        elapsedTime(startTime);
    }

    // DROP INDEX ON <CF>.<COLUMN>
    private void executeDropIndex(Tree statement) throws TException, SchemaDisagreementException, InvalidRequestException, NotFoundException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        // getColumnFamily will check if CF exists for us
        String columnFamily = CliCompiler.getColumnFamily(statement, keyspacesMap.get(keySpace).cf_defs);
        String rawColumName = CliUtils.unescapeSQLString(statement.getChild(1).getText());

        CfDef cfDef = getCfDef(columnFamily);

        ByteBuffer columnName = columnNameAsBytes(rawColumName, cfDef);

        boolean foundColumn = false;

        for (ColumnDef column : cfDef.getColumn_metadata())
        {
            if (column.name.equals(columnName))
            {
                foundColumn = true;

                if (column.getIndex_type() == null)
                    throw new RuntimeException(String.format("Column '%s' does not have an index.", rawColumName));

                column.setIndex_name(null);
                column.setIndex_type(null);
            }
        }

        if (!foundColumn)
            throw new RuntimeException(String.format("Column '%s' definition was not found in ColumnFamily '%s'.",
                                                     rawColumName,
                                                     columnFamily));

        String mySchemaVersion = thriftClient.system_update_column_family(cfDef);
        sessionState.out.println(mySchemaVersion);
        validateSchemaIsSettled(mySchemaVersion);
        keyspacesMap.put(keySpace, thriftClient.describe_keyspace(keySpace));
    }

    // TRUNCATE <columnFamily>
    private void executeTruncate(String columnFamily) throws TException, InvalidRequestException, UnavailableException
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        // getting CfDef, it will fail if there is no such column family in current keySpace. 
        CfDef cfDef = getCfDef(CliCompiler.getColumnFamily(columnFamily, keyspacesMap.get(keySpace).cf_defs));

        thriftClient.truncate(cfDef.getName());
        sessionState.out.println(columnFamily + " truncated.");
    }

    /**
     * Command: CONSISTENCYLEVEL AS (ONE | QUORUM ...)
     * Tree: ^(NODE_CONSISTENCY_LEVEL AS (ONE | QUORUM ...))
     * @param statement - tree representing current statement
     */
    private void executeConsistencyLevelStatement(Tree statement)
    {
        if (!CliMain.isConnected())
            return;

        String userSuppliedLevel = statement.getChild(0).getText().toUpperCase();

        try
        {
            consistencyLevel = ConsistencyLevel.valueOf(userSuppliedLevel);
        }
        catch (IllegalArgumentException e)
        {
            String elements = "ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, ANY";
            sessionState.out.println(String.format("'%s' is invalid. Available: %s", userSuppliedLevel, elements));
            return;
        }

        sessionState.out.println(String.format("Consistency level is set to '%s'.", consistencyLevel));
    }

    /**
     * Command: ASSUME <columnFamily> (VALIDATOR | COMPARATOR | KEYS | SUB_COMPARATOR) AS <type>
     * Tree: ^(NODE_ASSUME <columnFamily> (VALIDATOR | COMPARATOR | KEYS | SUB_COMPARATOR) <type>))
     * @param statement - tree representing current statement
     */
    private void executeAssumeStatement(Tree statement)
    {
        if (!CliMain.isConnected() || !hasKeySpace())
            return;

        String cfName = CliCompiler.getColumnFamily(statement, keyspacesMap.get(keySpace).cf_defs);
        CfDef columnFamily = getCfDef(cfName);

        // VALIDATOR | COMPARATOR | KEYS | SUB_COMPARATOR
        String assumptionElement = statement.getChild(1).getText().toUpperCase();
        // used to store in this.cfKeysComparator
        AbstractType comparator;

        // Could be UTF8Type, IntegerType, LexicalUUIDType etc.
        String defaultType = statement.getChild(2).getText();

        try
        {
            comparator = Function.valueOf(defaultType.toUpperCase()).getValidator();
        }
        catch (Exception e)
        {
            String functions = Function.getFunctionNames();
            sessionState.out.println("Type '" + defaultType + "' was not found. Available: " + functions);
            return;
        }

        // making string representation look property e.g. o.a.c.db.marshal.UTF8Type
        defaultType = comparator.getClass().getName();

        if (assumptionElement.equals("COMPARATOR"))
        {
            columnFamily.setComparator_type(defaultType);
        }
        else if (assumptionElement.equals("SUB_COMPARATOR"))
        {
            columnFamily.setSubcomparator_type(defaultType);
        }
        else if (assumptionElement.equals("VALIDATOR"))
        {
            columnFamily.setDefault_validation_class(defaultType);
        }
        else if (assumptionElement.equals("KEYS"))
        {
            this.cfKeysComparators.put(columnFamily.getName(), comparator);
        }
        else
        {
            String elements = "VALIDATOR, COMPARATOR, KEYS, SUB_COMPARATOR.";
            sessionState.out.println(String.format("'%s' is invalid. Available: %s", assumptionElement, elements));
            return;
        }

        sessionState.out.println(String.format("Assumption for column family '%s' added successfully.", columnFamily.getName()));
    }

    // SHOW API VERSION
    private void executeShowVersion() throws TException
    {
        if (!CliMain.isConnected())
            return;
        
        sessionState.out.println(thriftClient.describe_version());
    }

    // SHOW KEYSPACES
    private void executeShowKeySpaces() throws TException, InvalidRequestException
    {
        if (!CliMain.isConnected())
            return;

        List<KsDef> keySpaces = thriftClient.describe_keyspaces();

        Collections.sort(keySpaces, new KsDefNamesComparator());
        for (KsDef keySpace : keySpaces)
        {
            describeKeySpace(keySpace.name, keySpace);
        }
    }

    // SHOW SCHEMA
    private void executeShowSchema(Tree statement) throws TException, InvalidRequestException
    {
        if (!CliMain.isConnected())
            return;

        final List<KsDef> keyspaces = thriftClient.describe_keyspaces();
        Collections.sort(keyspaces, new KsDefNamesComparator());
        final String keyspaceName = (statement.getChildCount() == 0)
                                ? keySpace
                                : CliCompiler.getKeySpace(statement, keyspaces);

        Iterator<KsDef> ksIter;
        if (keyspaceName != null)
            ksIter = Collections2.filter(keyspaces, new Predicate<KsDef>()
            {
                public boolean apply(KsDef ksDef)
                {
                    return keyspaceName.equals(ksDef.name);
                }
            }).iterator();
        else
            ksIter = keyspaces.iterator();


        final StringBuilder sb = new StringBuilder();
        while (ksIter.hasNext())
            showKeyspace(sb, ksIter.next());

        sessionState.out.printf(sb.toString());
    }

    /**
     * Creates a CLI script to create the Keyspace it's Column Families
     * @param sb StringBuilder to write to.
     * @param ksDef KsDef to create the cli script for.
     */
    private void showKeyspace(StringBuilder sb, KsDef ksDef)
    {
        sb.append("create keyspace " + ksDef.name);

        writeAttr(sb, true, "placement_strategy", normaliseType(ksDef.strategy_class, "org.apache.cassandra.locator"));

        if (ksDef.strategy_options != null && !ksDef.strategy_options.isEmpty())
        {
            final StringBuilder opts = new StringBuilder();
            opts.append("{");
            String prefix = "";
            for (Map.Entry<String, String> opt : ksDef.strategy_options.entrySet())
            {
                opts.append(prefix + CliUtils.escapeSQLString(opt.getKey()) + " : " + CliUtils.escapeSQLString(opt.getValue()));
                prefix = ", ";
            }
            opts.append("}");
            writeAttrRaw(sb, false, "strategy_options", opts.toString());
        }

        writeAttr(sb, false, "durable_writes", ksDef.durable_writes);

        sb.append(";" + NEWLINE);
        sb.append(NEWLINE);

        sb.append("use " + ksDef.name + ";");
        sb.append(NEWLINE);
        sb.append(NEWLINE);

        Collections.sort(ksDef.cf_defs, new CfDefNamesComparator());
        for (CfDef cfDef : ksDef.cf_defs)
            showColumnFamily(sb, cfDef);
        sb.append(NEWLINE);
        sb.append(NEWLINE);
    }

    /**
     * Creates a CLI script for the CfDef including meta data to the supplied StringBuilder.
     * @param sb
     * @param cfDef
     */
    private void showColumnFamily(StringBuilder sb, CfDef cfDef)
    {
        sb.append("create column family " + CliUtils.escapeSQLString(cfDef.name));

        writeAttr(sb, true, "column_type", cfDef.column_type);
        writeAttr(sb, false, "comparator", normaliseType(cfDef.comparator_type, "org.apache.cassandra.db.marshal"));
        if (cfDef.column_type.equals("Super"))
            writeAttr(sb, false, "subcomparator", normaliseType(cfDef.subcomparator_type, "org.apache.cassandra.db.marshal"));
        if (!StringUtils.isEmpty(cfDef.default_validation_class))
            writeAttr(sb, false, "default_validation_class",
                        normaliseType(cfDef.default_validation_class, "org.apache.cassandra.db.marshal"));
        writeAttr(sb, false, "key_validation_class",
                    normaliseType(cfDef.key_validation_class, "org.apache.cassandra.db.marshal"));
        writeAttr(sb, false, "rows_cached", cfDef.row_cache_size);
        writeAttr(sb, false, "row_cache_save_period", cfDef.row_cache_save_period_in_seconds);
        writeAttr(sb, false, "row_cache_keys_to_save", cfDef.row_cache_keys_to_save);
        writeAttr(sb, false, "keys_cached", cfDef.key_cache_size);
        writeAttr(sb, false, "key_cache_save_period", cfDef.key_cache_save_period_in_seconds);
        writeAttr(sb, false, "read_repair_chance", cfDef.read_repair_chance);
        writeAttr(sb, false, "gc_grace", cfDef.gc_grace_seconds);
        writeAttr(sb, false, "min_compaction_threshold", cfDef.min_compaction_threshold);
        writeAttr(sb, false, "max_compaction_threshold", cfDef.max_compaction_threshold);
        writeAttr(sb, false, "replicate_on_write", cfDef.replicate_on_write);
        writeAttr(sb, false, "row_cache_provider", normaliseType(cfDef.row_cache_provider, "org.apache.cassandra.cache"));
        writeAttr(sb, false, "compaction_strategy", cfDef.compaction_strategy);

        if (!cfDef.compaction_strategy_options.isEmpty())
        {
            StringBuilder cOptions = new StringBuilder();

            cOptions.append("{");

            Map<String, String> options = cfDef.compaction_strategy_options;

            int i = 0, size = options.size();

            for (Map.Entry<String, String> entry : options.entrySet())
            {
                cOptions.append(CliUtils.quote(entry.getKey())).append(" : ").append(CliUtils.quote(entry.getValue()));

                if (i != size - 1)
                    cOptions.append(", ");

                i++;
            }

            cOptions.append("}");

            writeAttrRaw(sb, false, "compaction_strategy_options", cOptions.toString());
        }

        if (!StringUtils.isEmpty(cfDef.comment))
            writeAttr(sb, false, "comment", cfDef.comment);

        if (!cfDef.column_metadata.isEmpty())
        {
            StringBuilder colSb = new StringBuilder();
            colSb.append("[");
            boolean first = true;
            for (ColumnDef colDef : cfDef.column_metadata)
            {
                if (!first)
                    colSb.append(",");
                first = false;
                showColumnMeta(colSb, cfDef, colDef);
            }
            colSb.append("]");
            writeAttrRaw(sb, false, "column_metadata", colSb.toString());
        }

        if (cfDef.compression_options != null && !cfDef.compression_options.isEmpty())
        {
            StringBuilder compOptions = new StringBuilder();

            compOptions.append("{");

            int i = 0, size = cfDef.compression_options.size();

            for (Map.Entry<String, String> entry : cfDef.compression_options.entrySet())
            {
                compOptions.append(CliUtils.quote(entry.getKey())).append(" : ").append(CliUtils.quote(entry.getValue()));

                if (i != size - 1)
                    compOptions.append(", ");

                i++;
            }

            compOptions.append("}");

            writeAttrRaw(sb, false, "compression_options", compOptions.toString());
        }

        sb.append(";");
        sb.append(NEWLINE);
        sb.append(NEWLINE);
    }

    /**
     * Writes the supplied ColumnDef to the StringBuilder as a cli script.
     * @param sb
     * @param cfDef
     * @param colDef
     */
    private void showColumnMeta(StringBuilder sb, CfDef cfDef, ColumnDef colDef)
    {
        sb.append(NEWLINE + TAB + TAB + "{");

        final AbstractType comparator = getFormatType(cfDef.column_type.equals("Super")
                                                      ? cfDef.subcomparator_type
                                                      : cfDef.comparator_type);
        sb.append("column_name : '" + CliUtils.escapeSQLString(comparator.getString(colDef.name)) + "'," + NEWLINE);
        String validationClass = normaliseType(colDef.validation_class, "org.apache.cassandra.db.marshal");
        sb.append(TAB + TAB + "validation_class : " + CliUtils.escapeSQLString(validationClass));
        if (colDef.isSetIndex_name())
        {
            sb.append("," + NEWLINE);
            sb.append(TAB + TAB + "index_name : '" + CliUtils.escapeSQLString(colDef.index_name) + "'," + NEWLINE);
            sb.append(TAB + TAB + "index_type : " + CliUtils.escapeSQLString(Integer.toString(colDef.index_type.getValue())) + "," + NEWLINE);

            if (colDef.index_options != null)
            {
                sb.append(TAB + TAB + "index_options : {"+NEWLINE);        
                for (Map.Entry<String, String> entry : colDef.index_options.entrySet())
                {
                    sb.append(TAB + TAB + TAB + CliUtils.escapeSQLString(entry.getKey()) + ": '" + CliUtils.escapeSQLString(entry.getValue()) + "'," + NEWLINE);
                }
                sb.append("}");
            }
        }
        sb.append("}");
    }

    private String normaliseType(String path, String expectedPackage)
    {
        if (path.startsWith(expectedPackage))
            return path.substring(expectedPackage.length() + 1);

        return path;
    }

    private void writeAttr(StringBuilder sb, boolean first, String name, Boolean value)
    {
        writeAttrRaw(sb, first, name, value.toString());
    }
    private void writeAttr(StringBuilder sb, boolean first, String name, Number value)
    {
        writeAttrRaw(sb, first, name, value.toString());
    }

    private void writeAttr(StringBuilder sb, boolean first, String name, String value)
    {
        writeAttrRaw(sb, first, name, "'" + CliUtils.escapeSQLString(value) + "'");
    }

    private void writeAttrRaw(StringBuilder sb, boolean first, String name, String value)
    {
        sb.append(NEWLINE + TAB);
        sb.append(first ? "with " : "and ");
        sb.append(name + " = ");
        sb.append(value);
    }
    /**
     * Returns true if this.keySpace is set, false otherwise
     * @return boolean
     */
    private boolean hasKeySpace() 
    {
    	if (keySpace == null)
        {
            sessionState.out.println("Not authenticated to a working keyspace.");
            return false;
        }
        
        return true;
    }
    
    public String getKeySpace() 
    {
        return keySpace == null ? "unknown" : keySpace;
    }
    
    public void setKeySpace(String keySpace) throws NotFoundException, InvalidRequestException, TException 
    {
        this.keySpace = keySpace;
        // We do nothing with the return value, but it hits a cache and the tab-completer.
        getKSMetaData(keySpace);
    }
    
    public String getUsername() 
    {
        return username == null ? "default" : username;
    }
    
    public void setUsername(String username)
    {
        this.username = username;
    }

    // USE <keyspace_name>
    private void executeUseKeySpace(Tree statement) throws TException
    {
        if (!CliMain.isConnected())
            return;

        int childCount = statement.getChildCount();
        String keySpaceName, username = null, password = null;

        // Get keyspace name
        keySpaceName = CliUtils.unescapeSQLString(statement.getChild(0).getText());

        if (childCount == 3)
        {
            username  = statement.getChild(1).getText();
            password  = statement.getChild(2).getText();
        }
        
        if (keySpaceName == null)
        {
            sessionState.out.println("Keyspace argument required");
            return;
        }
        
        try 
        {
        	AuthenticationRequest authRequest;
        	Map<String, String> credentials = new HashMap<String, String>();

            keySpaceName = CliCompiler.getKeySpace(keySpaceName, thriftClient.describe_keyspaces());

            thriftClient.set_keyspace(keySpaceName);

        	if (username != null && password != null) 
        	{
        	    /* remove quotes */
        	    password = password.replace("\'", "");
        	    credentials.put(IAuthenticator.USERNAME_KEY, username);
                credentials.put(IAuthenticator.PASSWORD_KEY, password);
                authRequest = new AuthenticationRequest(credentials);
                thriftClient.login(authRequest);
        	}
        	
            keySpace = keySpaceName;
            this.username = username != null ? username : "default";
            
            CliMain.updateCompletor(CliUtils.getCfNamesByKeySpace(getKSMetaData(keySpace)));
            sessionState.out.println("Authenticated to keyspace: " + keySpace);
        } 
        catch (AuthenticationException e) 
        {
            sessionState.err.println("Exception during authentication to the cassandra node: " +
            		                 "verify keyspace exists, and you are using correct credentials.");
        } 
        catch (AuthorizationException e) 
        {
            sessionState.err.println("You are not authorized to use keyspace: " + keySpaceName);
        }
        catch (InvalidRequestException e)
        {
            sessionState.err.println(keySpaceName + " does not exist.");
        }
        catch (NotFoundException e)
        {
            sessionState.err.println(keySpaceName + " does not exist.");
        } 
        catch (TException e) 
        {
            if (sessionState.debug)
                e.printStackTrace(sessionState.err);

            sessionState.err.println("Login failure. Did you specify 'keyspace', 'username' and 'password'?");
        }
    }

    private void describeKeySpace(String keySpaceName, KsDef metadata) throws TException
    {
        NodeProbe probe = sessionState.getNodeProbe();

        // getting compaction manager MBean to displaying index building information
        CompactionManagerMBean compactionManagerMBean = (probe == null) ? null : probe.getCompactionManagerProxy();

        // Describe and display
        sessionState.out.println("Keyspace: " + keySpaceName + ":");
        try
        {
            KsDef ks_def;
            ks_def = metadata == null ? thriftClient.describe_keyspace(keySpaceName) : metadata;
            sessionState.out.println("  Replication Strategy: " + ks_def.strategy_class);

            sessionState.out.println("  Durable Writes: " + ks_def.durable_writes);

            Map<String, String> options = ks_def.strategy_options;
            sessionState.out.println("    Options: [" + ((options == null) ? "" : FBUtilities.toString(options)) + "]");

            sessionState.out.println("  Column Families:");

            Collections.sort(ks_def.cf_defs, new CfDefNamesComparator());

            for (CfDef cf_def : ks_def.cf_defs)
                describeColumnFamily(ks_def, cf_def, probe);

            // compaction manager information
            if (compactionManagerMBean != null)
            {
                for (CompactionInfo info : compactionManagerMBean.getCompactions())
                {
                    // if ongoing compaction type is index build
                    if (info.getTaskType() != OperationType.INDEX_BUILD)
                        continue;
                    sessionState.out.printf("%nCurrently building index %s, completed %d of %d bytes.%n",
                                            info.getColumnFamily(),
                                            info.getBytesComplete(),
                                            info.getTotalBytes());
                }
            }

            // closing JMX connection
            if (probe != null)
                probe.close();
        }
        catch (InvalidRequestException e)
        {
            sessionState.out.println("Invalid request: " + e);
        }
        catch (NotFoundException e)
        {
            sessionState.out.println("Keyspace " + keySpaceName + " could not be found.");
        }
        catch (IOException e)
        {
            sessionState.out.println("Error while closing JMX connection: " + e.getMessage());
        }
    }

    private void describeColumnFamily(KsDef ks_def, CfDef cf_def, NodeProbe probe) throws TException
    {
        // fetching bean for current column family store
        ColumnFamilyStoreMBean cfMBean = (probe == null) ? null : probe.getCfsProxy(ks_def.getName(), cf_def.getName());

        boolean isSuper = cf_def.column_type.equals("Super");
        sessionState.out.printf("    ColumnFamily: %s%s%n", cf_def.name, isSuper ? " (Super)" : "");

        if (cf_def.comment != null && !cf_def.comment.isEmpty())
            sessionState.out.printf("    \"%s\"%n", cf_def.comment);

        if (cf_def.key_validation_class != null)
            sessionState.out.printf("      Key Validation Class: %s%n", cf_def.key_validation_class);

        if (cf_def.default_validation_class != null)
            sessionState.out.printf("      Default column value validator: %s%n", cf_def.default_validation_class);

        sessionState.out.printf("      Columns sorted by: %s%s%n", cf_def.comparator_type, cf_def.column_type.equals("Super") ? "/" + cf_def.subcomparator_type : "");
        sessionState.out.printf("      Row cache size / save period in seconds / keys to save : %s/%s/%s%n",
                cf_def.row_cache_size, cf_def.row_cache_save_period_in_seconds,
                cf_def.row_cache_keys_to_save == Integer.MAX_VALUE ? "all" : cf_def.row_cache_keys_to_save);
        sessionState.out.printf("      Row Cache Provider: %s%n", cf_def.getRow_cache_provider());
        sessionState.out.printf("      Key cache size / save period in seconds: %s/%s%n", cf_def.key_cache_size, cf_def.key_cache_save_period_in_seconds);
        sessionState.out.printf("      GC grace seconds: %s%n", cf_def.gc_grace_seconds);
        sessionState.out.printf("      Compaction min/max thresholds: %s/%s%n", cf_def.min_compaction_threshold, cf_def.max_compaction_threshold);
        sessionState.out.printf("      Read repair chance: %s%n", cf_def.read_repair_chance);
        sessionState.out.printf("      Replicate on write: %s%n", cf_def.replicate_on_write);

        // if we have connection to the cfMBean established
        if (cfMBean != null)
            sessionState.out.printf("      Built indexes: %s%n", cfMBean.getBuiltIndexes());

        if (cf_def.getColumn_metadataSize() != 0)
        {
            String leftSpace = "      ";
            String columnLeftSpace = leftSpace + "    ";

            String compareWith = isSuper ? cf_def.subcomparator_type
                    : cf_def.comparator_type;
            AbstractType columnNameValidator = getFormatType(compareWith);

            sessionState.out.println(leftSpace + "Column Metadata:");
            for (ColumnDef columnDef : cf_def.getColumn_metadata())
            {
                String columnName = columnNameValidator.getString(columnDef.name);
                if (columnNameValidator instanceof BytesType)
                {
                    try
                    {
                        String columnString = UTF8Type.instance.getString(columnDef.name);
                        columnName = columnString + " (" + columnName + ")";
                    }
                    catch (MarshalException e)
                    {
                        // guess it wasn't a utf8 column name after all
                    }
                }

                sessionState.out.println(leftSpace + "  Column Name: " + columnName);
                sessionState.out.println(columnLeftSpace + "Validation Class: " + columnDef.getValidation_class());

                if (columnDef.isSetIndex_name())
                    sessionState.out.println(columnLeftSpace + "Index Name: " + columnDef.getIndex_name());

                if (columnDef.isSetIndex_type())
                    sessionState.out.println(columnLeftSpace + "Index Type: " + columnDef.getIndex_type().name());
            }
        }

        sessionState.out.printf("      Compaction Strategy: %s%n", cf_def.compaction_strategy);

        if (!cf_def.compaction_strategy_options.isEmpty())
        {
            sessionState.out.println("      Compaction Strategy Options:");
            for (Map.Entry<String, String> e : cf_def.compaction_strategy_options.entrySet())
                sessionState.out.printf("        %s: %s%n", e.getKey(), e.getValue());
        }

        if (cf_def.compression_options != null && !cf_def.compression_options.isEmpty())
        {
            sessionState.out.println("      Compression Options:");
            for (Map.Entry<String, String> e : cf_def.compression_options.entrySet())
                sessionState.out.printf("        %s: %s%n", e.getKey(), e.getValue());
        }
    }

    // DESCRIBE KEYSPACE (<keyspace> | <column_family>)?
    private void executeDescribe(Tree statement) throws TException, InvalidRequestException
    {
        if (!CliMain.isConnected())
            return;

        int argCount = statement.getChildCount();

        KsDef currentKeySpace = keyspacesMap.get(keySpace);

        if (argCount > 1) // in case somebody changes Cli grammar
            throw new RuntimeException("`describe` command take maximum one argument. See `help describe;`");

        if (argCount == 0)
        {
            if (currentKeySpace != null)
            {
                describeKeySpace(currentKeySpace.name, null);
                return;
            }

            sessionState.out.println("Authenticate to a Keyspace, before using `describe` or `describe <column_family>`");
        }
        else if (argCount == 1)
        {
            // name of the keyspace or ColumnFamily
            String entityName = statement.getChild(0).getText();

            KsDef inputKsDef = CliUtils.getKeySpaceDef(entityName, thriftClient.describe_keyspaces());

            if (inputKsDef == null && currentKeySpace == null)
                throw new RuntimeException(String.format("Keyspace with name '%s' wasn't found, " +
                                                         "to lookup ColumnFamily with that name, please, authorize to one " +
                                                         "of the keyspaces first.", entityName));

            CfDef inputCfDef = (inputKsDef == null)
                    ? getCfDef(currentKeySpace, entityName)
                    : null;  // no need to lookup CfDef if we know that it was keyspace

            if (inputKsDef != null)
            {
                describeKeySpace(inputKsDef.name, inputKsDef);
            }
            else if (inputCfDef != null)
            {
                NodeProbe probe = sessionState.getNodeProbe();

                try
                {
                    describeColumnFamily(currentKeySpace, inputCfDef, probe);

                    if (probe != null)
                        probe.close();
                }
                catch (IOException e)
                {
                    sessionState.out.println("Error while closing JMX connection: " + e.getMessage());
                }
            }
            else
            {
                sessionState.out.println("Sorry, no Keyspace nor ColumnFamily was found with name: " + entityName);
            }
        }
    }

    // ^(NODE_DESCRIBE_CLUSTER) or describe: schema_versions, partitioner, snitch
    private void executeDescribeCluster()
    {
        if (!CliMain.isConnected())
            return;

        sessionState.out.println("Cluster Information:");
        try
        {
            sessionState.out.println("   Snitch: " + thriftClient.describe_snitch());
            sessionState.out.println("   Partitioner: " + thriftClient.describe_partitioner());

            sessionState.out.println("   Schema versions: ");
            Map<String,List<String>> versions = thriftClient.describe_schema_versions();

            for (String version : versions.keySet())
            {
                sessionState.out.println(String.format("\t%s: %s%n", version, versions.get(version)));
            }
        }
        catch (Exception e)
        {
            String message = (e instanceof InvalidRequestException) ? ((InvalidRequestException) e).getWhy() : e.getMessage();
            sessionState.err.println("Error retrieving data: " + message);
        }
    }

    // process a statement of the form: connect hostname/port
    private void executeConnect(Tree statement)
    {
        Tree idList = statement.getChild(0);
        int portNumber = Integer.parseInt(statement.getChild(1).getText());

        StringBuilder hostName = new StringBuilder();
        int idCount = idList.getChildCount(); 
        for (int idx = 0; idx < idCount; idx++)
        {
            hostName.append(idList.getChild(idx).getText());
        }
        
        // disconnect current connection, if any.
        // This is a no-op, if you aren't currently connected.
        CliMain.disconnect();

        // now, connect to the newly specified host name and port
        sessionState.hostName = hostName.toString();
        sessionState.thriftPort = portNumber;

        // if we have user name and password
        if (statement.getChildCount() == 4)
        {
            sessionState.username = statement.getChild(2).getText();
            sessionState.password = CliUtils.unescapeSQLString(statement.getChild(3).getText());
        }

        CliMain.connect(sessionState.hostName, sessionState.thriftPort);
    }

    /**
     * To get Column Family Definition object from specified keyspace
     * @param keySpaceName key space name to search for specific column family
     * @param columnFamilyName column family name 
     * @return CfDef - Column family definition object
     */
    private CfDef getCfDef(String keySpaceName, String columnFamilyName)
    {
        KsDef keySpaceDefinition = keyspacesMap.get(keySpaceName);
        
        for (CfDef columnFamilyDef : keySpaceDefinition.cf_defs)
        {
            if (columnFamilyDef.name.equals(columnFamilyName))
            {
                return columnFamilyDef;
            }
        }

        throw new RuntimeException("No such column family: " + columnFamilyName);
    }

    /**
     * Uses getCfDef(keySpaceName, columnFamilyName) with current keyspace
     * @param columnFamilyName column family name to find in specified keyspace
     * @return CfDef - Column family definition object
     */
    private CfDef getCfDef(String columnFamilyName)
    {
        return getCfDef(this.keySpace, columnFamilyName);
    }

    private CfDef getCfDef(KsDef keyspace, String columnFamilyName)
    {
        for (CfDef cfDef : keyspace.cf_defs)
        {
            if (cfDef.name.equals(columnFamilyName))
                return cfDef;
        }

        return null;
    }

    /**
     * Used to parse meta tree and compile meta attributes into List<ColumnDef>
     * @param cfDef - column family definition 
     * @param meta (Tree representing Array of the hashes with metadata attributes)
     * @return List<ColumnDef> List of the ColumnDef's
     * 
     * meta is in following format - ^(ARRAY ^(HASH ^(PAIR .. ..) ^(PAIR .. ..)) ^(HASH ...))
     */
    private List<ColumnDef> getCFColumnMetaFromTree(CfDef cfDef, Tree meta)
    {
        // this list will be returned
        List<ColumnDef> columnDefinitions = new ArrayList<ColumnDef>();
        
        // each child node is a ^(HASH ...)
        for (int i = 0; i < meta.getChildCount(); i++)
        {
            Tree metaHash = meta.getChild(i);

            ColumnDef columnDefinition = new ColumnDef();
            
            // each child node is ^(PAIR $key $value)
            for (int j = 0; j < metaHash.getChildCount(); j++)
            {
                Tree metaPair = metaHash.getChild(j);

                // current $key
                String metaKey = CliUtils.unescapeSQLString(metaPair.getChild(0).getText());
                // current $value
                String metaVal = CliUtils.unescapeSQLString(metaPair.getChild(1).getText());

                if (metaKey.equals("column_name"))
                {
                    if (cfDef.column_type.equals("Super"))
                        columnDefinition.setName(subColumnNameAsByteArray(metaVal, cfDef));
                    else
                        columnDefinition.setName(columnNameAsByteArray(metaVal, cfDef));
                }
                else if (metaKey.equals("validation_class"))
                {
                    columnDefinition.setValidation_class(metaVal);
                }
                else if (metaKey.equals("index_type"))
                {
                    columnDefinition.setIndex_type(getIndexTypeFromString(metaVal));
                }
                else if (metaKey.equals("index_options"))
                {
                    columnDefinition.setIndex_options(getStrategyOptionsFromTree(metaPair.getChild(1)));
                }
                else if (metaKey.equals("index_name"))
                {
                    columnDefinition.setIndex_name(metaVal);    
                }
            }

            // validating columnDef structure, 'name' and 'validation_class' must be set 
            try
            {
                columnDefinition.validate();
            }
            catch (TException e)
            {
                throw new RuntimeException(e);
            }

            columnDefinitions.add(columnDefinition);
        }

        return columnDefinitions;
    }

    /**
     * Getting IndexType object from indexType string
     * @param indexTypeAsString - string return by parser corresponding to IndexType 
     * @return IndexType - an IndexType object
     */
    private IndexType getIndexTypeFromString(String indexTypeAsString)
    {
        IndexType indexType;

        try
        {
            indexType = IndexType.findByValue(new Integer(indexTypeAsString));
        }
        catch (NumberFormatException e)
        {
            try
            {
                // if this is not an integer lets try to get IndexType by name
                indexType = IndexType.valueOf(indexTypeAsString);
            }
            catch (IllegalArgumentException ie)
            {
                throw new RuntimeException("IndexType '" + indexTypeAsString + "' is unsupported.", ie);
            }
        }

        if (indexType == null)
        {
            throw new RuntimeException("IndexType '" + indexTypeAsString + "' is unsupported.");
        }

        return indexType;
    }

    /**
     * Converts object represented as string into byte[] according to comparator
     * @param object - object to covert into byte array
     * @param comparator - comparator used to convert object
     * @return byte[] - object in the byte array representation
     */
    private ByteBuffer getBytesAccordingToType(String object, AbstractType comparator)
    {
        if (comparator == null) // default comparator is BytesType
            comparator = BytesType.instance;

        try
        {
            return comparator.fromString(object);
        }
        catch (MarshalException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Converts column name into byte[] according to comparator type
     * @param column - column name from parser
     * @param columnFamily - column family name from parser
     * @return ByteBuffer - bytes into which column name was converted according to comparator type
     */
    private ByteBuffer columnNameAsBytes(String column, String columnFamily) 
    {
        CfDef columnFamilyDef = getCfDef(columnFamily);
        return columnNameAsBytes(column, columnFamilyDef);
    }
    /**
     * Converts column name into byte[] according to comparator type
     * @param column - column name from parser
     * @param columnFamilyDef - column family from parser
     * @return ByteBuffer bytes - into which column name was converted according to comparator type
     */
    private ByteBuffer columnNameAsBytes(String column, CfDef columnFamilyDef) 
    {
        String comparatorClass = columnFamilyDef.comparator_type;
        return getBytesAccordingToType(column, getFormatType(comparatorClass));
    }

    /**
     * Converts column name into byte[] according to comparator type
     * @param column - column name from parser
     * @param columnFamily - column family name from parser
     * @return bytes[] - into which column name was converted according to comparator type
     */
    private byte[] columnNameAsByteArray(String column, String columnFamily)
    {
        return TBaseHelper.byteBufferToByteArray(columnNameAsBytes(column, columnFamily));
    }

    /**
     * Converts column name into byte[] according to comparator type
     * @param column - column name from parser
     * @param cfDef  - column family from parser
     * @return bytes[] - into which column name was converted according to comparator type
     */
    private byte[] columnNameAsByteArray(String column, CfDef cfDef)
    {
        return TBaseHelper.byteBufferToByteArray(columnNameAsBytes(column, cfDef));
    }

    /**
     * Converts sub-column name into ByteBuffer according to comparator type
     * @param superColumn - sub-column name from parser
     * @param columnFamily - column family name from parser
     * @return ByteBuffer bytes - into which column name was converted according to comparator type
     */
    private ByteBuffer subColumnNameAsBytes(String superColumn, String columnFamily)
    {
        CfDef columnFamilyDef = getCfDef(columnFamily);
        return subColumnNameAsBytes(superColumn, columnFamilyDef);
    }

    /**
     * Converts column name into ByteBuffer according to comparator type
     * @param superColumn - sub-column name from parser
     * @param columnFamilyDef - column family from parser
     * @return ByteBuffer bytes - into which column name was converted according to comparator type
     */
    private ByteBuffer subColumnNameAsBytes(String superColumn, CfDef columnFamilyDef) 
    {
        String comparatorClass = columnFamilyDef.subcomparator_type;

        if (comparatorClass == null)
        {
            sessionState.out.println(String.format("Notice: defaulting to BytesType subcomparator for '%s'", columnFamilyDef.getName()));
            comparatorClass = "BytesType";
        }

        return getBytesAccordingToType(superColumn, getFormatType(comparatorClass));
    }

    /**
     * Converts sub-column name into byte[] according to comparator type
     * @param superColumn - sub-column name from parser
     * @param cfDef - column family from parser
     * @return bytes[] - into which column name was converted according to comparator type
     */
    private byte[] subColumnNameAsByteArray(String superColumn, CfDef cfDef)
    {
        return TBaseHelper.byteBufferToByteArray(subColumnNameAsBytes(superColumn, cfDef));
    }

    /**
     * Converts column value into byte[] according to validation class
     * @param columnName - column name to which value belongs
     * @param columnFamilyName - column family name
     * @param columnValue - actual column value
     * @return value in byte array representation
     */
    private ByteBuffer columnValueAsBytes(ByteBuffer columnName, String columnFamilyName, String columnValue)
    {
        CfDef columnFamilyDef = getCfDef(columnFamilyName);
        AbstractType defaultValidator = getFormatType(columnFamilyDef.default_validation_class);

        for (ColumnDef columnDefinition : columnFamilyDef.getColumn_metadata())
        {
            byte[] currentColumnName = columnDefinition.getName();

            if (ByteBufferUtil.compare(currentColumnName, columnName) == 0)
            {
                try
                {
                    String validationClass = columnDefinition.getValidation_class();
                    return getBytesAccordingToType(columnValue, getFormatType(validationClass));
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        }

        return defaultValidator.fromString(columnValue);
    }

    /**
     * Get validator for specific column value
     * @param ColumnFamilyDef - CfDef object representing column family with metadata
     * @param columnNameInBytes - column name as byte array
     * @return AbstractType - validator for column value
     */
    private AbstractType getValidatorForValue(CfDef ColumnFamilyDef, byte[] columnNameInBytes)
    {
        String defaultValidator = ColumnFamilyDef.default_validation_class;
        
        for (ColumnDef columnDefinition : ColumnFamilyDef.getColumn_metadata())
        {
            byte[] nameInBytes = columnDefinition.getName();

            if (Arrays.equals(nameInBytes, columnNameInBytes))
            {
                return getFormatType(columnDefinition.getValidation_class());
            }
        }

        if (defaultValidator != null && !defaultValidator.isEmpty()) 
        {
            return getFormatType(defaultValidator);
        }

        return null;
    }

    /**
     * Used to get Map of the provided options by create/update keyspace commands
     * @param options - tree representing options
     * @return Map - strategy_options map
     */
    private Map<String, String> getStrategyOptionsFromTree(Tree options)
    {
        //Check for old [{}] syntax
        if (options.getText().equalsIgnoreCase("ARRAY"))
        {
            System.err.println("WARNING: [{}] strategy_options syntax is deprecated, please use {}");

            if (options.getChildCount() == 0)
                return Collections.EMPTY_MAP;

            return getStrategyOptionsFromTree(options.getChild(0));
        }

        // this map will be returned
        Map<String, String> strategyOptions = new HashMap<String, String>();

        // each child node is ^(PAIR $key $value)
        for (int j = 0; j < options.getChildCount(); j++)
        {
            Tree optionPair = options.getChild(j);

            // current $key
            String key = CliUtils.unescapeSQLString(optionPair.getChild(0).getText());
            // current $value
            String val = CliUtils.unescapeSQLString(optionPair.getChild(1).getText());

            strategyOptions.put(key, val);           
        }

        return strategyOptions;
    }

    /**
     * Used to convert value (function argument, string) into byte[]
     * calls convertValueByFunction method with "withUpdate" set to false
     * @param functionCall - tree representing function call ^(FUNCTION_CALL function_name value)
     * @param columnFamily - column family definition (CfDef) 
     * @param columnName   - also updates column family metadata for given column
     * @return byte[] - string value as byte[] 
     */
    private ByteBuffer convertValueByFunction(Tree functionCall, CfDef columnFamily, ByteBuffer columnName)
    {
        return convertValueByFunction(functionCall, columnFamily, columnName, false);
    }
    
    /**
     * Used to convert value (function argument, string) into byte[]
     * @param functionCall - tree representing function call ^(FUNCTION_CALL function_name value)
     * @param columnFamily - column family definition (CfDef)
     * @param columnName   - column name as byte[] (used to update CfDef)
     * @param withUpdate   - also updates column family metadata for given column
     * @return byte[] - string value as byte[]
     */
    private ByteBuffer convertValueByFunction(Tree functionCall, CfDef columnFamily, ByteBuffer columnName, boolean withUpdate)
    {
        String functionName = functionCall.getChild(0).getText();
        Tree argumentTree = functionCall.getChild(1);
        String functionArg  = (argumentTree == null) ? "" : CliUtils.unescapeSQLString(argumentTree.getText());
        AbstractType validator = getTypeByFunction(functionName);

        try
        {

            ByteBuffer value;

            if (functionArg.isEmpty())
            {
                if (validator instanceof TimeUUIDType)
                {
                    value = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes());
                }
                else if (validator instanceof LexicalUUIDType)
                {
                    value = ByteBuffer.wrap(UUIDGen.decompose(UUID.randomUUID()));
                }
                else if (validator instanceof BytesType)
                {
                    value = ByteBuffer.wrap(new byte[0]);
                }
                else
                {
                    throw new RuntimeException(String.format("Argument for '%s' could not be empty.", functionName));
                }
            }
            else
            {
                value = getBytesAccordingToType(functionArg, validator);
            }

            // performing ColumnDef local validator update
            if (withUpdate)
            {
                updateColumnMetaData(columnFamily, columnName, validator.toString());
            }

            return value;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get AbstractType by function name
     * @param functionName - name of the function e.g. utf8, integer, long etc.
     * @return AbstractType type corresponding to the function name
     */
    public static AbstractType getTypeByFunction(String functionName)
    {
        Function function;

        try
        {
            function = Function.valueOf(functionName.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            StringBuilder errorMessage = new StringBuilder("Function '" + functionName + "' not found. ");
            errorMessage.append("Available functions: ");
            throw new RuntimeException(errorMessage.append(Function.getFunctionNames()).toString(), e);
        }

        return function.getValidator();
    }

    /**
     * Used to locally update column family definition with new column metadata
     * @param columnFamily    - CfDef record
     * @param columnName      - column name represented as byte[]
     * @param validationClass - value validation class
     */
    private void updateColumnMetaData(CfDef columnFamily, ByteBuffer columnName, String validationClass)
    {
        List<ColumnDef> columnMetaData = columnFamily.getColumn_metadata();
        ColumnDef column = getColumnDefByName(columnFamily, columnName);

        if (column != null)
        {
            // if validation class is the same - no need to modify it
            if (column.getValidation_class().equals(validationClass))
                return;

            // updating column definition with new validation_class
            column.setValidation_class(validationClass);
        }
        else
        {
            columnMetaData.add(new ColumnDef(columnName, validationClass));
        }
    }

    /**
     * Get specific ColumnDef in column family meta data by column name
     * @param columnFamily - CfDef record
     * @param columnName   - column name represented as byte[]
     * @return ColumnDef   - found column definition
     */
    private ColumnDef getColumnDefByName(CfDef columnFamily, ByteBuffer columnName)
    {
        for (ColumnDef columnDef : columnFamily.getColumn_metadata())
        {
            byte[] currName = columnDef.getName();

            if (ByteBufferUtil.compare(currName, columnName) == 0)
            {
                return columnDef;
            }
        }

        return null;
    }

    /**
     * Prints out KeySlice list
     * @param columnFamilyDef - column family definition
     * @param slices - list of the KeySlice's to print out
     * @throws NotFoundException - column not found
     * @throws TException - transfer is broken
     * @throws IllegalAccessException - can't do operation
     * @throws InstantiationException - can't instantiate a class
     * @throws NoSuchFieldException - column not found
     */
    private void printSliceList(CfDef columnFamilyDef, List<KeySlice> slices)
            throws NotFoundException, TException, IllegalAccessException, InstantiationException, NoSuchFieldException, CharacterCodingException
    {
        AbstractType validator;
        String columnFamilyName = columnFamilyDef.getName();
        AbstractType keyComparator = getKeyComparatorForCF(columnFamilyName);

        for (KeySlice ks : slices)
        {
            String keyName = (keyComparator == null) ? ByteBufferUtil.string(ks.key) : keyComparator.getString(ks.key);

            sessionState.out.printf("-------------------%n");
            sessionState.out.printf("RowKey: %s%n", keyName);
            Iterator<ColumnOrSuperColumn> iterator = ks.getColumnsIterator();

            while (iterator.hasNext())
            {
                ColumnOrSuperColumn columnOrSuperColumn = iterator.next();

                if (columnOrSuperColumn.column != null)
                {
                    Column col = columnOrSuperColumn.column;
                    validator = getValidatorForValue(columnFamilyDef, col.getName());

                    sessionState.out.printf("=> (column=%s, value=%s, timestamp=%d%s)%n",
                                    formatColumnName(keySpace, columnFamilyName, col.name), validator.getString(col.value), col.timestamp,
                                    col.isSetTtl() ? String.format(", ttl=%d", col.getTtl()) : "");
                }
                else if (columnOrSuperColumn.super_column != null)
                {
                    SuperColumn superCol = columnOrSuperColumn.super_column;
                    sessionState.out.printf("=> (super_column=%s,", formatColumnName(keySpace, columnFamilyName, superCol.name));

                    for (Column col : superCol.columns)
                    {
                        validator = getValidatorForValue(columnFamilyDef, col.getName());

                        sessionState.out.printf("%n     (column=%s, value=%s, timestamp=%d%s)",
                                        formatSubcolumnName(keySpace, columnFamilyName, col.name), validator.getString(col.value), col.timestamp,
                                        col.isSetTtl() ? String.format(", ttl=%d", col.getTtl()) : "");
                    }

                    sessionState.out.println(")");
                }
                else if (columnOrSuperColumn.counter_column != null)
                {
                    CounterColumn col = columnOrSuperColumn.counter_column;

                    sessionState.out.printf("=> (counter=%s, value=%s)%n", formatColumnName(keySpace, columnFamilyName, col.name), col.value);
                }
                else if (columnOrSuperColumn.counter_super_column != null)
                {
                    CounterSuperColumn superCol = columnOrSuperColumn.counter_super_column;
                    sessionState.out.printf("=> (super_column=%s,", formatColumnName(keySpace, columnFamilyName, superCol.name));

                    for (CounterColumn col : superCol.columns)
                    {
                        sessionState.out.printf("%n     (counter=%s, value=%s)", formatSubcolumnName(keySpace, columnFamilyName, col.name), col.value);
                    }

                    sessionState.out.println(")");
                }
            }
        }

        sessionState.out.printf("%n%d Row%s Returned.%n", slices.size(), (slices.size() > 1 ? "s" : ""));
    }

    // retuns sub-column name in human-readable format
    private String formatSubcolumnName(String keyspace, String columnFamily, ByteBuffer name)
            throws NotFoundException, TException, IllegalAccessException, InstantiationException, NoSuchFieldException
    {
        return getFormatType(getCfDef(keyspace, columnFamily).subcomparator_type).getString(name);
    }

    // retuns column name in human-readable format
    private String formatColumnName(String keyspace, String columnFamily, ByteBuffer name)
            throws NotFoundException, TException, IllegalAccessException, InstantiationException, NoSuchFieldException
    {
        return getFormatType(getCfDef(keyspace, columnFamily).comparator_type).getString(name);
    }

    private ByteBuffer getColumnName(String columnFamily, Tree columnTree)
    {
        return (columnTree.getType() == CliParser.FUNCTION_CALL)
                    ? convertValueByFunction(columnTree, null, null)
                    : columnNameAsBytes(CliUtils.unescapeSQLString(columnTree.getText()), columnFamily);
    }

    private ByteBuffer getSubColumnName(String columnFamily, Tree columnTree)
    {
        return (columnTree.getType() == CliParser.FUNCTION_CALL)
                    ? convertValueByFunction(columnTree, null, null)
                    : subColumnNameAsBytes(CliUtils.unescapeSQLString(columnTree.getText()), columnFamily);
    }

    public ByteBuffer getKeyAsBytes(String columnFamily, Tree keyTree)
    {
        if (keyTree.getType() == CliParser.FUNCTION_CALL)
            return convertValueByFunction(keyTree, null, null);

        String key = CliUtils.unescapeSQLString(keyTree.getText());

        return getBytesAccordingToType(key, getKeyComparatorForCF(columnFamily));
    }

    private AbstractType getKeyComparatorForCF(String columnFamily)
    {
        AbstractType keyComparator = cfKeysComparators.get(columnFamily);

        if (keyComparator == null)
        {
            String defaultValidationClass = getCfDef(columnFamily).getKey_validation_class();
            assert defaultValidationClass != null;
            keyComparator = getFormatType(defaultValidationClass);
        }

        return keyComparator;
    }

    private static class KsDefNamesComparator implements Comparator<KsDef>
    {
        public int compare(KsDef a, KsDef b)
        {
            return a.name.compareTo(b.name);
        }
    }

    /** validates schema is propagated to all nodes */
    private void validateSchemaIsSettled(String currentVersionId)
    {
        sessionState.out.println("Waiting for schema agreement...");
        Map<String, List<String>> versions = null;

        long limit = System.currentTimeMillis() + sessionState.schema_mwt;
        boolean inAgreement = false;
        outer:
        while (limit - System.currentTimeMillis() >= 0 && !inAgreement)
        {
            try
            {
                versions = thriftClient.describe_schema_versions(); // getting schema version for nodes of the ring
            }
            catch (Exception e)
            {
                sessionState.err.println((e instanceof InvalidRequestException) ? ((InvalidRequestException) e).getWhy() : e.getMessage());
                continue;
            }

            for (String version : versions.keySet())
            {
                if (!version.equals(currentVersionId) && !version.equals(StorageProxy.UNREACHABLE))
                    continue outer;
            }
            inAgreement = true;
        }

        if (versions.containsKey(StorageProxy.UNREACHABLE))
            sessionState.err.printf("Warning: unreachable nodes %s", Joiner.on(", ").join(versions.get(StorageProxy.UNREACHABLE)));
        if (!inAgreement)
        {
            sessionState.err.printf("The schema has not settled in %d seconds; further migrations are ill-advised until it does.%nVersions are %s%n",
                                    sessionState.schema_mwt / 1000, FBUtilities.toString(versions));
            System.exit(-1);
        }
        sessionState.out.println("... schemas agree across the cluster");
    }

    private static class CfDefNamesComparator implements Comparator<CfDef>
    {
        public int compare(CfDef a, CfDef b)
        {
            return a.name.compareTo(b.name);
        }
    }

    private boolean isCounterCF(CfDef cfdef)
    {
        String defaultValidator = cfdef.default_validation_class;
        if (defaultValidator != null && !defaultValidator.isEmpty())
        {
            return (getFormatType(defaultValidator) instanceof CounterColumnType);
        }
        return false;
    }

    private void elapsedTime(long startTime)
    {
        sessionState.out.println("Elapsed time: " + (System.currentTimeMillis() - startTime) + " msec(s).");
    }
}
