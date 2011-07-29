// $ANTLR 3.2 Sep 23, 2009 12:02:23 /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g 2011-07-28 15:11:02

package org.apache.cassandra.cli;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.antlr.runtime.tree.*;

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
public class CliParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "NODE_CONNECT", "NODE_DESCRIBE_TABLE", "NODE_DESCRIBE_CLUSTER", "NODE_USE_TABLE", "NODE_EXIT", "NODE_HELP", "NODE_NO_OP", "NODE_SHOW_CLUSTER_NAME", "NODE_SHOW_VERSION", "NODE_SHOW_KEYSPACES", "NODE_THRIFT_GET", "NODE_THRIFT_GET_WITH_CONDITIONS", "NODE_THRIFT_SET", "NODE_THRIFT_COUNT", "NODE_THRIFT_DEL", "NODE_THRIFT_INCR", "NODE_THRIFT_DECR", "NODE_ADD_COLUMN_FAMILY", "NODE_ADD_KEYSPACE", "NODE_DEL_KEYSPACE", "NODE_DEL_COLUMN_FAMILY", "NODE_UPDATE_KEYSPACE", "NODE_UPDATE_COLUMN_FAMILY", "NODE_LIST", "NODE_TRUNCATE", "NODE_ASSUME", "NODE_CONSISTENCY_LEVEL", "NODE_DROP_INDEX", "NODE_COLUMN_ACCESS", "NODE_ID_LIST", "NODE_NEW_CF_ACCESS", "NODE_NEW_KEYSPACE_ACCESS", "CONVERT_TO_TYPE", "FUNCTION_CALL", "CONDITION", "CONDITIONS", "ARRAY", "HASH", "PAIR", "NODE_LIMIT", "NODE_KEY_RANGE", "SEMICOLON", "CONNECT", "HELP", "USE", "DESCRIBE", "KEYSPACE", "EXIT", "QUIT", "SHOW", "KEYSPACES", "API_VERSION", "CREATE", "UPDATE", "COLUMN", "FAMILY", "DROP", "INDEX", "GET", "SET", "INCR", "DECR", "DEL", "COUNT", "LIST", "TRUNCATE", "ASSUME", "CONSISTENCYLEVEL", "IntegerPositiveLiteral", "Identifier", "StringLiteral", "WITH", "TTL", "BY", "ON", "AND", "IntegerNegativeLiteral", "DoubleLiteral", "IP_ADDRESS", "CONFIG", "FILE", "LIMIT", "Letter", "Digit", "Alnum", "SingleStringCharacter", "EscapeSequence", "CharacterEscapeSequence", "HexEscapeSequence", "UnicodeEscapeSequence", "SingleEscapeCharacter", "NonEscapeCharacter", "EscapeCharacter", "DecimalDigit", "HexDigit", "WS", "COMMENT", "'/'", "'CLUSTER'", "'CLUSTER NAME'", "'?'", "'AS'", "'WHERE'", "'='", "'>'", "'<'", "'>='", "'<='", "'.'", "'['", "','", "']'", "'{'", "'}'", "':'", "'('", "')'"
    };
    public static final int NODE_THRIFT_GET_WITH_CONDITIONS=15;
    public static final int TTL=76;
    public static final int NODE_SHOW_KEYSPACES=13;
    public static final int CONDITION=38;
    public static final int COUNT=67;
    public static final int DecimalDigit=97;
    public static final int EOF=-1;
    public static final int Identifier=73;
    public static final int NODE_UPDATE_COLUMN_FAMILY=26;
    public static final int SingleStringCharacter=89;
    public static final int NODE_USE_TABLE=7;
    public static final int NODE_DEL_KEYSPACE=23;
    public static final int CREATE=56;
    public static final int NODE_CONNECT=4;
    public static final int CONNECT=46;
    public static final int INCR=64;
    public static final int SingleEscapeCharacter=94;
    public static final int FAMILY=59;
    public static final int GET=62;
    public static final int NODE_DESCRIBE_TABLE=5;
    public static final int COMMENT=100;
    public static final int SHOW=53;
    public static final int ARRAY=40;
    public static final int NODE_ADD_KEYSPACE=22;
    public static final int EXIT=51;
    public static final int NODE_THRIFT_DEL=18;
    public static final int IntegerNegativeLiteral=80;
    public static final int ON=78;
    public static final int NODE_DROP_INDEX=31;
    public static final int SEMICOLON=45;
    public static final int KEYSPACES=54;
    public static final int CONDITIONS=39;
    public static final int FILE=84;
    public static final int NODE_LIMIT=43;
    public static final int LIST=68;
    public static final int NODE_DESCRIBE_CLUSTER=6;
    public static final int IP_ADDRESS=82;
    public static final int NODE_THRIFT_SET=16;
    public static final int NODE_NO_OP=10;
    public static final int NODE_ID_LIST=33;
    public static final int WS=99;
    public static final int ASSUME=70;
    public static final int NODE_THRIFT_COUNT=17;
    public static final int DESCRIBE=49;
    public static final int Alnum=88;
    public static final int CharacterEscapeSequence=91;
    public static final int NODE_SHOW_CLUSTER_NAME=11;
    public static final int USE=48;
    public static final int NODE_THRIFT_DECR=20;
    public static final int FUNCTION_CALL=37;
    public static final int EscapeSequence=90;
    public static final int Letter=86;
    public static final int DoubleLiteral=81;
    public static final int HELP=47;
    public static final int HexEscapeSequence=92;
    public static final int NODE_EXIT=8;
    public static final int LIMIT=85;
    public static final int T__118=118;
    public static final int T__119=119;
    public static final int DEL=66;
    public static final int T__116=116;
    public static final int T__117=117;
    public static final int T__114=114;
    public static final int T__115=115;
    public static final int NODE_LIST=27;
    public static final int UPDATE=57;
    public static final int NODE_UPDATE_KEYSPACE=25;
    public static final int T__120=120;
    public static final int AND=79;
    public static final int NODE_NEW_CF_ACCESS=34;
    public static final int CONSISTENCYLEVEL=71;
    public static final int HexDigit=98;
    public static final int QUIT=52;
    public static final int NODE_TRUNCATE=28;
    public static final int INDEX=61;
    public static final int NODE_SHOW_VERSION=12;
    public static final int T__107=107;
    public static final int T__108=108;
    public static final int NODE_NEW_KEYSPACE_ACCESS=35;
    public static final int T__109=109;
    public static final int T__103=103;
    public static final int T__104=104;
    public static final int TRUNCATE=69;
    public static final int T__105=105;
    public static final int T__106=106;
    public static final int COLUMN=58;
    public static final int T__111=111;
    public static final int T__110=110;
    public static final int T__113=113;
    public static final int EscapeCharacter=96;
    public static final int T__112=112;
    public static final int PAIR=42;
    public static final int NODE_CONSISTENCY_LEVEL=30;
    public static final int WITH=75;
    public static final int BY=77;
    public static final int UnicodeEscapeSequence=93;
    public static final int HASH=41;
    public static final int SET=63;
    public static final int T__102=102;
    public static final int T__101=101;
    public static final int Digit=87;
    public static final int API_VERSION=55;
    public static final int NODE_ASSUME=29;
    public static final int CONVERT_TO_TYPE=36;
    public static final int NODE_THRIFT_GET=14;
    public static final int NODE_DEL_COLUMN_FAMILY=24;
    public static final int NODE_KEY_RANGE=44;
    public static final int KEYSPACE=50;
    public static final int StringLiteral=74;
    public static final int NODE_HELP=9;
    public static final int CONFIG=83;
    public static final int IntegerPositiveLiteral=72;
    public static final int DROP=60;
    public static final int NonEscapeCharacter=95;
    public static final int DECR=65;
    public static final int NODE_ADD_COLUMN_FAMILY=21;
    public static final int NODE_THRIFT_INCR=19;
    public static final int NODE_COLUMN_ACCESS=32;

    // delegates
    // delegators


        public CliParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public CliParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return CliParser.tokenNames; }
    public String getGrammarFileName() { return "/home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g"; }


        public void reportError(RecognitionException e) 
        {
            String errorMessage;

            if (e instanceof NoViableAltException)
            {
                errorMessage = "Command not found: `" + this.input + "`. Type 'help;' or '?' for help.";
            }
            else
            {
                errorMessage = "Syntax error at position " + e.charPositionInLine + ": " + this.getErrorMessage(e, this.getTokenNames());
            }

            throw new RuntimeException(errorMessage);
        }


    public static class root_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "root"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:139:1: root : statement ( SEMICOLON )? EOF -> statement ;
    public final CliParser.root_return root() throws RecognitionException {
        CliParser.root_return retval = new CliParser.root_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token SEMICOLON2=null;
        Token EOF3=null;
        CliParser.statement_return statement1 = null;


        CommonTree SEMICOLON2_tree=null;
        CommonTree EOF3_tree=null;
        RewriteRuleTokenStream stream_SEMICOLON=new RewriteRuleTokenStream(adaptor,"token SEMICOLON");
        RewriteRuleTokenStream stream_EOF=new RewriteRuleTokenStream(adaptor,"token EOF");
        RewriteRuleSubtreeStream stream_statement=new RewriteRuleSubtreeStream(adaptor,"rule statement");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:139:5: ( statement ( SEMICOLON )? EOF -> statement )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:139:7: statement ( SEMICOLON )? EOF
            {
            pushFollow(FOLLOW_statement_in_root414);
            statement1=statement();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_statement.add(statement1.getTree());
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:139:17: ( SEMICOLON )?
            int alt1=2;
            int LA1_0 = input.LA(1);

            if ( (LA1_0==SEMICOLON) ) {
                alt1=1;
            }
            switch (alt1) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:0:0: SEMICOLON
                    {
                    SEMICOLON2=(Token)match(input,SEMICOLON,FOLLOW_SEMICOLON_in_root416); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_SEMICOLON.add(SEMICOLON2);


                    }
                    break;

            }

            EOF3=(Token)match(input,EOF,FOLLOW_EOF_in_root419); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_EOF.add(EOF3);



            // AST REWRITE
            // elements: statement
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 139:32: -> statement
            {
                adaptor.addChild(root_0, stream_statement.nextTree());

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "root"

    public static class statement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "statement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:141:1: statement : ( connectStatement | exitStatement | countStatement | describeTable | describeCluster | addKeyspace | addColumnFamily | updateKeyspace | updateColumnFamily | delColumnFamily | delKeyspace | useKeyspace | delStatement | getStatement | helpStatement | setStatement | incrStatement | showStatement | listStatement | truncateStatement | assumeStatement | consistencyLevelStatement | dropIndex | -> ^( NODE_NO_OP ) );
    public final CliParser.statement_return statement() throws RecognitionException {
        CliParser.statement_return retval = new CliParser.statement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CliParser.connectStatement_return connectStatement4 = null;

        CliParser.exitStatement_return exitStatement5 = null;

        CliParser.countStatement_return countStatement6 = null;

        CliParser.describeTable_return describeTable7 = null;

        CliParser.describeCluster_return describeCluster8 = null;

        CliParser.addKeyspace_return addKeyspace9 = null;

        CliParser.addColumnFamily_return addColumnFamily10 = null;

        CliParser.updateKeyspace_return updateKeyspace11 = null;

        CliParser.updateColumnFamily_return updateColumnFamily12 = null;

        CliParser.delColumnFamily_return delColumnFamily13 = null;

        CliParser.delKeyspace_return delKeyspace14 = null;

        CliParser.useKeyspace_return useKeyspace15 = null;

        CliParser.delStatement_return delStatement16 = null;

        CliParser.getStatement_return getStatement17 = null;

        CliParser.helpStatement_return helpStatement18 = null;

        CliParser.setStatement_return setStatement19 = null;

        CliParser.incrStatement_return incrStatement20 = null;

        CliParser.showStatement_return showStatement21 = null;

        CliParser.listStatement_return listStatement22 = null;

        CliParser.truncateStatement_return truncateStatement23 = null;

        CliParser.assumeStatement_return assumeStatement24 = null;

        CliParser.consistencyLevelStatement_return consistencyLevelStatement25 = null;

        CliParser.dropIndex_return dropIndex26 = null;



        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:142:5: ( connectStatement | exitStatement | countStatement | describeTable | describeCluster | addKeyspace | addColumnFamily | updateKeyspace | updateColumnFamily | delColumnFamily | delKeyspace | useKeyspace | delStatement | getStatement | helpStatement | setStatement | incrStatement | showStatement | listStatement | truncateStatement | assumeStatement | consistencyLevelStatement | dropIndex | -> ^( NODE_NO_OP ) )
            int alt2=24;
            alt2 = dfa2.predict(input);
            switch (alt2) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:142:7: connectStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_connectStatement_in_statement435);
                    connectStatement4=connectStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, connectStatement4.getTree());

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:143:7: exitStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_exitStatement_in_statement443);
                    exitStatement5=exitStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, exitStatement5.getTree());

                    }
                    break;
                case 3 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:144:7: countStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_countStatement_in_statement451);
                    countStatement6=countStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, countStatement6.getTree());

                    }
                    break;
                case 4 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:145:7: describeTable
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_describeTable_in_statement459);
                    describeTable7=describeTable();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, describeTable7.getTree());

                    }
                    break;
                case 5 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:146:7: describeCluster
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_describeCluster_in_statement467);
                    describeCluster8=describeCluster();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, describeCluster8.getTree());

                    }
                    break;
                case 6 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:147:7: addKeyspace
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_addKeyspace_in_statement475);
                    addKeyspace9=addKeyspace();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, addKeyspace9.getTree());

                    }
                    break;
                case 7 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:148:7: addColumnFamily
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_addColumnFamily_in_statement483);
                    addColumnFamily10=addColumnFamily();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, addColumnFamily10.getTree());

                    }
                    break;
                case 8 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:149:7: updateKeyspace
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_updateKeyspace_in_statement491);
                    updateKeyspace11=updateKeyspace();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, updateKeyspace11.getTree());

                    }
                    break;
                case 9 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:150:7: updateColumnFamily
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_updateColumnFamily_in_statement499);
                    updateColumnFamily12=updateColumnFamily();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, updateColumnFamily12.getTree());

                    }
                    break;
                case 10 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:151:7: delColumnFamily
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_delColumnFamily_in_statement507);
                    delColumnFamily13=delColumnFamily();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, delColumnFamily13.getTree());

                    }
                    break;
                case 11 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:152:7: delKeyspace
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_delKeyspace_in_statement515);
                    delKeyspace14=delKeyspace();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, delKeyspace14.getTree());

                    }
                    break;
                case 12 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:153:7: useKeyspace
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_useKeyspace_in_statement523);
                    useKeyspace15=useKeyspace();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, useKeyspace15.getTree());

                    }
                    break;
                case 13 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:154:7: delStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_delStatement_in_statement531);
                    delStatement16=delStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, delStatement16.getTree());

                    }
                    break;
                case 14 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:155:7: getStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_getStatement_in_statement539);
                    getStatement17=getStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, getStatement17.getTree());

                    }
                    break;
                case 15 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:156:7: helpStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_helpStatement_in_statement547);
                    helpStatement18=helpStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, helpStatement18.getTree());

                    }
                    break;
                case 16 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:157:7: setStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_setStatement_in_statement555);
                    setStatement19=setStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, setStatement19.getTree());

                    }
                    break;
                case 17 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:158:7: incrStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_incrStatement_in_statement563);
                    incrStatement20=incrStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, incrStatement20.getTree());

                    }
                    break;
                case 18 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:159:7: showStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_showStatement_in_statement571);
                    showStatement21=showStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, showStatement21.getTree());

                    }
                    break;
                case 19 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:160:7: listStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_listStatement_in_statement579);
                    listStatement22=listStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, listStatement22.getTree());

                    }
                    break;
                case 20 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:161:7: truncateStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_truncateStatement_in_statement587);
                    truncateStatement23=truncateStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, truncateStatement23.getTree());

                    }
                    break;
                case 21 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:162:7: assumeStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_assumeStatement_in_statement595);
                    assumeStatement24=assumeStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, assumeStatement24.getTree());

                    }
                    break;
                case 22 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:163:7: consistencyLevelStatement
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_consistencyLevelStatement_in_statement603);
                    consistencyLevelStatement25=consistencyLevelStatement();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, consistencyLevelStatement25.getTree());

                    }
                    break;
                case 23 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:164:7: dropIndex
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_dropIndex_in_statement611);
                    dropIndex26=dropIndex();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, dropIndex26.getTree());

                    }
                    break;
                case 24 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:165:7: 
                    {

                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 165:7: -> ^( NODE_NO_OP )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:165:10: ^( NODE_NO_OP )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_NO_OP, "NODE_NO_OP"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "statement"

    public static class connectStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "connectStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:168:1: connectStatement : ( CONNECT host '/' port ( username password )? -> ^( NODE_CONNECT host port ( username password )? ) | CONNECT ip_address '/' port ( username password )? -> ^( NODE_CONNECT ip_address port ( username password )? ) );
    public final CliParser.connectStatement_return connectStatement() throws RecognitionException {
        CliParser.connectStatement_return retval = new CliParser.connectStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token CONNECT27=null;
        Token char_literal29=null;
        Token CONNECT33=null;
        Token char_literal35=null;
        CliParser.host_return host28 = null;

        CliParser.port_return port30 = null;

        CliParser.username_return username31 = null;

        CliParser.password_return password32 = null;

        CliParser.ip_address_return ip_address34 = null;

        CliParser.port_return port36 = null;

        CliParser.username_return username37 = null;

        CliParser.password_return password38 = null;


        CommonTree CONNECT27_tree=null;
        CommonTree char_literal29_tree=null;
        CommonTree CONNECT33_tree=null;
        CommonTree char_literal35_tree=null;
        RewriteRuleTokenStream stream_CONNECT=new RewriteRuleTokenStream(adaptor,"token CONNECT");
        RewriteRuleTokenStream stream_101=new RewriteRuleTokenStream(adaptor,"token 101");
        RewriteRuleSubtreeStream stream_port=new RewriteRuleSubtreeStream(adaptor,"rule port");
        RewriteRuleSubtreeStream stream_ip_address=new RewriteRuleSubtreeStream(adaptor,"rule ip_address");
        RewriteRuleSubtreeStream stream_username=new RewriteRuleSubtreeStream(adaptor,"rule username");
        RewriteRuleSubtreeStream stream_host=new RewriteRuleSubtreeStream(adaptor,"rule host");
        RewriteRuleSubtreeStream stream_password=new RewriteRuleSubtreeStream(adaptor,"rule password");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:169:5: ( CONNECT host '/' port ( username password )? -> ^( NODE_CONNECT host port ( username password )? ) | CONNECT ip_address '/' port ( username password )? -> ^( NODE_CONNECT ip_address port ( username password )? ) )
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==CONNECT) ) {
                int LA5_1 = input.LA(2);

                if ( (LA5_1==Identifier) ) {
                    alt5=1;
                }
                else if ( (LA5_1==IP_ADDRESS) ) {
                    alt5=2;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 5, 1, input);

                    throw nvae;
                }
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 5, 0, input);

                throw nvae;
            }
            switch (alt5) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:169:7: CONNECT host '/' port ( username password )?
                    {
                    CONNECT27=(Token)match(input,CONNECT,FOLLOW_CONNECT_in_connectStatement640); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_CONNECT.add(CONNECT27);

                    pushFollow(FOLLOW_host_in_connectStatement642);
                    host28=host();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_host.add(host28.getTree());
                    char_literal29=(Token)match(input,101,FOLLOW_101_in_connectStatement644); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_101.add(char_literal29);

                    pushFollow(FOLLOW_port_in_connectStatement646);
                    port30=port();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_port.add(port30.getTree());
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:169:29: ( username password )?
                    int alt3=2;
                    int LA3_0 = input.LA(1);

                    if ( (LA3_0==Identifier) ) {
                        alt3=1;
                    }
                    switch (alt3) {
                        case 1 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:169:30: username password
                            {
                            pushFollow(FOLLOW_username_in_connectStatement649);
                            username31=username();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_username.add(username31.getTree());
                            pushFollow(FOLLOW_password_in_connectStatement651);
                            password32=password();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_password.add(password32.getTree());

                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: username, port, password, host
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 170:9: -> ^( NODE_CONNECT host port ( username password )? )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:170:12: ^( NODE_CONNECT host port ( username password )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_CONNECT, "NODE_CONNECT"), root_1);

                        adaptor.addChild(root_1, stream_host.nextTree());
                        adaptor.addChild(root_1, stream_port.nextTree());
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:170:37: ( username password )?
                        if ( stream_username.hasNext()||stream_password.hasNext() ) {
                            adaptor.addChild(root_1, stream_username.nextTree());
                            adaptor.addChild(root_1, stream_password.nextTree());

                        }
                        stream_username.reset();
                        stream_password.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:171:7: CONNECT ip_address '/' port ( username password )?
                    {
                    CONNECT33=(Token)match(input,CONNECT,FOLLOW_CONNECT_in_connectStatement686); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_CONNECT.add(CONNECT33);

                    pushFollow(FOLLOW_ip_address_in_connectStatement688);
                    ip_address34=ip_address();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_ip_address.add(ip_address34.getTree());
                    char_literal35=(Token)match(input,101,FOLLOW_101_in_connectStatement690); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_101.add(char_literal35);

                    pushFollow(FOLLOW_port_in_connectStatement692);
                    port36=port();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_port.add(port36.getTree());
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:171:35: ( username password )?
                    int alt4=2;
                    int LA4_0 = input.LA(1);

                    if ( (LA4_0==Identifier) ) {
                        alt4=1;
                    }
                    switch (alt4) {
                        case 1 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:171:36: username password
                            {
                            pushFollow(FOLLOW_username_in_connectStatement695);
                            username37=username();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_username.add(username37.getTree());
                            pushFollow(FOLLOW_password_in_connectStatement697);
                            password38=password();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_password.add(password38.getTree());

                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: password, port, ip_address, username
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 172:9: -> ^( NODE_CONNECT ip_address port ( username password )? )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:172:12: ^( NODE_CONNECT ip_address port ( username password )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_CONNECT, "NODE_CONNECT"), root_1);

                        adaptor.addChild(root_1, stream_ip_address.nextTree());
                        adaptor.addChild(root_1, stream_port.nextTree());
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:172:43: ( username password )?
                        if ( stream_password.hasNext()||stream_username.hasNext() ) {
                            adaptor.addChild(root_1, stream_username.nextTree());
                            adaptor.addChild(root_1, stream_password.nextTree());

                        }
                        stream_password.reset();
                        stream_username.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "connectStatement"

    public static class helpStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "helpStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:175:1: helpStatement : ( HELP HELP -> ^( NODE_HELP NODE_HELP ) | HELP CONNECT -> ^( NODE_HELP NODE_CONNECT ) | HELP USE -> ^( NODE_HELP NODE_USE_TABLE ) | HELP DESCRIBE KEYSPACE -> ^( NODE_HELP NODE_DESCRIBE_TABLE ) | HELP DESCRIBE 'CLUSTER' -> ^( NODE_HELP NODE_DESCRIBE_CLUSTER ) | HELP EXIT -> ^( NODE_HELP NODE_EXIT ) | HELP QUIT -> ^( NODE_HELP NODE_EXIT ) | HELP SHOW 'CLUSTER NAME' -> ^( NODE_HELP NODE_SHOW_CLUSTER_NAME ) | HELP SHOW KEYSPACES -> ^( NODE_HELP NODE_SHOW_KEYSPACES ) | HELP SHOW API_VERSION -> ^( NODE_HELP NODE_SHOW_VERSION ) | HELP CREATE KEYSPACE -> ^( NODE_HELP NODE_ADD_KEYSPACE ) | HELP UPDATE KEYSPACE -> ^( NODE_HELP NODE_UPDATE_KEYSPACE ) | HELP CREATE COLUMN FAMILY -> ^( NODE_HELP NODE_ADD_COLUMN_FAMILY ) | HELP UPDATE COLUMN FAMILY -> ^( NODE_HELP NODE_UPDATE_COLUMN_FAMILY ) | HELP DROP KEYSPACE -> ^( NODE_HELP NODE_DEL_KEYSPACE ) | HELP DROP COLUMN FAMILY -> ^( NODE_HELP NODE_DEL_COLUMN_FAMILY ) | HELP DROP INDEX -> ^( NODE_HELP NODE_DROP_INDEX ) | HELP GET -> ^( NODE_HELP NODE_THRIFT_GET ) | HELP SET -> ^( NODE_HELP NODE_THRIFT_SET ) | HELP INCR -> ^( NODE_HELP NODE_THRIFT_INCR ) | HELP DECR -> ^( NODE_HELP NODE_THRIFT_DECR ) | HELP DEL -> ^( NODE_HELP NODE_THRIFT_DEL ) | HELP COUNT -> ^( NODE_HELP NODE_THRIFT_COUNT ) | HELP LIST -> ^( NODE_HELP NODE_LIST ) | HELP TRUNCATE -> ^( NODE_HELP NODE_TRUNCATE ) | HELP ASSUME -> ^( NODE_HELP NODE_ASSUME ) | HELP CONSISTENCYLEVEL -> ^( NODE_HELP NODE_CONSISTENCY_LEVEL ) | HELP -> ^( NODE_HELP ) | '?' -> ^( NODE_HELP ) );
    public final CliParser.helpStatement_return helpStatement() throws RecognitionException {
        CliParser.helpStatement_return retval = new CliParser.helpStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token HELP39=null;
        Token HELP40=null;
        Token HELP41=null;
        Token CONNECT42=null;
        Token HELP43=null;
        Token USE44=null;
        Token HELP45=null;
        Token DESCRIBE46=null;
        Token KEYSPACE47=null;
        Token HELP48=null;
        Token DESCRIBE49=null;
        Token string_literal50=null;
        Token HELP51=null;
        Token EXIT52=null;
        Token HELP53=null;
        Token QUIT54=null;
        Token HELP55=null;
        Token SHOW56=null;
        Token string_literal57=null;
        Token HELP58=null;
        Token SHOW59=null;
        Token KEYSPACES60=null;
        Token HELP61=null;
        Token SHOW62=null;
        Token API_VERSION63=null;
        Token HELP64=null;
        Token CREATE65=null;
        Token KEYSPACE66=null;
        Token HELP67=null;
        Token UPDATE68=null;
        Token KEYSPACE69=null;
        Token HELP70=null;
        Token CREATE71=null;
        Token COLUMN72=null;
        Token FAMILY73=null;
        Token HELP74=null;
        Token UPDATE75=null;
        Token COLUMN76=null;
        Token FAMILY77=null;
        Token HELP78=null;
        Token DROP79=null;
        Token KEYSPACE80=null;
        Token HELP81=null;
        Token DROP82=null;
        Token COLUMN83=null;
        Token FAMILY84=null;
        Token HELP85=null;
        Token DROP86=null;
        Token INDEX87=null;
        Token HELP88=null;
        Token GET89=null;
        Token HELP90=null;
        Token SET91=null;
        Token HELP92=null;
        Token INCR93=null;
        Token HELP94=null;
        Token DECR95=null;
        Token HELP96=null;
        Token DEL97=null;
        Token HELP98=null;
        Token COUNT99=null;
        Token HELP100=null;
        Token LIST101=null;
        Token HELP102=null;
        Token TRUNCATE103=null;
        Token HELP104=null;
        Token ASSUME105=null;
        Token HELP106=null;
        Token CONSISTENCYLEVEL107=null;
        Token HELP108=null;
        Token char_literal109=null;

        CommonTree HELP39_tree=null;
        CommonTree HELP40_tree=null;
        CommonTree HELP41_tree=null;
        CommonTree CONNECT42_tree=null;
        CommonTree HELP43_tree=null;
        CommonTree USE44_tree=null;
        CommonTree HELP45_tree=null;
        CommonTree DESCRIBE46_tree=null;
        CommonTree KEYSPACE47_tree=null;
        CommonTree HELP48_tree=null;
        CommonTree DESCRIBE49_tree=null;
        CommonTree string_literal50_tree=null;
        CommonTree HELP51_tree=null;
        CommonTree EXIT52_tree=null;
        CommonTree HELP53_tree=null;
        CommonTree QUIT54_tree=null;
        CommonTree HELP55_tree=null;
        CommonTree SHOW56_tree=null;
        CommonTree string_literal57_tree=null;
        CommonTree HELP58_tree=null;
        CommonTree SHOW59_tree=null;
        CommonTree KEYSPACES60_tree=null;
        CommonTree HELP61_tree=null;
        CommonTree SHOW62_tree=null;
        CommonTree API_VERSION63_tree=null;
        CommonTree HELP64_tree=null;
        CommonTree CREATE65_tree=null;
        CommonTree KEYSPACE66_tree=null;
        CommonTree HELP67_tree=null;
        CommonTree UPDATE68_tree=null;
        CommonTree KEYSPACE69_tree=null;
        CommonTree HELP70_tree=null;
        CommonTree CREATE71_tree=null;
        CommonTree COLUMN72_tree=null;
        CommonTree FAMILY73_tree=null;
        CommonTree HELP74_tree=null;
        CommonTree UPDATE75_tree=null;
        CommonTree COLUMN76_tree=null;
        CommonTree FAMILY77_tree=null;
        CommonTree HELP78_tree=null;
        CommonTree DROP79_tree=null;
        CommonTree KEYSPACE80_tree=null;
        CommonTree HELP81_tree=null;
        CommonTree DROP82_tree=null;
        CommonTree COLUMN83_tree=null;
        CommonTree FAMILY84_tree=null;
        CommonTree HELP85_tree=null;
        CommonTree DROP86_tree=null;
        CommonTree INDEX87_tree=null;
        CommonTree HELP88_tree=null;
        CommonTree GET89_tree=null;
        CommonTree HELP90_tree=null;
        CommonTree SET91_tree=null;
        CommonTree HELP92_tree=null;
        CommonTree INCR93_tree=null;
        CommonTree HELP94_tree=null;
        CommonTree DECR95_tree=null;
        CommonTree HELP96_tree=null;
        CommonTree DEL97_tree=null;
        CommonTree HELP98_tree=null;
        CommonTree COUNT99_tree=null;
        CommonTree HELP100_tree=null;
        CommonTree LIST101_tree=null;
        CommonTree HELP102_tree=null;
        CommonTree TRUNCATE103_tree=null;
        CommonTree HELP104_tree=null;
        CommonTree ASSUME105_tree=null;
        CommonTree HELP106_tree=null;
        CommonTree CONSISTENCYLEVEL107_tree=null;
        CommonTree HELP108_tree=null;
        CommonTree char_literal109_tree=null;
        RewriteRuleTokenStream stream_EXIT=new RewriteRuleTokenStream(adaptor,"token EXIT");
        RewriteRuleTokenStream stream_HELP=new RewriteRuleTokenStream(adaptor,"token HELP");
        RewriteRuleTokenStream stream_DEL=new RewriteRuleTokenStream(adaptor,"token DEL");
        RewriteRuleTokenStream stream_UPDATE=new RewriteRuleTokenStream(adaptor,"token UPDATE");
        RewriteRuleTokenStream stream_SET=new RewriteRuleTokenStream(adaptor,"token SET");
        RewriteRuleTokenStream stream_COUNT=new RewriteRuleTokenStream(adaptor,"token COUNT");
        RewriteRuleTokenStream stream_KEYSPACES=new RewriteRuleTokenStream(adaptor,"token KEYSPACES");
        RewriteRuleTokenStream stream_API_VERSION=new RewriteRuleTokenStream(adaptor,"token API_VERSION");
        RewriteRuleTokenStream stream_CONSISTENCYLEVEL=new RewriteRuleTokenStream(adaptor,"token CONSISTENCYLEVEL");
        RewriteRuleTokenStream stream_LIST=new RewriteRuleTokenStream(adaptor,"token LIST");
        RewriteRuleTokenStream stream_104=new RewriteRuleTokenStream(adaptor,"token 104");
        RewriteRuleTokenStream stream_103=new RewriteRuleTokenStream(adaptor,"token 103");
        RewriteRuleTokenStream stream_102=new RewriteRuleTokenStream(adaptor,"token 102");
        RewriteRuleTokenStream stream_QUIT=new RewriteRuleTokenStream(adaptor,"token QUIT");
        RewriteRuleTokenStream stream_KEYSPACE=new RewriteRuleTokenStream(adaptor,"token KEYSPACE");
        RewriteRuleTokenStream stream_INDEX=new RewriteRuleTokenStream(adaptor,"token INDEX");
        RewriteRuleTokenStream stream_CREATE=new RewriteRuleTokenStream(adaptor,"token CREATE");
        RewriteRuleTokenStream stream_CONNECT=new RewriteRuleTokenStream(adaptor,"token CONNECT");
        RewriteRuleTokenStream stream_DROP=new RewriteRuleTokenStream(adaptor,"token DROP");
        RewriteRuleTokenStream stream_INCR=new RewriteRuleTokenStream(adaptor,"token INCR");
        RewriteRuleTokenStream stream_ASSUME=new RewriteRuleTokenStream(adaptor,"token ASSUME");
        RewriteRuleTokenStream stream_TRUNCATE=new RewriteRuleTokenStream(adaptor,"token TRUNCATE");
        RewriteRuleTokenStream stream_DESCRIBE=new RewriteRuleTokenStream(adaptor,"token DESCRIBE");
        RewriteRuleTokenStream stream_COLUMN=new RewriteRuleTokenStream(adaptor,"token COLUMN");
        RewriteRuleTokenStream stream_FAMILY=new RewriteRuleTokenStream(adaptor,"token FAMILY");
        RewriteRuleTokenStream stream_DECR=new RewriteRuleTokenStream(adaptor,"token DECR");
        RewriteRuleTokenStream stream_GET=new RewriteRuleTokenStream(adaptor,"token GET");
        RewriteRuleTokenStream stream_USE=new RewriteRuleTokenStream(adaptor,"token USE");
        RewriteRuleTokenStream stream_SHOW=new RewriteRuleTokenStream(adaptor,"token SHOW");

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:176:5: ( HELP HELP -> ^( NODE_HELP NODE_HELP ) | HELP CONNECT -> ^( NODE_HELP NODE_CONNECT ) | HELP USE -> ^( NODE_HELP NODE_USE_TABLE ) | HELP DESCRIBE KEYSPACE -> ^( NODE_HELP NODE_DESCRIBE_TABLE ) | HELP DESCRIBE 'CLUSTER' -> ^( NODE_HELP NODE_DESCRIBE_CLUSTER ) | HELP EXIT -> ^( NODE_HELP NODE_EXIT ) | HELP QUIT -> ^( NODE_HELP NODE_EXIT ) | HELP SHOW 'CLUSTER NAME' -> ^( NODE_HELP NODE_SHOW_CLUSTER_NAME ) | HELP SHOW KEYSPACES -> ^( NODE_HELP NODE_SHOW_KEYSPACES ) | HELP SHOW API_VERSION -> ^( NODE_HELP NODE_SHOW_VERSION ) | HELP CREATE KEYSPACE -> ^( NODE_HELP NODE_ADD_KEYSPACE ) | HELP UPDATE KEYSPACE -> ^( NODE_HELP NODE_UPDATE_KEYSPACE ) | HELP CREATE COLUMN FAMILY -> ^( NODE_HELP NODE_ADD_COLUMN_FAMILY ) | HELP UPDATE COLUMN FAMILY -> ^( NODE_HELP NODE_UPDATE_COLUMN_FAMILY ) | HELP DROP KEYSPACE -> ^( NODE_HELP NODE_DEL_KEYSPACE ) | HELP DROP COLUMN FAMILY -> ^( NODE_HELP NODE_DEL_COLUMN_FAMILY ) | HELP DROP INDEX -> ^( NODE_HELP NODE_DROP_INDEX ) | HELP GET -> ^( NODE_HELP NODE_THRIFT_GET ) | HELP SET -> ^( NODE_HELP NODE_THRIFT_SET ) | HELP INCR -> ^( NODE_HELP NODE_THRIFT_INCR ) | HELP DECR -> ^( NODE_HELP NODE_THRIFT_DECR ) | HELP DEL -> ^( NODE_HELP NODE_THRIFT_DEL ) | HELP COUNT -> ^( NODE_HELP NODE_THRIFT_COUNT ) | HELP LIST -> ^( NODE_HELP NODE_LIST ) | HELP TRUNCATE -> ^( NODE_HELP NODE_TRUNCATE ) | HELP ASSUME -> ^( NODE_HELP NODE_ASSUME ) | HELP CONSISTENCYLEVEL -> ^( NODE_HELP NODE_CONSISTENCY_LEVEL ) | HELP -> ^( NODE_HELP ) | '?' -> ^( NODE_HELP ) )
            int alt6=29;
            alt6 = dfa6.predict(input);
            switch (alt6) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:176:7: HELP HELP
                    {
                    HELP39=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement741); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP39);

                    HELP40=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement743); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP40);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 177:9: -> ^( NODE_HELP NODE_HELP )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:177:12: ^( NODE_HELP NODE_HELP )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:178:7: HELP CONNECT
                    {
                    HELP41=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement768); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP41);

                    CONNECT42=(Token)match(input,CONNECT,FOLLOW_CONNECT_in_helpStatement770); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_CONNECT.add(CONNECT42);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 179:9: -> ^( NODE_HELP NODE_CONNECT )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:179:12: ^( NODE_HELP NODE_CONNECT )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_CONNECT, "NODE_CONNECT"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:180:7: HELP USE
                    {
                    HELP43=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement795); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP43);

                    USE44=(Token)match(input,USE,FOLLOW_USE_in_helpStatement797); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_USE.add(USE44);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 181:9: -> ^( NODE_HELP NODE_USE_TABLE )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:181:12: ^( NODE_HELP NODE_USE_TABLE )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_USE_TABLE, "NODE_USE_TABLE"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 4 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:182:7: HELP DESCRIBE KEYSPACE
                    {
                    HELP45=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement822); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP45);

                    DESCRIBE46=(Token)match(input,DESCRIBE,FOLLOW_DESCRIBE_in_helpStatement824); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_DESCRIBE.add(DESCRIBE46);

                    KEYSPACE47=(Token)match(input,KEYSPACE,FOLLOW_KEYSPACE_in_helpStatement826); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KEYSPACE.add(KEYSPACE47);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 183:9: -> ^( NODE_HELP NODE_DESCRIBE_TABLE )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:183:12: ^( NODE_HELP NODE_DESCRIBE_TABLE )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_DESCRIBE_TABLE, "NODE_DESCRIBE_TABLE"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 5 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:184:7: HELP DESCRIBE 'CLUSTER'
                    {
                    HELP48=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement851); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP48);

                    DESCRIBE49=(Token)match(input,DESCRIBE,FOLLOW_DESCRIBE_in_helpStatement853); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_DESCRIBE.add(DESCRIBE49);

                    string_literal50=(Token)match(input,102,FOLLOW_102_in_helpStatement855); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_102.add(string_literal50);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 185:9: -> ^( NODE_HELP NODE_DESCRIBE_CLUSTER )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:185:12: ^( NODE_HELP NODE_DESCRIBE_CLUSTER )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_DESCRIBE_CLUSTER, "NODE_DESCRIBE_CLUSTER"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 6 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:186:7: HELP EXIT
                    {
                    HELP51=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement879); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP51);

                    EXIT52=(Token)match(input,EXIT,FOLLOW_EXIT_in_helpStatement881); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_EXIT.add(EXIT52);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 187:9: -> ^( NODE_HELP NODE_EXIT )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:187:12: ^( NODE_HELP NODE_EXIT )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_EXIT, "NODE_EXIT"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 7 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:188:7: HELP QUIT
                    {
                    HELP53=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement906); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP53);

                    QUIT54=(Token)match(input,QUIT,FOLLOW_QUIT_in_helpStatement908); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_QUIT.add(QUIT54);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 189:9: -> ^( NODE_HELP NODE_EXIT )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:189:12: ^( NODE_HELP NODE_EXIT )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_EXIT, "NODE_EXIT"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 8 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:190:7: HELP SHOW 'CLUSTER NAME'
                    {
                    HELP55=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement933); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP55);

                    SHOW56=(Token)match(input,SHOW,FOLLOW_SHOW_in_helpStatement935); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_SHOW.add(SHOW56);

                    string_literal57=(Token)match(input,103,FOLLOW_103_in_helpStatement937); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_103.add(string_literal57);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 191:9: -> ^( NODE_HELP NODE_SHOW_CLUSTER_NAME )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:191:12: ^( NODE_HELP NODE_SHOW_CLUSTER_NAME )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_SHOW_CLUSTER_NAME, "NODE_SHOW_CLUSTER_NAME"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 9 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:192:7: HELP SHOW KEYSPACES
                    {
                    HELP58=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement961); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP58);

                    SHOW59=(Token)match(input,SHOW,FOLLOW_SHOW_in_helpStatement963); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_SHOW.add(SHOW59);

                    KEYSPACES60=(Token)match(input,KEYSPACES,FOLLOW_KEYSPACES_in_helpStatement965); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KEYSPACES.add(KEYSPACES60);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 193:9: -> ^( NODE_HELP NODE_SHOW_KEYSPACES )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:193:12: ^( NODE_HELP NODE_SHOW_KEYSPACES )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_SHOW_KEYSPACES, "NODE_SHOW_KEYSPACES"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 10 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:194:7: HELP SHOW API_VERSION
                    {
                    HELP61=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement990); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP61);

                    SHOW62=(Token)match(input,SHOW,FOLLOW_SHOW_in_helpStatement992); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_SHOW.add(SHOW62);

                    API_VERSION63=(Token)match(input,API_VERSION,FOLLOW_API_VERSION_in_helpStatement994); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_API_VERSION.add(API_VERSION63);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 195:9: -> ^( NODE_HELP NODE_SHOW_VERSION )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:195:12: ^( NODE_HELP NODE_SHOW_VERSION )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_SHOW_VERSION, "NODE_SHOW_VERSION"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 11 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:196:7: HELP CREATE KEYSPACE
                    {
                    HELP64=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1018); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP64);

                    CREATE65=(Token)match(input,CREATE,FOLLOW_CREATE_in_helpStatement1020); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_CREATE.add(CREATE65);

                    KEYSPACE66=(Token)match(input,KEYSPACE,FOLLOW_KEYSPACE_in_helpStatement1022); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KEYSPACE.add(KEYSPACE66);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 197:9: -> ^( NODE_HELP NODE_ADD_KEYSPACE )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:197:12: ^( NODE_HELP NODE_ADD_KEYSPACE )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_ADD_KEYSPACE, "NODE_ADD_KEYSPACE"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 12 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:198:7: HELP UPDATE KEYSPACE
                    {
                    HELP67=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1047); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP67);

                    UPDATE68=(Token)match(input,UPDATE,FOLLOW_UPDATE_in_helpStatement1049); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_UPDATE.add(UPDATE68);

                    KEYSPACE69=(Token)match(input,KEYSPACE,FOLLOW_KEYSPACE_in_helpStatement1051); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KEYSPACE.add(KEYSPACE69);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 199:9: -> ^( NODE_HELP NODE_UPDATE_KEYSPACE )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:199:12: ^( NODE_HELP NODE_UPDATE_KEYSPACE )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_UPDATE_KEYSPACE, "NODE_UPDATE_KEYSPACE"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 13 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:200:7: HELP CREATE COLUMN FAMILY
                    {
                    HELP70=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1075); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP70);

                    CREATE71=(Token)match(input,CREATE,FOLLOW_CREATE_in_helpStatement1077); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_CREATE.add(CREATE71);

                    COLUMN72=(Token)match(input,COLUMN,FOLLOW_COLUMN_in_helpStatement1079); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_COLUMN.add(COLUMN72);

                    FAMILY73=(Token)match(input,FAMILY,FOLLOW_FAMILY_in_helpStatement1081); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_FAMILY.add(FAMILY73);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 201:9: -> ^( NODE_HELP NODE_ADD_COLUMN_FAMILY )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:201:12: ^( NODE_HELP NODE_ADD_COLUMN_FAMILY )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_ADD_COLUMN_FAMILY, "NODE_ADD_COLUMN_FAMILY"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 14 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:202:7: HELP UPDATE COLUMN FAMILY
                    {
                    HELP74=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1106); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP74);

                    UPDATE75=(Token)match(input,UPDATE,FOLLOW_UPDATE_in_helpStatement1108); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_UPDATE.add(UPDATE75);

                    COLUMN76=(Token)match(input,COLUMN,FOLLOW_COLUMN_in_helpStatement1110); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_COLUMN.add(COLUMN76);

                    FAMILY77=(Token)match(input,FAMILY,FOLLOW_FAMILY_in_helpStatement1112); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_FAMILY.add(FAMILY77);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 203:9: -> ^( NODE_HELP NODE_UPDATE_COLUMN_FAMILY )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:203:12: ^( NODE_HELP NODE_UPDATE_COLUMN_FAMILY )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_UPDATE_COLUMN_FAMILY, "NODE_UPDATE_COLUMN_FAMILY"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 15 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:204:7: HELP DROP KEYSPACE
                    {
                    HELP78=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1136); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP78);

                    DROP79=(Token)match(input,DROP,FOLLOW_DROP_in_helpStatement1138); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_DROP.add(DROP79);

                    KEYSPACE80=(Token)match(input,KEYSPACE,FOLLOW_KEYSPACE_in_helpStatement1140); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KEYSPACE.add(KEYSPACE80);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 205:9: -> ^( NODE_HELP NODE_DEL_KEYSPACE )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:205:12: ^( NODE_HELP NODE_DEL_KEYSPACE )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_DEL_KEYSPACE, "NODE_DEL_KEYSPACE"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 16 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:206:7: HELP DROP COLUMN FAMILY
                    {
                    HELP81=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1165); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP81);

                    DROP82=(Token)match(input,DROP,FOLLOW_DROP_in_helpStatement1167); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_DROP.add(DROP82);

                    COLUMN83=(Token)match(input,COLUMN,FOLLOW_COLUMN_in_helpStatement1169); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_COLUMN.add(COLUMN83);

                    FAMILY84=(Token)match(input,FAMILY,FOLLOW_FAMILY_in_helpStatement1171); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_FAMILY.add(FAMILY84);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 207:9: -> ^( NODE_HELP NODE_DEL_COLUMN_FAMILY )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:207:12: ^( NODE_HELP NODE_DEL_COLUMN_FAMILY )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_DEL_COLUMN_FAMILY, "NODE_DEL_COLUMN_FAMILY"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 17 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:208:7: HELP DROP INDEX
                    {
                    HELP85=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1196); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP85);

                    DROP86=(Token)match(input,DROP,FOLLOW_DROP_in_helpStatement1198); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_DROP.add(DROP86);

                    INDEX87=(Token)match(input,INDEX,FOLLOW_INDEX_in_helpStatement1200); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_INDEX.add(INDEX87);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 209:9: -> ^( NODE_HELP NODE_DROP_INDEX )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:209:12: ^( NODE_HELP NODE_DROP_INDEX )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_DROP_INDEX, "NODE_DROP_INDEX"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 18 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:210:7: HELP GET
                    {
                    HELP88=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1224); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP88);

                    GET89=(Token)match(input,GET,FOLLOW_GET_in_helpStatement1226); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_GET.add(GET89);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 211:9: -> ^( NODE_HELP NODE_THRIFT_GET )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:211:12: ^( NODE_HELP NODE_THRIFT_GET )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_THRIFT_GET, "NODE_THRIFT_GET"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 19 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:212:7: HELP SET
                    {
                    HELP90=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1251); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP90);

                    SET91=(Token)match(input,SET,FOLLOW_SET_in_helpStatement1253); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_SET.add(SET91);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 213:9: -> ^( NODE_HELP NODE_THRIFT_SET )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:213:12: ^( NODE_HELP NODE_THRIFT_SET )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_THRIFT_SET, "NODE_THRIFT_SET"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 20 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:214:7: HELP INCR
                    {
                    HELP92=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1278); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP92);

                    INCR93=(Token)match(input,INCR,FOLLOW_INCR_in_helpStatement1280); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_INCR.add(INCR93);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 215:9: -> ^( NODE_HELP NODE_THRIFT_INCR )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:215:12: ^( NODE_HELP NODE_THRIFT_INCR )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_THRIFT_INCR, "NODE_THRIFT_INCR"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 21 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:216:7: HELP DECR
                    {
                    HELP94=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1304); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP94);

                    DECR95=(Token)match(input,DECR,FOLLOW_DECR_in_helpStatement1306); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_DECR.add(DECR95);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 217:9: -> ^( NODE_HELP NODE_THRIFT_DECR )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:217:12: ^( NODE_HELP NODE_THRIFT_DECR )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_THRIFT_DECR, "NODE_THRIFT_DECR"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 22 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:218:7: HELP DEL
                    {
                    HELP96=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1330); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP96);

                    DEL97=(Token)match(input,DEL,FOLLOW_DEL_in_helpStatement1332); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_DEL.add(DEL97);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 219:9: -> ^( NODE_HELP NODE_THRIFT_DEL )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:219:12: ^( NODE_HELP NODE_THRIFT_DEL )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_THRIFT_DEL, "NODE_THRIFT_DEL"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 23 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:220:7: HELP COUNT
                    {
                    HELP98=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1357); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP98);

                    COUNT99=(Token)match(input,COUNT,FOLLOW_COUNT_in_helpStatement1359); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_COUNT.add(COUNT99);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 221:9: -> ^( NODE_HELP NODE_THRIFT_COUNT )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:221:12: ^( NODE_HELP NODE_THRIFT_COUNT )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_THRIFT_COUNT, "NODE_THRIFT_COUNT"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 24 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:222:7: HELP LIST
                    {
                    HELP100=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1384); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP100);

                    LIST101=(Token)match(input,LIST,FOLLOW_LIST_in_helpStatement1386); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LIST.add(LIST101);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 223:9: -> ^( NODE_HELP NODE_LIST )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:223:12: ^( NODE_HELP NODE_LIST )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_LIST, "NODE_LIST"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 25 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:224:7: HELP TRUNCATE
                    {
                    HELP102=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1411); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP102);

                    TRUNCATE103=(Token)match(input,TRUNCATE,FOLLOW_TRUNCATE_in_helpStatement1413); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_TRUNCATE.add(TRUNCATE103);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 225:9: -> ^( NODE_HELP NODE_TRUNCATE )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:225:12: ^( NODE_HELP NODE_TRUNCATE )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_TRUNCATE, "NODE_TRUNCATE"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 26 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:226:7: HELP ASSUME
                    {
                    HELP104=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1437); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP104);

                    ASSUME105=(Token)match(input,ASSUME,FOLLOW_ASSUME_in_helpStatement1439); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ASSUME.add(ASSUME105);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 227:9: -> ^( NODE_HELP NODE_ASSUME )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:227:12: ^( NODE_HELP NODE_ASSUME )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_ASSUME, "NODE_ASSUME"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 27 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:228:7: HELP CONSISTENCYLEVEL
                    {
                    HELP106=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1463); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP106);

                    CONSISTENCYLEVEL107=(Token)match(input,CONSISTENCYLEVEL,FOLLOW_CONSISTENCYLEVEL_in_helpStatement1465); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_CONSISTENCYLEVEL.add(CONSISTENCYLEVEL107);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 229:9: -> ^( NODE_HELP NODE_CONSISTENCY_LEVEL )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:229:12: ^( NODE_HELP NODE_CONSISTENCY_LEVEL )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_1, (CommonTree)adaptor.create(NODE_CONSISTENCY_LEVEL, "NODE_CONSISTENCY_LEVEL"));

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 28 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:230:7: HELP
                    {
                    HELP108=(Token)match(input,HELP,FOLLOW_HELP_in_helpStatement1489); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_HELP.add(HELP108);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 231:9: -> ^( NODE_HELP )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:231:12: ^( NODE_HELP )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 29 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:232:7: '?'
                    {
                    char_literal109=(Token)match(input,104,FOLLOW_104_in_helpStatement1512); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_104.add(char_literal109);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 233:9: -> ^( NODE_HELP )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:233:12: ^( NODE_HELP )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "helpStatement"

    public static class exitStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "exitStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:236:1: exitStatement : ( QUIT -> ^( NODE_EXIT ) | EXIT -> ^( NODE_EXIT ) );
    public final CliParser.exitStatement_return exitStatement() throws RecognitionException {
        CliParser.exitStatement_return retval = new CliParser.exitStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token QUIT110=null;
        Token EXIT111=null;

        CommonTree QUIT110_tree=null;
        CommonTree EXIT111_tree=null;
        RewriteRuleTokenStream stream_EXIT=new RewriteRuleTokenStream(adaptor,"token EXIT");
        RewriteRuleTokenStream stream_QUIT=new RewriteRuleTokenStream(adaptor,"token QUIT");

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:237:5: ( QUIT -> ^( NODE_EXIT ) | EXIT -> ^( NODE_EXIT ) )
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==QUIT) ) {
                alt7=1;
            }
            else if ( (LA7_0==EXIT) ) {
                alt7=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 7, 0, input);

                throw nvae;
            }
            switch (alt7) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:237:7: QUIT
                    {
                    QUIT110=(Token)match(input,QUIT,FOLLOW_QUIT_in_exitStatement1547); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_QUIT.add(QUIT110);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 237:12: -> ^( NODE_EXIT )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:237:15: ^( NODE_EXIT )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_EXIT, "NODE_EXIT"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:238:7: EXIT
                    {
                    EXIT111=(Token)match(input,EXIT,FOLLOW_EXIT_in_exitStatement1561); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_EXIT.add(EXIT111);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 238:12: -> ^( NODE_EXIT )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:238:15: ^( NODE_EXIT )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_EXIT, "NODE_EXIT"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "exitStatement"

    public static class getStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "getStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:241:1: getStatement : ( GET columnFamilyExpr ( 'AS' typeIdentifier )? ( 'LIMIT' limit= IntegerPositiveLiteral )? -> ^( NODE_THRIFT_GET columnFamilyExpr ( ^( CONVERT_TO_TYPE typeIdentifier ) )? ( ^( NODE_LIMIT $limit) )? ) | GET columnFamily 'WHERE' getCondition ( 'AND' getCondition )* ( 'LIMIT' limit= IntegerPositiveLiteral )? -> ^( NODE_THRIFT_GET_WITH_CONDITIONS columnFamily ^( CONDITIONS ( getCondition )+ ) ( ^( NODE_LIMIT $limit) )? ) );
    public final CliParser.getStatement_return getStatement() throws RecognitionException {
        CliParser.getStatement_return retval = new CliParser.getStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token limit=null;
        Token GET112=null;
        Token string_literal114=null;
        Token string_literal116=null;
        Token GET117=null;
        Token string_literal119=null;
        Token string_literal121=null;
        Token string_literal123=null;
        CliParser.columnFamilyExpr_return columnFamilyExpr113 = null;

        CliParser.typeIdentifier_return typeIdentifier115 = null;

        CliParser.columnFamily_return columnFamily118 = null;

        CliParser.getCondition_return getCondition120 = null;

        CliParser.getCondition_return getCondition122 = null;


        CommonTree limit_tree=null;
        CommonTree GET112_tree=null;
        CommonTree string_literal114_tree=null;
        CommonTree string_literal116_tree=null;
        CommonTree GET117_tree=null;
        CommonTree string_literal119_tree=null;
        CommonTree string_literal121_tree=null;
        CommonTree string_literal123_tree=null;
        RewriteRuleTokenStream stream_IntegerPositiveLiteral=new RewriteRuleTokenStream(adaptor,"token IntegerPositiveLiteral");
        RewriteRuleTokenStream stream_GET=new RewriteRuleTokenStream(adaptor,"token GET");
        RewriteRuleTokenStream stream_AND=new RewriteRuleTokenStream(adaptor,"token AND");
        RewriteRuleTokenStream stream_106=new RewriteRuleTokenStream(adaptor,"token 106");
        RewriteRuleTokenStream stream_105=new RewriteRuleTokenStream(adaptor,"token 105");
        RewriteRuleTokenStream stream_LIMIT=new RewriteRuleTokenStream(adaptor,"token LIMIT");
        RewriteRuleSubtreeStream stream_typeIdentifier=new RewriteRuleSubtreeStream(adaptor,"rule typeIdentifier");
        RewriteRuleSubtreeStream stream_getCondition=new RewriteRuleSubtreeStream(adaptor,"rule getCondition");
        RewriteRuleSubtreeStream stream_columnFamilyExpr=new RewriteRuleSubtreeStream(adaptor,"rule columnFamilyExpr");
        RewriteRuleSubtreeStream stream_columnFamily=new RewriteRuleSubtreeStream(adaptor,"rule columnFamily");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:242:5: ( GET columnFamilyExpr ( 'AS' typeIdentifier )? ( 'LIMIT' limit= IntegerPositiveLiteral )? -> ^( NODE_THRIFT_GET columnFamilyExpr ( ^( CONVERT_TO_TYPE typeIdentifier ) )? ( ^( NODE_LIMIT $limit) )? ) | GET columnFamily 'WHERE' getCondition ( 'AND' getCondition )* ( 'LIMIT' limit= IntegerPositiveLiteral )? -> ^( NODE_THRIFT_GET_WITH_CONDITIONS columnFamily ^( CONDITIONS ( getCondition )+ ) ( ^( NODE_LIMIT $limit) )? ) )
            int alt12=2;
            int LA12_0 = input.LA(1);

            if ( (LA12_0==GET) ) {
                int LA12_1 = input.LA(2);

                if ( (LA12_1==Identifier) ) {
                    int LA12_2 = input.LA(3);

                    if ( (LA12_2==113) ) {
                        alt12=1;
                    }
                    else if ( (LA12_2==106) ) {
                        alt12=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 12, 2, input);

                        throw nvae;
                    }
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 12, 1, input);

                    throw nvae;
                }
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 12, 0, input);

                throw nvae;
            }
            switch (alt12) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:242:7: GET columnFamilyExpr ( 'AS' typeIdentifier )? ( 'LIMIT' limit= IntegerPositiveLiteral )?
                    {
                    GET112=(Token)match(input,GET,FOLLOW_GET_in_getStatement1584); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_GET.add(GET112);

                    pushFollow(FOLLOW_columnFamilyExpr_in_getStatement1586);
                    columnFamilyExpr113=columnFamilyExpr();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_columnFamilyExpr.add(columnFamilyExpr113.getTree());
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:242:28: ( 'AS' typeIdentifier )?
                    int alt8=2;
                    int LA8_0 = input.LA(1);

                    if ( (LA8_0==105) ) {
                        alt8=1;
                    }
                    switch (alt8) {
                        case 1 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:242:29: 'AS' typeIdentifier
                            {
                            string_literal114=(Token)match(input,105,FOLLOW_105_in_getStatement1589); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_105.add(string_literal114);

                            pushFollow(FOLLOW_typeIdentifier_in_getStatement1591);
                            typeIdentifier115=typeIdentifier();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_typeIdentifier.add(typeIdentifier115.getTree());

                            }
                            break;

                    }

                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:242:51: ( 'LIMIT' limit= IntegerPositiveLiteral )?
                    int alt9=2;
                    int LA9_0 = input.LA(1);

                    if ( (LA9_0==LIMIT) ) {
                        alt9=1;
                    }
                    switch (alt9) {
                        case 1 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:242:52: 'LIMIT' limit= IntegerPositiveLiteral
                            {
                            string_literal116=(Token)match(input,LIMIT,FOLLOW_LIMIT_in_getStatement1596); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_LIMIT.add(string_literal116);

                            limit=(Token)match(input,IntegerPositiveLiteral,FOLLOW_IntegerPositiveLiteral_in_getStatement1600); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_IntegerPositiveLiteral.add(limit);


                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: columnFamilyExpr, limit, typeIdentifier
                    // token labels: limit
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleTokenStream stream_limit=new RewriteRuleTokenStream(adaptor,"token limit",limit);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 243:9: -> ^( NODE_THRIFT_GET columnFamilyExpr ( ^( CONVERT_TO_TYPE typeIdentifier ) )? ( ^( NODE_LIMIT $limit) )? )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:243:12: ^( NODE_THRIFT_GET columnFamilyExpr ( ^( CONVERT_TO_TYPE typeIdentifier ) )? ( ^( NODE_LIMIT $limit) )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_THRIFT_GET, "NODE_THRIFT_GET"), root_1);

                        adaptor.addChild(root_1, stream_columnFamilyExpr.nextTree());
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:243:47: ( ^( CONVERT_TO_TYPE typeIdentifier ) )?
                        if ( stream_typeIdentifier.hasNext() ) {
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:243:49: ^( CONVERT_TO_TYPE typeIdentifier )
                            {
                            CommonTree root_2 = (CommonTree)adaptor.nil();
                            root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(CONVERT_TO_TYPE, "CONVERT_TO_TYPE"), root_2);

                            adaptor.addChild(root_2, stream_typeIdentifier.nextTree());

                            adaptor.addChild(root_1, root_2);
                            }

                        }
                        stream_typeIdentifier.reset();
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:243:86: ( ^( NODE_LIMIT $limit) )?
                        if ( stream_limit.hasNext() ) {
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:243:86: ^( NODE_LIMIT $limit)
                            {
                            CommonTree root_2 = (CommonTree)adaptor.nil();
                            root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_LIMIT, "NODE_LIMIT"), root_2);

                            adaptor.addChild(root_2, stream_limit.nextNode());

                            adaptor.addChild(root_1, root_2);
                            }

                        }
                        stream_limit.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:244:7: GET columnFamily 'WHERE' getCondition ( 'AND' getCondition )* ( 'LIMIT' limit= IntegerPositiveLiteral )?
                    {
                    GET117=(Token)match(input,GET,FOLLOW_GET_in_getStatement1645); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_GET.add(GET117);

                    pushFollow(FOLLOW_columnFamily_in_getStatement1647);
                    columnFamily118=columnFamily();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_columnFamily.add(columnFamily118.getTree());
                    string_literal119=(Token)match(input,106,FOLLOW_106_in_getStatement1649); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_106.add(string_literal119);

                    pushFollow(FOLLOW_getCondition_in_getStatement1651);
                    getCondition120=getCondition();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_getCondition.add(getCondition120.getTree());
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:244:45: ( 'AND' getCondition )*
                    loop10:
                    do {
                        int alt10=2;
                        int LA10_0 = input.LA(1);

                        if ( (LA10_0==AND) ) {
                            alt10=1;
                        }


                        switch (alt10) {
                    	case 1 :
                    	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:244:46: 'AND' getCondition
                    	    {
                    	    string_literal121=(Token)match(input,AND,FOLLOW_AND_in_getStatement1654); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_AND.add(string_literal121);

                    	    pushFollow(FOLLOW_getCondition_in_getStatement1656);
                    	    getCondition122=getCondition();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_getCondition.add(getCondition122.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop10;
                        }
                    } while (true);

                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:244:67: ( 'LIMIT' limit= IntegerPositiveLiteral )?
                    int alt11=2;
                    int LA11_0 = input.LA(1);

                    if ( (LA11_0==LIMIT) ) {
                        alt11=1;
                    }
                    switch (alt11) {
                        case 1 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:244:68: 'LIMIT' limit= IntegerPositiveLiteral
                            {
                            string_literal123=(Token)match(input,LIMIT,FOLLOW_LIMIT_in_getStatement1661); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_LIMIT.add(string_literal123);

                            limit=(Token)match(input,IntegerPositiveLiteral,FOLLOW_IntegerPositiveLiteral_in_getStatement1665); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_IntegerPositiveLiteral.add(limit);


                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: columnFamily, getCondition, limit
                    // token labels: limit
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleTokenStream stream_limit=new RewriteRuleTokenStream(adaptor,"token limit",limit);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 245:9: -> ^( NODE_THRIFT_GET_WITH_CONDITIONS columnFamily ^( CONDITIONS ( getCondition )+ ) ( ^( NODE_LIMIT $limit) )? )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:245:12: ^( NODE_THRIFT_GET_WITH_CONDITIONS columnFamily ^( CONDITIONS ( getCondition )+ ) ( ^( NODE_LIMIT $limit) )? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_THRIFT_GET_WITH_CONDITIONS, "NODE_THRIFT_GET_WITH_CONDITIONS"), root_1);

                        adaptor.addChild(root_1, stream_columnFamily.nextTree());
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:245:59: ^( CONDITIONS ( getCondition )+ )
                        {
                        CommonTree root_2 = (CommonTree)adaptor.nil();
                        root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(CONDITIONS, "CONDITIONS"), root_2);

                        if ( !(stream_getCondition.hasNext()) ) {
                            throw new RewriteEarlyExitException();
                        }
                        while ( stream_getCondition.hasNext() ) {
                            adaptor.addChild(root_2, stream_getCondition.nextTree());

                        }
                        stream_getCondition.reset();

                        adaptor.addChild(root_1, root_2);
                        }
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:245:87: ( ^( NODE_LIMIT $limit) )?
                        if ( stream_limit.hasNext() ) {
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:245:87: ^( NODE_LIMIT $limit)
                            {
                            CommonTree root_2 = (CommonTree)adaptor.nil();
                            root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_LIMIT, "NODE_LIMIT"), root_2);

                            adaptor.addChild(root_2, stream_limit.nextNode());

                            adaptor.addChild(root_1, root_2);
                            }

                        }
                        stream_limit.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "getStatement"

    public static class getCondition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "getCondition"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:248:1: getCondition : columnOrSuperColumn operator value -> ^( CONDITION operator columnOrSuperColumn value ) ;
    public final CliParser.getCondition_return getCondition() throws RecognitionException {
        CliParser.getCondition_return retval = new CliParser.getCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CliParser.columnOrSuperColumn_return columnOrSuperColumn124 = null;

        CliParser.operator_return operator125 = null;

        CliParser.value_return value126 = null;


        RewriteRuleSubtreeStream stream_value=new RewriteRuleSubtreeStream(adaptor,"rule value");
        RewriteRuleSubtreeStream stream_columnOrSuperColumn=new RewriteRuleSubtreeStream(adaptor,"rule columnOrSuperColumn");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:249:5: ( columnOrSuperColumn operator value -> ^( CONDITION operator columnOrSuperColumn value ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:249:7: columnOrSuperColumn operator value
            {
            pushFollow(FOLLOW_columnOrSuperColumn_in_getCondition1716);
            columnOrSuperColumn124=columnOrSuperColumn();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_columnOrSuperColumn.add(columnOrSuperColumn124.getTree());
            pushFollow(FOLLOW_operator_in_getCondition1718);
            operator125=operator();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_operator.add(operator125.getTree());
            pushFollow(FOLLOW_value_in_getCondition1720);
            value126=value();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_value.add(value126.getTree());


            // AST REWRITE
            // elements: value, columnOrSuperColumn, operator
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 250:9: -> ^( CONDITION operator columnOrSuperColumn value )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:250:12: ^( CONDITION operator columnOrSuperColumn value )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(CONDITION, "CONDITION"), root_1);

                adaptor.addChild(root_1, stream_operator.nextTree());
                adaptor.addChild(root_1, stream_columnOrSuperColumn.nextTree());
                adaptor.addChild(root_1, stream_value.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "getCondition"

    public static class operator_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operator"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:253:1: operator : ( '=' | '>' | '<' | '>=' | '<=' );
    public final CliParser.operator_return operator() throws RecognitionException {
        CliParser.operator_return retval = new CliParser.operator_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set127=null;

        CommonTree set127_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:254:5: ( '=' | '>' | '<' | '>=' | '<=' )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:
            {
            root_0 = (CommonTree)adaptor.nil();

            set127=(Token)input.LT(1);
            if ( (input.LA(1)>=107 && input.LA(1)<=111) ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set127));
                state.errorRecovery=false;state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "operator"

    public static class typeIdentifier_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "typeIdentifier"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:257:1: typeIdentifier : ( Identifier | StringLiteral | IntegerPositiveLiteral );
    public final CliParser.typeIdentifier_return typeIdentifier() throws RecognitionException {
        CliParser.typeIdentifier_return retval = new CliParser.typeIdentifier_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set128=null;

        CommonTree set128_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:258:5: ( Identifier | StringLiteral | IntegerPositiveLiteral )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:
            {
            root_0 = (CommonTree)adaptor.nil();

            set128=(Token)input.LT(1);
            if ( (input.LA(1)>=IntegerPositiveLiteral && input.LA(1)<=StringLiteral) ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set128));
                state.errorRecovery=false;state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "typeIdentifier"

    public static class setStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "setStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:261:1: setStatement : SET columnFamilyExpr '=' objectValue= value ( WITH TTL '=' ttlValue= IntegerPositiveLiteral )? -> ^( NODE_THRIFT_SET columnFamilyExpr $objectValue ( $ttlValue)? ) ;
    public final CliParser.setStatement_return setStatement() throws RecognitionException {
        CliParser.setStatement_return retval = new CliParser.setStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ttlValue=null;
        Token SET129=null;
        Token char_literal131=null;
        Token WITH132=null;
        Token TTL133=null;
        Token char_literal134=null;
        CliParser.value_return objectValue = null;

        CliParser.columnFamilyExpr_return columnFamilyExpr130 = null;


        CommonTree ttlValue_tree=null;
        CommonTree SET129_tree=null;
        CommonTree char_literal131_tree=null;
        CommonTree WITH132_tree=null;
        CommonTree TTL133_tree=null;
        CommonTree char_literal134_tree=null;
        RewriteRuleTokenStream stream_IntegerPositiveLiteral=new RewriteRuleTokenStream(adaptor,"token IntegerPositiveLiteral");
        RewriteRuleTokenStream stream_SET=new RewriteRuleTokenStream(adaptor,"token SET");
        RewriteRuleTokenStream stream_107=new RewriteRuleTokenStream(adaptor,"token 107");
        RewriteRuleTokenStream stream_WITH=new RewriteRuleTokenStream(adaptor,"token WITH");
        RewriteRuleTokenStream stream_TTL=new RewriteRuleTokenStream(adaptor,"token TTL");
        RewriteRuleSubtreeStream stream_columnFamilyExpr=new RewriteRuleSubtreeStream(adaptor,"rule columnFamilyExpr");
        RewriteRuleSubtreeStream stream_value=new RewriteRuleSubtreeStream(adaptor,"rule value");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:262:5: ( SET columnFamilyExpr '=' objectValue= value ( WITH TTL '=' ttlValue= IntegerPositiveLiteral )? -> ^( NODE_THRIFT_SET columnFamilyExpr $objectValue ( $ttlValue)? ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:262:7: SET columnFamilyExpr '=' objectValue= value ( WITH TTL '=' ttlValue= IntegerPositiveLiteral )?
            {
            SET129=(Token)match(input,SET,FOLLOW_SET_in_setStatement1816); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_SET.add(SET129);

            pushFollow(FOLLOW_columnFamilyExpr_in_setStatement1818);
            columnFamilyExpr130=columnFamilyExpr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_columnFamilyExpr.add(columnFamilyExpr130.getTree());
            char_literal131=(Token)match(input,107,FOLLOW_107_in_setStatement1820); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_107.add(char_literal131);

            pushFollow(FOLLOW_value_in_setStatement1824);
            objectValue=value();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_value.add(objectValue.getTree());
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:262:50: ( WITH TTL '=' ttlValue= IntegerPositiveLiteral )?
            int alt13=2;
            int LA13_0 = input.LA(1);

            if ( (LA13_0==WITH) ) {
                alt13=1;
            }
            switch (alt13) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:262:51: WITH TTL '=' ttlValue= IntegerPositiveLiteral
                    {
                    WITH132=(Token)match(input,WITH,FOLLOW_WITH_in_setStatement1827); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_WITH.add(WITH132);

                    TTL133=(Token)match(input,TTL,FOLLOW_TTL_in_setStatement1829); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_TTL.add(TTL133);

                    char_literal134=(Token)match(input,107,FOLLOW_107_in_setStatement1831); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_107.add(char_literal134);

                    ttlValue=(Token)match(input,IntegerPositiveLiteral,FOLLOW_IntegerPositiveLiteral_in_setStatement1835); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_IntegerPositiveLiteral.add(ttlValue);


                    }
                    break;

            }



            // AST REWRITE
            // elements: columnFamilyExpr, objectValue, ttlValue
            // token labels: ttlValue
            // rule labels: retval, objectValue
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleTokenStream stream_ttlValue=new RewriteRuleTokenStream(adaptor,"token ttlValue",ttlValue);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_objectValue=new RewriteRuleSubtreeStream(adaptor,"rule objectValue",objectValue!=null?objectValue.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 263:9: -> ^( NODE_THRIFT_SET columnFamilyExpr $objectValue ( $ttlValue)? )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:263:12: ^( NODE_THRIFT_SET columnFamilyExpr $objectValue ( $ttlValue)? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_THRIFT_SET, "NODE_THRIFT_SET"), root_1);

                adaptor.addChild(root_1, stream_columnFamilyExpr.nextTree());
                adaptor.addChild(root_1, stream_objectValue.nextTree());
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:263:60: ( $ttlValue)?
                if ( stream_ttlValue.hasNext() ) {
                    adaptor.addChild(root_1, stream_ttlValue.nextNode());

                }
                stream_ttlValue.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "setStatement"

    public static class incrStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "incrStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:266:1: incrStatement : ( INCR columnFamilyExpr ( BY byValue= incrementValue )? -> ^( NODE_THRIFT_INCR columnFamilyExpr ( $byValue)? ) | DECR columnFamilyExpr ( BY byValue= incrementValue )? -> ^( NODE_THRIFT_DECR columnFamilyExpr ( $byValue)? ) );
    public final CliParser.incrStatement_return incrStatement() throws RecognitionException {
        CliParser.incrStatement_return retval = new CliParser.incrStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token INCR135=null;
        Token BY137=null;
        Token DECR138=null;
        Token BY140=null;
        CliParser.incrementValue_return byValue = null;

        CliParser.columnFamilyExpr_return columnFamilyExpr136 = null;

        CliParser.columnFamilyExpr_return columnFamilyExpr139 = null;


        CommonTree INCR135_tree=null;
        CommonTree BY137_tree=null;
        CommonTree DECR138_tree=null;
        CommonTree BY140_tree=null;
        RewriteRuleTokenStream stream_DECR=new RewriteRuleTokenStream(adaptor,"token DECR");
        RewriteRuleTokenStream stream_BY=new RewriteRuleTokenStream(adaptor,"token BY");
        RewriteRuleTokenStream stream_INCR=new RewriteRuleTokenStream(adaptor,"token INCR");
        RewriteRuleSubtreeStream stream_columnFamilyExpr=new RewriteRuleSubtreeStream(adaptor,"rule columnFamilyExpr");
        RewriteRuleSubtreeStream stream_incrementValue=new RewriteRuleSubtreeStream(adaptor,"rule incrementValue");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:267:5: ( INCR columnFamilyExpr ( BY byValue= incrementValue )? -> ^( NODE_THRIFT_INCR columnFamilyExpr ( $byValue)? ) | DECR columnFamilyExpr ( BY byValue= incrementValue )? -> ^( NODE_THRIFT_DECR columnFamilyExpr ( $byValue)? ) )
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( (LA16_0==INCR) ) {
                alt16=1;
            }
            else if ( (LA16_0==DECR) ) {
                alt16=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 16, 0, input);

                throw nvae;
            }
            switch (alt16) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:267:7: INCR columnFamilyExpr ( BY byValue= incrementValue )?
                    {
                    INCR135=(Token)match(input,INCR,FOLLOW_INCR_in_incrStatement1881); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_INCR.add(INCR135);

                    pushFollow(FOLLOW_columnFamilyExpr_in_incrStatement1883);
                    columnFamilyExpr136=columnFamilyExpr();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_columnFamilyExpr.add(columnFamilyExpr136.getTree());
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:267:29: ( BY byValue= incrementValue )?
                    int alt14=2;
                    int LA14_0 = input.LA(1);

                    if ( (LA14_0==BY) ) {
                        alt14=1;
                    }
                    switch (alt14) {
                        case 1 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:267:30: BY byValue= incrementValue
                            {
                            BY137=(Token)match(input,BY,FOLLOW_BY_in_incrStatement1886); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_BY.add(BY137);

                            pushFollow(FOLLOW_incrementValue_in_incrStatement1890);
                            byValue=incrementValue();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_incrementValue.add(byValue.getTree());

                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: byValue, columnFamilyExpr
                    // token labels: 
                    // rule labels: retval, byValue
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_byValue=new RewriteRuleSubtreeStream(adaptor,"rule byValue",byValue!=null?byValue.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 268:9: -> ^( NODE_THRIFT_INCR columnFamilyExpr ( $byValue)? )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:268:12: ^( NODE_THRIFT_INCR columnFamilyExpr ( $byValue)? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_THRIFT_INCR, "NODE_THRIFT_INCR"), root_1);

                        adaptor.addChild(root_1, stream_columnFamilyExpr.nextTree());
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:268:48: ( $byValue)?
                        if ( stream_byValue.hasNext() ) {
                            adaptor.addChild(root_1, stream_byValue.nextTree());

                        }
                        stream_byValue.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:269:7: DECR columnFamilyExpr ( BY byValue= incrementValue )?
                    {
                    DECR138=(Token)match(input,DECR,FOLLOW_DECR_in_incrStatement1924); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_DECR.add(DECR138);

                    pushFollow(FOLLOW_columnFamilyExpr_in_incrStatement1926);
                    columnFamilyExpr139=columnFamilyExpr();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_columnFamilyExpr.add(columnFamilyExpr139.getTree());
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:269:29: ( BY byValue= incrementValue )?
                    int alt15=2;
                    int LA15_0 = input.LA(1);

                    if ( (LA15_0==BY) ) {
                        alt15=1;
                    }
                    switch (alt15) {
                        case 1 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:269:30: BY byValue= incrementValue
                            {
                            BY140=(Token)match(input,BY,FOLLOW_BY_in_incrStatement1929); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_BY.add(BY140);

                            pushFollow(FOLLOW_incrementValue_in_incrStatement1933);
                            byValue=incrementValue();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_incrementValue.add(byValue.getTree());

                            }
                            break;

                    }



                    // AST REWRITE
                    // elements: byValue, columnFamilyExpr
                    // token labels: 
                    // rule labels: retval, byValue
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_byValue=new RewriteRuleSubtreeStream(adaptor,"rule byValue",byValue!=null?byValue.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 270:9: -> ^( NODE_THRIFT_DECR columnFamilyExpr ( $byValue)? )
                    {
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:270:12: ^( NODE_THRIFT_DECR columnFamilyExpr ( $byValue)? )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_THRIFT_DECR, "NODE_THRIFT_DECR"), root_1);

                        adaptor.addChild(root_1, stream_columnFamilyExpr.nextTree());
                        // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:270:48: ( $byValue)?
                        if ( stream_byValue.hasNext() ) {
                            adaptor.addChild(root_1, stream_byValue.nextTree());

                        }
                        stream_byValue.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "incrStatement"

    public static class countStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "countStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:273:1: countStatement : COUNT columnFamilyExpr -> ^( NODE_THRIFT_COUNT columnFamilyExpr ) ;
    public final CliParser.countStatement_return countStatement() throws RecognitionException {
        CliParser.countStatement_return retval = new CliParser.countStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token COUNT141=null;
        CliParser.columnFamilyExpr_return columnFamilyExpr142 = null;


        CommonTree COUNT141_tree=null;
        RewriteRuleTokenStream stream_COUNT=new RewriteRuleTokenStream(adaptor,"token COUNT");
        RewriteRuleSubtreeStream stream_columnFamilyExpr=new RewriteRuleSubtreeStream(adaptor,"rule columnFamilyExpr");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:274:5: ( COUNT columnFamilyExpr -> ^( NODE_THRIFT_COUNT columnFamilyExpr ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:274:7: COUNT columnFamilyExpr
            {
            COUNT141=(Token)match(input,COUNT,FOLLOW_COUNT_in_countStatement1976); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_COUNT.add(COUNT141);

            pushFollow(FOLLOW_columnFamilyExpr_in_countStatement1978);
            columnFamilyExpr142=columnFamilyExpr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_columnFamilyExpr.add(columnFamilyExpr142.getTree());


            // AST REWRITE
            // elements: columnFamilyExpr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 275:9: -> ^( NODE_THRIFT_COUNT columnFamilyExpr )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:275:12: ^( NODE_THRIFT_COUNT columnFamilyExpr )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_THRIFT_COUNT, "NODE_THRIFT_COUNT"), root_1);

                adaptor.addChild(root_1, stream_columnFamilyExpr.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "countStatement"

    public static class delStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "delStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:278:1: delStatement : DEL columnFamilyExpr -> ^( NODE_THRIFT_DEL columnFamilyExpr ) ;
    public final CliParser.delStatement_return delStatement() throws RecognitionException {
        CliParser.delStatement_return retval = new CliParser.delStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DEL143=null;
        CliParser.columnFamilyExpr_return columnFamilyExpr144 = null;


        CommonTree DEL143_tree=null;
        RewriteRuleTokenStream stream_DEL=new RewriteRuleTokenStream(adaptor,"token DEL");
        RewriteRuleSubtreeStream stream_columnFamilyExpr=new RewriteRuleSubtreeStream(adaptor,"rule columnFamilyExpr");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:279:5: ( DEL columnFamilyExpr -> ^( NODE_THRIFT_DEL columnFamilyExpr ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:279:7: DEL columnFamilyExpr
            {
            DEL143=(Token)match(input,DEL,FOLLOW_DEL_in_delStatement2012); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_DEL.add(DEL143);

            pushFollow(FOLLOW_columnFamilyExpr_in_delStatement2014);
            columnFamilyExpr144=columnFamilyExpr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_columnFamilyExpr.add(columnFamilyExpr144.getTree());


            // AST REWRITE
            // elements: columnFamilyExpr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 280:9: -> ^( NODE_THRIFT_DEL columnFamilyExpr )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:280:12: ^( NODE_THRIFT_DEL columnFamilyExpr )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_THRIFT_DEL, "NODE_THRIFT_DEL"), root_1);

                adaptor.addChild(root_1, stream_columnFamilyExpr.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "delStatement"

    public static class showStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "showStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:283:1: showStatement : ( showClusterName | showVersion | showKeyspaces );
    public final CliParser.showStatement_return showStatement() throws RecognitionException {
        CliParser.showStatement_return retval = new CliParser.showStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CliParser.showClusterName_return showClusterName145 = null;

        CliParser.showVersion_return showVersion146 = null;

        CliParser.showKeyspaces_return showKeyspaces147 = null;



        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:284:5: ( showClusterName | showVersion | showKeyspaces )
            int alt17=3;
            int LA17_0 = input.LA(1);

            if ( (LA17_0==SHOW) ) {
                switch ( input.LA(2) ) {
                case 103:
                    {
                    alt17=1;
                    }
                    break;
                case API_VERSION:
                    {
                    alt17=2;
                    }
                    break;
                case KEYSPACES:
                    {
                    alt17=3;
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 17, 1, input);

                    throw nvae;
                }

            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 17, 0, input);

                throw nvae;
            }
            switch (alt17) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:284:7: showClusterName
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_showClusterName_in_showStatement2048);
                    showClusterName145=showClusterName();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, showClusterName145.getTree());

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:285:7: showVersion
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_showVersion_in_showStatement2056);
                    showVersion146=showVersion();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, showVersion146.getTree());

                    }
                    break;
                case 3 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:286:7: showKeyspaces
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_showKeyspaces_in_showStatement2064);
                    showKeyspaces147=showKeyspaces();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, showKeyspaces147.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "showStatement"

    public static class listStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "listStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:289:1: listStatement : LIST columnFamily ( keyRangeExpr )? ( 'LIMIT' limit= IntegerPositiveLiteral )? -> ^( NODE_LIST columnFamily ( keyRangeExpr )? ( ^( NODE_LIMIT $limit) )? ) ;
    public final CliParser.listStatement_return listStatement() throws RecognitionException {
        CliParser.listStatement_return retval = new CliParser.listStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token limit=null;
        Token LIST148=null;
        Token string_literal151=null;
        CliParser.columnFamily_return columnFamily149 = null;

        CliParser.keyRangeExpr_return keyRangeExpr150 = null;


        CommonTree limit_tree=null;
        CommonTree LIST148_tree=null;
        CommonTree string_literal151_tree=null;
        RewriteRuleTokenStream stream_IntegerPositiveLiteral=new RewriteRuleTokenStream(adaptor,"token IntegerPositiveLiteral");
        RewriteRuleTokenStream stream_LIST=new RewriteRuleTokenStream(adaptor,"token LIST");
        RewriteRuleTokenStream stream_LIMIT=new RewriteRuleTokenStream(adaptor,"token LIMIT");
        RewriteRuleSubtreeStream stream_columnFamily=new RewriteRuleSubtreeStream(adaptor,"rule columnFamily");
        RewriteRuleSubtreeStream stream_keyRangeExpr=new RewriteRuleSubtreeStream(adaptor,"rule keyRangeExpr");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:290:5: ( LIST columnFamily ( keyRangeExpr )? ( 'LIMIT' limit= IntegerPositiveLiteral )? -> ^( NODE_LIST columnFamily ( keyRangeExpr )? ( ^( NODE_LIMIT $limit) )? ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:290:7: LIST columnFamily ( keyRangeExpr )? ( 'LIMIT' limit= IntegerPositiveLiteral )?
            {
            LIST148=(Token)match(input,LIST,FOLLOW_LIST_in_listStatement2081); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_LIST.add(LIST148);

            pushFollow(FOLLOW_columnFamily_in_listStatement2083);
            columnFamily149=columnFamily();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_columnFamily.add(columnFamily149.getTree());
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:290:25: ( keyRangeExpr )?
            int alt18=2;
            int LA18_0 = input.LA(1);

            if ( (LA18_0==113) ) {
                alt18=1;
            }
            switch (alt18) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:0:0: keyRangeExpr
                    {
                    pushFollow(FOLLOW_keyRangeExpr_in_listStatement2085);
                    keyRangeExpr150=keyRangeExpr();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_keyRangeExpr.add(keyRangeExpr150.getTree());

                    }
                    break;

            }

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:290:39: ( 'LIMIT' limit= IntegerPositiveLiteral )?
            int alt19=2;
            int LA19_0 = input.LA(1);

            if ( (LA19_0==LIMIT) ) {
                alt19=1;
            }
            switch (alt19) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:290:40: 'LIMIT' limit= IntegerPositiveLiteral
                    {
                    string_literal151=(Token)match(input,LIMIT,FOLLOW_LIMIT_in_listStatement2089); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_LIMIT.add(string_literal151);

                    limit=(Token)match(input,IntegerPositiveLiteral,FOLLOW_IntegerPositiveLiteral_in_listStatement2093); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_IntegerPositiveLiteral.add(limit);


                    }
                    break;

            }



            // AST REWRITE
            // elements: limit, columnFamily, keyRangeExpr
            // token labels: limit
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleTokenStream stream_limit=new RewriteRuleTokenStream(adaptor,"token limit",limit);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 291:9: -> ^( NODE_LIST columnFamily ( keyRangeExpr )? ( ^( NODE_LIMIT $limit) )? )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:291:12: ^( NODE_LIST columnFamily ( keyRangeExpr )? ( ^( NODE_LIMIT $limit) )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_LIST, "NODE_LIST"), root_1);

                adaptor.addChild(root_1, stream_columnFamily.nextTree());
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:291:37: ( keyRangeExpr )?
                if ( stream_keyRangeExpr.hasNext() ) {
                    adaptor.addChild(root_1, stream_keyRangeExpr.nextTree());

                }
                stream_keyRangeExpr.reset();
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:291:51: ( ^( NODE_LIMIT $limit) )?
                if ( stream_limit.hasNext() ) {
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:291:51: ^( NODE_LIMIT $limit)
                    {
                    CommonTree root_2 = (CommonTree)adaptor.nil();
                    root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_LIMIT, "NODE_LIMIT"), root_2);

                    adaptor.addChild(root_2, stream_limit.nextNode());

                    adaptor.addChild(root_1, root_2);
                    }

                }
                stream_limit.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "listStatement"

    public static class truncateStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "truncateStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:294:1: truncateStatement : TRUNCATE columnFamily -> ^( NODE_TRUNCATE columnFamily ) ;
    public final CliParser.truncateStatement_return truncateStatement() throws RecognitionException {
        CliParser.truncateStatement_return retval = new CliParser.truncateStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token TRUNCATE152=null;
        CliParser.columnFamily_return columnFamily153 = null;


        CommonTree TRUNCATE152_tree=null;
        RewriteRuleTokenStream stream_TRUNCATE=new RewriteRuleTokenStream(adaptor,"token TRUNCATE");
        RewriteRuleSubtreeStream stream_columnFamily=new RewriteRuleSubtreeStream(adaptor,"rule columnFamily");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:295:5: ( TRUNCATE columnFamily -> ^( NODE_TRUNCATE columnFamily ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:295:7: TRUNCATE columnFamily
            {
            TRUNCATE152=(Token)match(input,TRUNCATE,FOLLOW_TRUNCATE_in_truncateStatement2139); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_TRUNCATE.add(TRUNCATE152);

            pushFollow(FOLLOW_columnFamily_in_truncateStatement2141);
            columnFamily153=columnFamily();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_columnFamily.add(columnFamily153.getTree());


            // AST REWRITE
            // elements: columnFamily
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 296:9: -> ^( NODE_TRUNCATE columnFamily )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:296:12: ^( NODE_TRUNCATE columnFamily )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_TRUNCATE, "NODE_TRUNCATE"), root_1);

                adaptor.addChild(root_1, stream_columnFamily.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "truncateStatement"

    public static class assumeStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "assumeStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:299:1: assumeStatement : ASSUME columnFamily assumptionElement= Identifier 'AS' defaultType= Identifier -> ^( NODE_ASSUME columnFamily $assumptionElement $defaultType) ;
    public final CliParser.assumeStatement_return assumeStatement() throws RecognitionException {
        CliParser.assumeStatement_return retval = new CliParser.assumeStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token assumptionElement=null;
        Token defaultType=null;
        Token ASSUME154=null;
        Token string_literal156=null;
        CliParser.columnFamily_return columnFamily155 = null;


        CommonTree assumptionElement_tree=null;
        CommonTree defaultType_tree=null;
        CommonTree ASSUME154_tree=null;
        CommonTree string_literal156_tree=null;
        RewriteRuleTokenStream stream_105=new RewriteRuleTokenStream(adaptor,"token 105");
        RewriteRuleTokenStream stream_ASSUME=new RewriteRuleTokenStream(adaptor,"token ASSUME");
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleSubtreeStream stream_columnFamily=new RewriteRuleSubtreeStream(adaptor,"rule columnFamily");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:300:5: ( ASSUME columnFamily assumptionElement= Identifier 'AS' defaultType= Identifier -> ^( NODE_ASSUME columnFamily $assumptionElement $defaultType) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:300:7: ASSUME columnFamily assumptionElement= Identifier 'AS' defaultType= Identifier
            {
            ASSUME154=(Token)match(input,ASSUME,FOLLOW_ASSUME_in_assumeStatement2174); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ASSUME.add(ASSUME154);

            pushFollow(FOLLOW_columnFamily_in_assumeStatement2176);
            columnFamily155=columnFamily();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_columnFamily.add(columnFamily155.getTree());
            assumptionElement=(Token)match(input,Identifier,FOLLOW_Identifier_in_assumeStatement2180); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Identifier.add(assumptionElement);

            string_literal156=(Token)match(input,105,FOLLOW_105_in_assumeStatement2182); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_105.add(string_literal156);

            defaultType=(Token)match(input,Identifier,FOLLOW_Identifier_in_assumeStatement2186); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Identifier.add(defaultType);



            // AST REWRITE
            // elements: columnFamily, defaultType, assumptionElement
            // token labels: defaultType, assumptionElement
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleTokenStream stream_defaultType=new RewriteRuleTokenStream(adaptor,"token defaultType",defaultType);
            RewriteRuleTokenStream stream_assumptionElement=new RewriteRuleTokenStream(adaptor,"token assumptionElement",assumptionElement);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 301:9: -> ^( NODE_ASSUME columnFamily $assumptionElement $defaultType)
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:301:12: ^( NODE_ASSUME columnFamily $assumptionElement $defaultType)
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_ASSUME, "NODE_ASSUME"), root_1);

                adaptor.addChild(root_1, stream_columnFamily.nextTree());
                adaptor.addChild(root_1, stream_assumptionElement.nextNode());
                adaptor.addChild(root_1, stream_defaultType.nextNode());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "assumeStatement"

    public static class consistencyLevelStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "consistencyLevelStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:304:1: consistencyLevelStatement : CONSISTENCYLEVEL 'AS' defaultType= Identifier -> ^( NODE_CONSISTENCY_LEVEL $defaultType) ;
    public final CliParser.consistencyLevelStatement_return consistencyLevelStatement() throws RecognitionException {
        CliParser.consistencyLevelStatement_return retval = new CliParser.consistencyLevelStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token defaultType=null;
        Token CONSISTENCYLEVEL157=null;
        Token string_literal158=null;

        CommonTree defaultType_tree=null;
        CommonTree CONSISTENCYLEVEL157_tree=null;
        CommonTree string_literal158_tree=null;
        RewriteRuleTokenStream stream_105=new RewriteRuleTokenStream(adaptor,"token 105");
        RewriteRuleTokenStream stream_CONSISTENCYLEVEL=new RewriteRuleTokenStream(adaptor,"token CONSISTENCYLEVEL");
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:305:5: ( CONSISTENCYLEVEL 'AS' defaultType= Identifier -> ^( NODE_CONSISTENCY_LEVEL $defaultType) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:305:7: CONSISTENCYLEVEL 'AS' defaultType= Identifier
            {
            CONSISTENCYLEVEL157=(Token)match(input,CONSISTENCYLEVEL,FOLLOW_CONSISTENCYLEVEL_in_consistencyLevelStatement2225); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_CONSISTENCYLEVEL.add(CONSISTENCYLEVEL157);

            string_literal158=(Token)match(input,105,FOLLOW_105_in_consistencyLevelStatement2227); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_105.add(string_literal158);

            defaultType=(Token)match(input,Identifier,FOLLOW_Identifier_in_consistencyLevelStatement2231); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Identifier.add(defaultType);



            // AST REWRITE
            // elements: defaultType
            // token labels: defaultType
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleTokenStream stream_defaultType=new RewriteRuleTokenStream(adaptor,"token defaultType",defaultType);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 306:9: -> ^( NODE_CONSISTENCY_LEVEL $defaultType)
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:306:12: ^( NODE_CONSISTENCY_LEVEL $defaultType)
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_CONSISTENCY_LEVEL, "NODE_CONSISTENCY_LEVEL"), root_1);

                adaptor.addChild(root_1, stream_defaultType.nextNode());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "consistencyLevelStatement"

    public static class showClusterName_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "showClusterName"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:309:1: showClusterName : SHOW 'CLUSTER NAME' -> ^( NODE_SHOW_CLUSTER_NAME ) ;
    public final CliParser.showClusterName_return showClusterName() throws RecognitionException {
        CliParser.showClusterName_return retval = new CliParser.showClusterName_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token SHOW159=null;
        Token string_literal160=null;

        CommonTree SHOW159_tree=null;
        CommonTree string_literal160_tree=null;
        RewriteRuleTokenStream stream_SHOW=new RewriteRuleTokenStream(adaptor,"token SHOW");
        RewriteRuleTokenStream stream_103=new RewriteRuleTokenStream(adaptor,"token 103");

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:310:5: ( SHOW 'CLUSTER NAME' -> ^( NODE_SHOW_CLUSTER_NAME ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:310:7: SHOW 'CLUSTER NAME'
            {
            SHOW159=(Token)match(input,SHOW,FOLLOW_SHOW_in_showClusterName2265); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_SHOW.add(SHOW159);

            string_literal160=(Token)match(input,103,FOLLOW_103_in_showClusterName2267); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_103.add(string_literal160);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 311:9: -> ^( NODE_SHOW_CLUSTER_NAME )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:311:12: ^( NODE_SHOW_CLUSTER_NAME )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_SHOW_CLUSTER_NAME, "NODE_SHOW_CLUSTER_NAME"), root_1);

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "showClusterName"

    public static class addKeyspace_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "addKeyspace"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:314:1: addKeyspace : CREATE KEYSPACE keyValuePairExpr -> ^( NODE_ADD_KEYSPACE keyValuePairExpr ) ;
    public final CliParser.addKeyspace_return addKeyspace() throws RecognitionException {
        CliParser.addKeyspace_return retval = new CliParser.addKeyspace_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token CREATE161=null;
        Token KEYSPACE162=null;
        CliParser.keyValuePairExpr_return keyValuePairExpr163 = null;


        CommonTree CREATE161_tree=null;
        CommonTree KEYSPACE162_tree=null;
        RewriteRuleTokenStream stream_CREATE=new RewriteRuleTokenStream(adaptor,"token CREATE");
        RewriteRuleTokenStream stream_KEYSPACE=new RewriteRuleTokenStream(adaptor,"token KEYSPACE");
        RewriteRuleSubtreeStream stream_keyValuePairExpr=new RewriteRuleSubtreeStream(adaptor,"rule keyValuePairExpr");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:315:5: ( CREATE KEYSPACE keyValuePairExpr -> ^( NODE_ADD_KEYSPACE keyValuePairExpr ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:315:7: CREATE KEYSPACE keyValuePairExpr
            {
            CREATE161=(Token)match(input,CREATE,FOLLOW_CREATE_in_addKeyspace2298); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_CREATE.add(CREATE161);

            KEYSPACE162=(Token)match(input,KEYSPACE,FOLLOW_KEYSPACE_in_addKeyspace2300); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KEYSPACE.add(KEYSPACE162);

            pushFollow(FOLLOW_keyValuePairExpr_in_addKeyspace2302);
            keyValuePairExpr163=keyValuePairExpr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_keyValuePairExpr.add(keyValuePairExpr163.getTree());


            // AST REWRITE
            // elements: keyValuePairExpr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 316:9: -> ^( NODE_ADD_KEYSPACE keyValuePairExpr )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:316:12: ^( NODE_ADD_KEYSPACE keyValuePairExpr )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_ADD_KEYSPACE, "NODE_ADD_KEYSPACE"), root_1);

                adaptor.addChild(root_1, stream_keyValuePairExpr.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "addKeyspace"

    public static class addColumnFamily_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "addColumnFamily"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:319:1: addColumnFamily : CREATE COLUMN FAMILY keyValuePairExpr -> ^( NODE_ADD_COLUMN_FAMILY keyValuePairExpr ) ;
    public final CliParser.addColumnFamily_return addColumnFamily() throws RecognitionException {
        CliParser.addColumnFamily_return retval = new CliParser.addColumnFamily_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token CREATE164=null;
        Token COLUMN165=null;
        Token FAMILY166=null;
        CliParser.keyValuePairExpr_return keyValuePairExpr167 = null;


        CommonTree CREATE164_tree=null;
        CommonTree COLUMN165_tree=null;
        CommonTree FAMILY166_tree=null;
        RewriteRuleTokenStream stream_FAMILY=new RewriteRuleTokenStream(adaptor,"token FAMILY");
        RewriteRuleTokenStream stream_CREATE=new RewriteRuleTokenStream(adaptor,"token CREATE");
        RewriteRuleTokenStream stream_COLUMN=new RewriteRuleTokenStream(adaptor,"token COLUMN");
        RewriteRuleSubtreeStream stream_keyValuePairExpr=new RewriteRuleSubtreeStream(adaptor,"rule keyValuePairExpr");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:320:5: ( CREATE COLUMN FAMILY keyValuePairExpr -> ^( NODE_ADD_COLUMN_FAMILY keyValuePairExpr ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:320:7: CREATE COLUMN FAMILY keyValuePairExpr
            {
            CREATE164=(Token)match(input,CREATE,FOLLOW_CREATE_in_addColumnFamily2336); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_CREATE.add(CREATE164);

            COLUMN165=(Token)match(input,COLUMN,FOLLOW_COLUMN_in_addColumnFamily2338); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_COLUMN.add(COLUMN165);

            FAMILY166=(Token)match(input,FAMILY,FOLLOW_FAMILY_in_addColumnFamily2340); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_FAMILY.add(FAMILY166);

            pushFollow(FOLLOW_keyValuePairExpr_in_addColumnFamily2342);
            keyValuePairExpr167=keyValuePairExpr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_keyValuePairExpr.add(keyValuePairExpr167.getTree());


            // AST REWRITE
            // elements: keyValuePairExpr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 321:9: -> ^( NODE_ADD_COLUMN_FAMILY keyValuePairExpr )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:321:12: ^( NODE_ADD_COLUMN_FAMILY keyValuePairExpr )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_ADD_COLUMN_FAMILY, "NODE_ADD_COLUMN_FAMILY"), root_1);

                adaptor.addChild(root_1, stream_keyValuePairExpr.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "addColumnFamily"

    public static class updateKeyspace_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "updateKeyspace"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:324:1: updateKeyspace : UPDATE KEYSPACE keyValuePairExpr -> ^( NODE_UPDATE_KEYSPACE keyValuePairExpr ) ;
    public final CliParser.updateKeyspace_return updateKeyspace() throws RecognitionException {
        CliParser.updateKeyspace_return retval = new CliParser.updateKeyspace_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token UPDATE168=null;
        Token KEYSPACE169=null;
        CliParser.keyValuePairExpr_return keyValuePairExpr170 = null;


        CommonTree UPDATE168_tree=null;
        CommonTree KEYSPACE169_tree=null;
        RewriteRuleTokenStream stream_UPDATE=new RewriteRuleTokenStream(adaptor,"token UPDATE");
        RewriteRuleTokenStream stream_KEYSPACE=new RewriteRuleTokenStream(adaptor,"token KEYSPACE");
        RewriteRuleSubtreeStream stream_keyValuePairExpr=new RewriteRuleSubtreeStream(adaptor,"rule keyValuePairExpr");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:325:5: ( UPDATE KEYSPACE keyValuePairExpr -> ^( NODE_UPDATE_KEYSPACE keyValuePairExpr ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:325:7: UPDATE KEYSPACE keyValuePairExpr
            {
            UPDATE168=(Token)match(input,UPDATE,FOLLOW_UPDATE_in_updateKeyspace2376); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_UPDATE.add(UPDATE168);

            KEYSPACE169=(Token)match(input,KEYSPACE,FOLLOW_KEYSPACE_in_updateKeyspace2378); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KEYSPACE.add(KEYSPACE169);

            pushFollow(FOLLOW_keyValuePairExpr_in_updateKeyspace2380);
            keyValuePairExpr170=keyValuePairExpr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_keyValuePairExpr.add(keyValuePairExpr170.getTree());


            // AST REWRITE
            // elements: keyValuePairExpr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 326:9: -> ^( NODE_UPDATE_KEYSPACE keyValuePairExpr )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:326:12: ^( NODE_UPDATE_KEYSPACE keyValuePairExpr )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_UPDATE_KEYSPACE, "NODE_UPDATE_KEYSPACE"), root_1);

                adaptor.addChild(root_1, stream_keyValuePairExpr.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "updateKeyspace"

    public static class updateColumnFamily_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "updateColumnFamily"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:329:1: updateColumnFamily : UPDATE COLUMN FAMILY keyValuePairExpr -> ^( NODE_UPDATE_COLUMN_FAMILY keyValuePairExpr ) ;
    public final CliParser.updateColumnFamily_return updateColumnFamily() throws RecognitionException {
        CliParser.updateColumnFamily_return retval = new CliParser.updateColumnFamily_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token UPDATE171=null;
        Token COLUMN172=null;
        Token FAMILY173=null;
        CliParser.keyValuePairExpr_return keyValuePairExpr174 = null;


        CommonTree UPDATE171_tree=null;
        CommonTree COLUMN172_tree=null;
        CommonTree FAMILY173_tree=null;
        RewriteRuleTokenStream stream_FAMILY=new RewriteRuleTokenStream(adaptor,"token FAMILY");
        RewriteRuleTokenStream stream_UPDATE=new RewriteRuleTokenStream(adaptor,"token UPDATE");
        RewriteRuleTokenStream stream_COLUMN=new RewriteRuleTokenStream(adaptor,"token COLUMN");
        RewriteRuleSubtreeStream stream_keyValuePairExpr=new RewriteRuleSubtreeStream(adaptor,"rule keyValuePairExpr");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:330:5: ( UPDATE COLUMN FAMILY keyValuePairExpr -> ^( NODE_UPDATE_COLUMN_FAMILY keyValuePairExpr ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:330:7: UPDATE COLUMN FAMILY keyValuePairExpr
            {
            UPDATE171=(Token)match(input,UPDATE,FOLLOW_UPDATE_in_updateColumnFamily2413); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_UPDATE.add(UPDATE171);

            COLUMN172=(Token)match(input,COLUMN,FOLLOW_COLUMN_in_updateColumnFamily2415); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_COLUMN.add(COLUMN172);

            FAMILY173=(Token)match(input,FAMILY,FOLLOW_FAMILY_in_updateColumnFamily2417); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_FAMILY.add(FAMILY173);

            pushFollow(FOLLOW_keyValuePairExpr_in_updateColumnFamily2419);
            keyValuePairExpr174=keyValuePairExpr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_keyValuePairExpr.add(keyValuePairExpr174.getTree());


            // AST REWRITE
            // elements: keyValuePairExpr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 331:9: -> ^( NODE_UPDATE_COLUMN_FAMILY keyValuePairExpr )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:331:12: ^( NODE_UPDATE_COLUMN_FAMILY keyValuePairExpr )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_UPDATE_COLUMN_FAMILY, "NODE_UPDATE_COLUMN_FAMILY"), root_1);

                adaptor.addChild(root_1, stream_keyValuePairExpr.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "updateColumnFamily"

    public static class delKeyspace_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "delKeyspace"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:334:1: delKeyspace : DROP KEYSPACE keyspace -> ^( NODE_DEL_KEYSPACE keyspace ) ;
    public final CliParser.delKeyspace_return delKeyspace() throws RecognitionException {
        CliParser.delKeyspace_return retval = new CliParser.delKeyspace_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DROP175=null;
        Token KEYSPACE176=null;
        CliParser.keyspace_return keyspace177 = null;


        CommonTree DROP175_tree=null;
        CommonTree KEYSPACE176_tree=null;
        RewriteRuleTokenStream stream_DROP=new RewriteRuleTokenStream(adaptor,"token DROP");
        RewriteRuleTokenStream stream_KEYSPACE=new RewriteRuleTokenStream(adaptor,"token KEYSPACE");
        RewriteRuleSubtreeStream stream_keyspace=new RewriteRuleSubtreeStream(adaptor,"rule keyspace");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:335:5: ( DROP KEYSPACE keyspace -> ^( NODE_DEL_KEYSPACE keyspace ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:335:7: DROP KEYSPACE keyspace
            {
            DROP175=(Token)match(input,DROP,FOLLOW_DROP_in_delKeyspace2452); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_DROP.add(DROP175);

            KEYSPACE176=(Token)match(input,KEYSPACE,FOLLOW_KEYSPACE_in_delKeyspace2454); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KEYSPACE.add(KEYSPACE176);

            pushFollow(FOLLOW_keyspace_in_delKeyspace2456);
            keyspace177=keyspace();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_keyspace.add(keyspace177.getTree());


            // AST REWRITE
            // elements: keyspace
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 336:9: -> ^( NODE_DEL_KEYSPACE keyspace )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:336:12: ^( NODE_DEL_KEYSPACE keyspace )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_DEL_KEYSPACE, "NODE_DEL_KEYSPACE"), root_1);

                adaptor.addChild(root_1, stream_keyspace.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "delKeyspace"

    public static class delColumnFamily_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "delColumnFamily"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:339:1: delColumnFamily : DROP COLUMN FAMILY columnFamily -> ^( NODE_DEL_COLUMN_FAMILY columnFamily ) ;
    public final CliParser.delColumnFamily_return delColumnFamily() throws RecognitionException {
        CliParser.delColumnFamily_return retval = new CliParser.delColumnFamily_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DROP178=null;
        Token COLUMN179=null;
        Token FAMILY180=null;
        CliParser.columnFamily_return columnFamily181 = null;


        CommonTree DROP178_tree=null;
        CommonTree COLUMN179_tree=null;
        CommonTree FAMILY180_tree=null;
        RewriteRuleTokenStream stream_FAMILY=new RewriteRuleTokenStream(adaptor,"token FAMILY");
        RewriteRuleTokenStream stream_DROP=new RewriteRuleTokenStream(adaptor,"token DROP");
        RewriteRuleTokenStream stream_COLUMN=new RewriteRuleTokenStream(adaptor,"token COLUMN");
        RewriteRuleSubtreeStream stream_columnFamily=new RewriteRuleSubtreeStream(adaptor,"rule columnFamily");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:340:5: ( DROP COLUMN FAMILY columnFamily -> ^( NODE_DEL_COLUMN_FAMILY columnFamily ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:340:7: DROP COLUMN FAMILY columnFamily
            {
            DROP178=(Token)match(input,DROP,FOLLOW_DROP_in_delColumnFamily2490); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_DROP.add(DROP178);

            COLUMN179=(Token)match(input,COLUMN,FOLLOW_COLUMN_in_delColumnFamily2492); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_COLUMN.add(COLUMN179);

            FAMILY180=(Token)match(input,FAMILY,FOLLOW_FAMILY_in_delColumnFamily2494); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_FAMILY.add(FAMILY180);

            pushFollow(FOLLOW_columnFamily_in_delColumnFamily2496);
            columnFamily181=columnFamily();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_columnFamily.add(columnFamily181.getTree());


            // AST REWRITE
            // elements: columnFamily
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 341:9: -> ^( NODE_DEL_COLUMN_FAMILY columnFamily )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:341:12: ^( NODE_DEL_COLUMN_FAMILY columnFamily )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_DEL_COLUMN_FAMILY, "NODE_DEL_COLUMN_FAMILY"), root_1);

                adaptor.addChild(root_1, stream_columnFamily.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "delColumnFamily"

    public static class dropIndex_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "dropIndex"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:344:1: dropIndex : DROP INDEX ON columnFamily '.' columnName -> ^( NODE_DROP_INDEX columnFamily columnName ) ;
    public final CliParser.dropIndex_return dropIndex() throws RecognitionException {
        CliParser.dropIndex_return retval = new CliParser.dropIndex_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DROP182=null;
        Token INDEX183=null;
        Token ON184=null;
        Token char_literal186=null;
        CliParser.columnFamily_return columnFamily185 = null;

        CliParser.columnName_return columnName187 = null;


        CommonTree DROP182_tree=null;
        CommonTree INDEX183_tree=null;
        CommonTree ON184_tree=null;
        CommonTree char_literal186_tree=null;
        RewriteRuleTokenStream stream_INDEX=new RewriteRuleTokenStream(adaptor,"token INDEX");
        RewriteRuleTokenStream stream_ON=new RewriteRuleTokenStream(adaptor,"token ON");
        RewriteRuleTokenStream stream_112=new RewriteRuleTokenStream(adaptor,"token 112");
        RewriteRuleTokenStream stream_DROP=new RewriteRuleTokenStream(adaptor,"token DROP");
        RewriteRuleSubtreeStream stream_columnFamily=new RewriteRuleSubtreeStream(adaptor,"rule columnFamily");
        RewriteRuleSubtreeStream stream_columnName=new RewriteRuleSubtreeStream(adaptor,"rule columnName");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:345:5: ( DROP INDEX ON columnFamily '.' columnName -> ^( NODE_DROP_INDEX columnFamily columnName ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:345:7: DROP INDEX ON columnFamily '.' columnName
            {
            DROP182=(Token)match(input,DROP,FOLLOW_DROP_in_dropIndex2530); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_DROP.add(DROP182);

            INDEX183=(Token)match(input,INDEX,FOLLOW_INDEX_in_dropIndex2532); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_INDEX.add(INDEX183);

            ON184=(Token)match(input,ON,FOLLOW_ON_in_dropIndex2534); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ON.add(ON184);

            pushFollow(FOLLOW_columnFamily_in_dropIndex2536);
            columnFamily185=columnFamily();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_columnFamily.add(columnFamily185.getTree());
            char_literal186=(Token)match(input,112,FOLLOW_112_in_dropIndex2538); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_112.add(char_literal186);

            pushFollow(FOLLOW_columnName_in_dropIndex2540);
            columnName187=columnName();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_columnName.add(columnName187.getTree());


            // AST REWRITE
            // elements: columnFamily, columnName
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 346:9: -> ^( NODE_DROP_INDEX columnFamily columnName )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:346:12: ^( NODE_DROP_INDEX columnFamily columnName )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_DROP_INDEX, "NODE_DROP_INDEX"), root_1);

                adaptor.addChild(root_1, stream_columnFamily.nextTree());
                adaptor.addChild(root_1, stream_columnName.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "dropIndex"

    public static class showVersion_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "showVersion"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:349:1: showVersion : SHOW API_VERSION -> ^( NODE_SHOW_VERSION ) ;
    public final CliParser.showVersion_return showVersion() throws RecognitionException {
        CliParser.showVersion_return retval = new CliParser.showVersion_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token SHOW188=null;
        Token API_VERSION189=null;

        CommonTree SHOW188_tree=null;
        CommonTree API_VERSION189_tree=null;
        RewriteRuleTokenStream stream_API_VERSION=new RewriteRuleTokenStream(adaptor,"token API_VERSION");
        RewriteRuleTokenStream stream_SHOW=new RewriteRuleTokenStream(adaptor,"token SHOW");

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:350:5: ( SHOW API_VERSION -> ^( NODE_SHOW_VERSION ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:350:7: SHOW API_VERSION
            {
            SHOW188=(Token)match(input,SHOW,FOLLOW_SHOW_in_showVersion2575); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_SHOW.add(SHOW188);

            API_VERSION189=(Token)match(input,API_VERSION,FOLLOW_API_VERSION_in_showVersion2577); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_API_VERSION.add(API_VERSION189);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 351:9: -> ^( NODE_SHOW_VERSION )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:351:12: ^( NODE_SHOW_VERSION )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_SHOW_VERSION, "NODE_SHOW_VERSION"), root_1);

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "showVersion"

    public static class showKeyspaces_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "showKeyspaces"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:354:1: showKeyspaces : SHOW KEYSPACES -> ^( NODE_SHOW_KEYSPACES ) ;
    public final CliParser.showKeyspaces_return showKeyspaces() throws RecognitionException {
        CliParser.showKeyspaces_return retval = new CliParser.showKeyspaces_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token SHOW190=null;
        Token KEYSPACES191=null;

        CommonTree SHOW190_tree=null;
        CommonTree KEYSPACES191_tree=null;
        RewriteRuleTokenStream stream_KEYSPACES=new RewriteRuleTokenStream(adaptor,"token KEYSPACES");
        RewriteRuleTokenStream stream_SHOW=new RewriteRuleTokenStream(adaptor,"token SHOW");

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:355:5: ( SHOW KEYSPACES -> ^( NODE_SHOW_KEYSPACES ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:355:7: SHOW KEYSPACES
            {
            SHOW190=(Token)match(input,SHOW,FOLLOW_SHOW_in_showKeyspaces2608); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_SHOW.add(SHOW190);

            KEYSPACES191=(Token)match(input,KEYSPACES,FOLLOW_KEYSPACES_in_showKeyspaces2610); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KEYSPACES.add(KEYSPACES191);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 356:9: -> ^( NODE_SHOW_KEYSPACES )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:356:12: ^( NODE_SHOW_KEYSPACES )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_SHOW_KEYSPACES, "NODE_SHOW_KEYSPACES"), root_1);

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "showKeyspaces"

    public static class describeTable_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "describeTable"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:359:1: describeTable : DESCRIBE KEYSPACE ( keyspace )? -> ^( NODE_DESCRIBE_TABLE ( keyspace )? ) ;
    public final CliParser.describeTable_return describeTable() throws RecognitionException {
        CliParser.describeTable_return retval = new CliParser.describeTable_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DESCRIBE192=null;
        Token KEYSPACE193=null;
        CliParser.keyspace_return keyspace194 = null;


        CommonTree DESCRIBE192_tree=null;
        CommonTree KEYSPACE193_tree=null;
        RewriteRuleTokenStream stream_DESCRIBE=new RewriteRuleTokenStream(adaptor,"token DESCRIBE");
        RewriteRuleTokenStream stream_KEYSPACE=new RewriteRuleTokenStream(adaptor,"token KEYSPACE");
        RewriteRuleSubtreeStream stream_keyspace=new RewriteRuleSubtreeStream(adaptor,"rule keyspace");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:360:5: ( DESCRIBE KEYSPACE ( keyspace )? -> ^( NODE_DESCRIBE_TABLE ( keyspace )? ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:360:7: DESCRIBE KEYSPACE ( keyspace )?
            {
            DESCRIBE192=(Token)match(input,DESCRIBE,FOLLOW_DESCRIBE_in_describeTable2642); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_DESCRIBE.add(DESCRIBE192);

            KEYSPACE193=(Token)match(input,KEYSPACE,FOLLOW_KEYSPACE_in_describeTable2644); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KEYSPACE.add(KEYSPACE193);

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:360:25: ( keyspace )?
            int alt20=2;
            int LA20_0 = input.LA(1);

            if ( (LA20_0==Identifier) ) {
                alt20=1;
            }
            switch (alt20) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:360:26: keyspace
                    {
                    pushFollow(FOLLOW_keyspace_in_describeTable2647);
                    keyspace194=keyspace();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_keyspace.add(keyspace194.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: keyspace
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 361:9: -> ^( NODE_DESCRIBE_TABLE ( keyspace )? )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:361:12: ^( NODE_DESCRIBE_TABLE ( keyspace )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_DESCRIBE_TABLE, "NODE_DESCRIBE_TABLE"), root_1);

                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:361:34: ( keyspace )?
                if ( stream_keyspace.hasNext() ) {
                    adaptor.addChild(root_1, stream_keyspace.nextTree());

                }
                stream_keyspace.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "describeTable"

    public static class describeCluster_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "describeCluster"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:364:1: describeCluster : DESCRIBE 'CLUSTER' -> ^( NODE_DESCRIBE_CLUSTER ) ;
    public final CliParser.describeCluster_return describeCluster() throws RecognitionException {
        CliParser.describeCluster_return retval = new CliParser.describeCluster_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DESCRIBE195=null;
        Token string_literal196=null;

        CommonTree DESCRIBE195_tree=null;
        CommonTree string_literal196_tree=null;
        RewriteRuleTokenStream stream_102=new RewriteRuleTokenStream(adaptor,"token 102");
        RewriteRuleTokenStream stream_DESCRIBE=new RewriteRuleTokenStream(adaptor,"token DESCRIBE");

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:365:5: ( DESCRIBE 'CLUSTER' -> ^( NODE_DESCRIBE_CLUSTER ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:365:7: DESCRIBE 'CLUSTER'
            {
            DESCRIBE195=(Token)match(input,DESCRIBE,FOLLOW_DESCRIBE_in_describeCluster2689); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_DESCRIBE.add(DESCRIBE195);

            string_literal196=(Token)match(input,102,FOLLOW_102_in_describeCluster2691); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_102.add(string_literal196);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 366:9: -> ^( NODE_DESCRIBE_CLUSTER )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:366:12: ^( NODE_DESCRIBE_CLUSTER )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_DESCRIBE_CLUSTER, "NODE_DESCRIBE_CLUSTER"), root_1);

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "describeCluster"

    public static class useKeyspace_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "useKeyspace"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:369:1: useKeyspace : USE keyspace ( username )? ( password )? -> ^( NODE_USE_TABLE keyspace ( username )? ( password )? ) ;
    public final CliParser.useKeyspace_return useKeyspace() throws RecognitionException {
        CliParser.useKeyspace_return retval = new CliParser.useKeyspace_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token USE197=null;
        CliParser.keyspace_return keyspace198 = null;

        CliParser.username_return username199 = null;

        CliParser.password_return password200 = null;


        CommonTree USE197_tree=null;
        RewriteRuleTokenStream stream_USE=new RewriteRuleTokenStream(adaptor,"token USE");
        RewriteRuleSubtreeStream stream_username=new RewriteRuleSubtreeStream(adaptor,"rule username");
        RewriteRuleSubtreeStream stream_keyspace=new RewriteRuleSubtreeStream(adaptor,"rule keyspace");
        RewriteRuleSubtreeStream stream_password=new RewriteRuleSubtreeStream(adaptor,"rule password");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:370:5: ( USE keyspace ( username )? ( password )? -> ^( NODE_USE_TABLE keyspace ( username )? ( password )? ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:370:7: USE keyspace ( username )? ( password )?
            {
            USE197=(Token)match(input,USE,FOLLOW_USE_in_useKeyspace2722); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_USE.add(USE197);

            pushFollow(FOLLOW_keyspace_in_useKeyspace2724);
            keyspace198=keyspace();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_keyspace.add(keyspace198.getTree());
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:370:20: ( username )?
            int alt21=2;
            int LA21_0 = input.LA(1);

            if ( (LA21_0==Identifier) ) {
                alt21=1;
            }
            switch (alt21) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:370:22: username
                    {
                    pushFollow(FOLLOW_username_in_useKeyspace2728);
                    username199=username();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_username.add(username199.getTree());

                    }
                    break;

            }

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:370:34: ( password )?
            int alt22=2;
            int LA22_0 = input.LA(1);

            if ( (LA22_0==StringLiteral) ) {
                alt22=1;
            }
            switch (alt22) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:370:36: password
                    {
                    pushFollow(FOLLOW_password_in_useKeyspace2735);
                    password200=password();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_password.add(password200.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: password, keyspace, username
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 371:9: -> ^( NODE_USE_TABLE keyspace ( username )? ( password )? )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:371:12: ^( NODE_USE_TABLE keyspace ( username )? ( password )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_USE_TABLE, "NODE_USE_TABLE"), root_1);

                adaptor.addChild(root_1, stream_keyspace.nextTree());
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:371:38: ( username )?
                if ( stream_username.hasNext() ) {
                    adaptor.addChild(root_1, stream_username.nextTree());

                }
                stream_username.reset();
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:371:52: ( password )?
                if ( stream_password.hasNext() ) {
                    adaptor.addChild(root_1, stream_password.nextTree());

                }
                stream_password.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "useKeyspace"

    public static class keyValuePairExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "keyValuePairExpr"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:375:1: keyValuePairExpr : objectName ( ( AND | WITH ) keyValuePair )* -> ^( NODE_NEW_KEYSPACE_ACCESS objectName ( keyValuePair )* ) ;
    public final CliParser.keyValuePairExpr_return keyValuePairExpr() throws RecognitionException {
        CliParser.keyValuePairExpr_return retval = new CliParser.keyValuePairExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token AND202=null;
        Token WITH203=null;
        CliParser.objectName_return objectName201 = null;

        CliParser.keyValuePair_return keyValuePair204 = null;


        CommonTree AND202_tree=null;
        CommonTree WITH203_tree=null;
        RewriteRuleTokenStream stream_AND=new RewriteRuleTokenStream(adaptor,"token AND");
        RewriteRuleTokenStream stream_WITH=new RewriteRuleTokenStream(adaptor,"token WITH");
        RewriteRuleSubtreeStream stream_objectName=new RewriteRuleSubtreeStream(adaptor,"rule objectName");
        RewriteRuleSubtreeStream stream_keyValuePair=new RewriteRuleSubtreeStream(adaptor,"rule keyValuePair");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:376:5: ( objectName ( ( AND | WITH ) keyValuePair )* -> ^( NODE_NEW_KEYSPACE_ACCESS objectName ( keyValuePair )* ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:376:7: objectName ( ( AND | WITH ) keyValuePair )*
            {
            pushFollow(FOLLOW_objectName_in_keyValuePairExpr2787);
            objectName201=objectName();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_objectName.add(objectName201.getTree());
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:376:18: ( ( AND | WITH ) keyValuePair )*
            loop24:
            do {
                int alt24=2;
                int LA24_0 = input.LA(1);

                if ( (LA24_0==WITH||LA24_0==AND) ) {
                    alt24=1;
                }


                switch (alt24) {
            	case 1 :
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:376:20: ( AND | WITH ) keyValuePair
            	    {
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:376:20: ( AND | WITH )
            	    int alt23=2;
            	    int LA23_0 = input.LA(1);

            	    if ( (LA23_0==AND) ) {
            	        alt23=1;
            	    }
            	    else if ( (LA23_0==WITH) ) {
            	        alt23=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 23, 0, input);

            	        throw nvae;
            	    }
            	    switch (alt23) {
            	        case 1 :
            	            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:376:21: AND
            	            {
            	            AND202=(Token)match(input,AND,FOLLOW_AND_in_keyValuePairExpr2792); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_AND.add(AND202);


            	            }
            	            break;
            	        case 2 :
            	            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:376:27: WITH
            	            {
            	            WITH203=(Token)match(input,WITH,FOLLOW_WITH_in_keyValuePairExpr2796); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_WITH.add(WITH203);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_keyValuePair_in_keyValuePairExpr2799);
            	    keyValuePair204=keyValuePair();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_keyValuePair.add(keyValuePair204.getTree());

            	    }
            	    break;

            	default :
            	    break loop24;
                }
            } while (true);



            // AST REWRITE
            // elements: objectName, keyValuePair
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 377:9: -> ^( NODE_NEW_KEYSPACE_ACCESS objectName ( keyValuePair )* )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:377:12: ^( NODE_NEW_KEYSPACE_ACCESS objectName ( keyValuePair )* )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_NEW_KEYSPACE_ACCESS, "NODE_NEW_KEYSPACE_ACCESS"), root_1);

                adaptor.addChild(root_1, stream_objectName.nextTree());
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:377:50: ( keyValuePair )*
                while ( stream_keyValuePair.hasNext() ) {
                    adaptor.addChild(root_1, stream_keyValuePair.nextTree());

                }
                stream_keyValuePair.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "keyValuePairExpr"

    public static class keyValuePair_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "keyValuePair"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:380:1: keyValuePair : attr_name '=' attrValue -> attr_name attrValue ;
    public final CliParser.keyValuePair_return keyValuePair() throws RecognitionException {
        CliParser.keyValuePair_return retval = new CliParser.keyValuePair_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal206=null;
        CliParser.attr_name_return attr_name205 = null;

        CliParser.attrValue_return attrValue207 = null;


        CommonTree char_literal206_tree=null;
        RewriteRuleTokenStream stream_107=new RewriteRuleTokenStream(adaptor,"token 107");
        RewriteRuleSubtreeStream stream_attr_name=new RewriteRuleSubtreeStream(adaptor,"rule attr_name");
        RewriteRuleSubtreeStream stream_attrValue=new RewriteRuleSubtreeStream(adaptor,"rule attrValue");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:381:5: ( attr_name '=' attrValue -> attr_name attrValue )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:381:7: attr_name '=' attrValue
            {
            pushFollow(FOLLOW_attr_name_in_keyValuePair2857);
            attr_name205=attr_name();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_attr_name.add(attr_name205.getTree());
            char_literal206=(Token)match(input,107,FOLLOW_107_in_keyValuePair2859); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_107.add(char_literal206);

            pushFollow(FOLLOW_attrValue_in_keyValuePair2861);
            attrValue207=attrValue();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_attrValue.add(attrValue207.getTree());


            // AST REWRITE
            // elements: attr_name, attrValue
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 382:6: -> attr_name attrValue
            {
                adaptor.addChild(root_0, stream_attr_name.nextTree());
                adaptor.addChild(root_0, stream_attrValue.nextTree());

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "keyValuePair"

    public static class attrValue_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "attrValue"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:385:1: attrValue : ( arrayConstruct | attrValueString | attrValueInt | attrValueDouble );
    public final CliParser.attrValue_return attrValue() throws RecognitionException {
        CliParser.attrValue_return retval = new CliParser.attrValue_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CliParser.arrayConstruct_return arrayConstruct208 = null;

        CliParser.attrValueString_return attrValueString209 = null;

        CliParser.attrValueInt_return attrValueInt210 = null;

        CliParser.attrValueDouble_return attrValueDouble211 = null;



        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:386:5: ( arrayConstruct | attrValueString | attrValueInt | attrValueDouble )
            int alt25=4;
            switch ( input.LA(1) ) {
            case 113:
                {
                alt25=1;
                }
                break;
            case Identifier:
            case StringLiteral:
                {
                alt25=2;
                }
                break;
            case IntegerPositiveLiteral:
            case IntegerNegativeLiteral:
                {
                alt25=3;
                }
                break;
            case DoubleLiteral:
                {
                alt25=4;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 25, 0, input);

                throw nvae;
            }

            switch (alt25) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:386:7: arrayConstruct
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_arrayConstruct_in_attrValue2890);
                    arrayConstruct208=arrayConstruct();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, arrayConstruct208.getTree());

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:387:7: attrValueString
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_attrValueString_in_attrValue2898);
                    attrValueString209=attrValueString();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, attrValueString209.getTree());

                    }
                    break;
                case 3 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:388:7: attrValueInt
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_attrValueInt_in_attrValue2906);
                    attrValueInt210=attrValueInt();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, attrValueInt210.getTree());

                    }
                    break;
                case 4 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:389:7: attrValueDouble
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_attrValueDouble_in_attrValue2914);
                    attrValueDouble211=attrValueDouble();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, attrValueDouble211.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "attrValue"

    public static class arrayConstruct_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayConstruct"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:393:1: arrayConstruct : '[' ( hashConstruct ( ',' )? )+ ']' -> ^( ARRAY ( hashConstruct )+ ) ;
    public final CliParser.arrayConstruct_return arrayConstruct() throws RecognitionException {
        CliParser.arrayConstruct_return retval = new CliParser.arrayConstruct_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal212=null;
        Token char_literal214=null;
        Token char_literal215=null;
        CliParser.hashConstruct_return hashConstruct213 = null;


        CommonTree char_literal212_tree=null;
        CommonTree char_literal214_tree=null;
        CommonTree char_literal215_tree=null;
        RewriteRuleTokenStream stream_114=new RewriteRuleTokenStream(adaptor,"token 114");
        RewriteRuleTokenStream stream_115=new RewriteRuleTokenStream(adaptor,"token 115");
        RewriteRuleTokenStream stream_113=new RewriteRuleTokenStream(adaptor,"token 113");
        RewriteRuleSubtreeStream stream_hashConstruct=new RewriteRuleSubtreeStream(adaptor,"rule hashConstruct");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:394:5: ( '[' ( hashConstruct ( ',' )? )+ ']' -> ^( ARRAY ( hashConstruct )+ ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:394:7: '[' ( hashConstruct ( ',' )? )+ ']'
            {
            char_literal212=(Token)match(input,113,FOLLOW_113_in_arrayConstruct2933); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_113.add(char_literal212);

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:394:11: ( hashConstruct ( ',' )? )+
            int cnt27=0;
            loop27:
            do {
                int alt27=2;
                int LA27_0 = input.LA(1);

                if ( (LA27_0==116) ) {
                    alt27=1;
                }


                switch (alt27) {
            	case 1 :
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:394:12: hashConstruct ( ',' )?
            	    {
            	    pushFollow(FOLLOW_hashConstruct_in_arrayConstruct2936);
            	    hashConstruct213=hashConstruct();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_hashConstruct.add(hashConstruct213.getTree());
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:394:26: ( ',' )?
            	    int alt26=2;
            	    int LA26_0 = input.LA(1);

            	    if ( (LA26_0==114) ) {
            	        alt26=1;
            	    }
            	    switch (alt26) {
            	        case 1 :
            	            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:0:0: ','
            	            {
            	            char_literal214=(Token)match(input,114,FOLLOW_114_in_arrayConstruct2938); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_114.add(char_literal214);


            	            }
            	            break;

            	    }


            	    }
            	    break;

            	default :
            	    if ( cnt27 >= 1 ) break loop27;
            	    if (state.backtracking>0) {state.failed=true; return retval;}
                        EarlyExitException eee =
                            new EarlyExitException(27, input);
                        throw eee;
                }
                cnt27++;
            } while (true);

            char_literal215=(Token)match(input,115,FOLLOW_115_in_arrayConstruct2943); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_115.add(char_literal215);



            // AST REWRITE
            // elements: hashConstruct
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 395:9: -> ^( ARRAY ( hashConstruct )+ )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:395:12: ^( ARRAY ( hashConstruct )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(ARRAY, "ARRAY"), root_1);

                if ( !(stream_hashConstruct.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_hashConstruct.hasNext() ) {
                    adaptor.addChild(root_1, stream_hashConstruct.nextTree());

                }
                stream_hashConstruct.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "arrayConstruct"

    public static class hashConstruct_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "hashConstruct"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:398:1: hashConstruct : '{' hashElementPair ( ',' hashElementPair )* '}' -> ^( HASH ( hashElementPair )+ ) ;
    public final CliParser.hashConstruct_return hashConstruct() throws RecognitionException {
        CliParser.hashConstruct_return retval = new CliParser.hashConstruct_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal216=null;
        Token char_literal218=null;
        Token char_literal220=null;
        CliParser.hashElementPair_return hashElementPair217 = null;

        CliParser.hashElementPair_return hashElementPair219 = null;


        CommonTree char_literal216_tree=null;
        CommonTree char_literal218_tree=null;
        CommonTree char_literal220_tree=null;
        RewriteRuleTokenStream stream_116=new RewriteRuleTokenStream(adaptor,"token 116");
        RewriteRuleTokenStream stream_117=new RewriteRuleTokenStream(adaptor,"token 117");
        RewriteRuleTokenStream stream_114=new RewriteRuleTokenStream(adaptor,"token 114");
        RewriteRuleSubtreeStream stream_hashElementPair=new RewriteRuleSubtreeStream(adaptor,"rule hashElementPair");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:399:5: ( '{' hashElementPair ( ',' hashElementPair )* '}' -> ^( HASH ( hashElementPair )+ ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:399:7: '{' hashElementPair ( ',' hashElementPair )* '}'
            {
            char_literal216=(Token)match(input,116,FOLLOW_116_in_hashConstruct2981); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_116.add(char_literal216);

            pushFollow(FOLLOW_hashElementPair_in_hashConstruct2983);
            hashElementPair217=hashElementPair();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_hashElementPair.add(hashElementPair217.getTree());
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:399:27: ( ',' hashElementPair )*
            loop28:
            do {
                int alt28=2;
                int LA28_0 = input.LA(1);

                if ( (LA28_0==114) ) {
                    alt28=1;
                }


                switch (alt28) {
            	case 1 :
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:399:28: ',' hashElementPair
            	    {
            	    char_literal218=(Token)match(input,114,FOLLOW_114_in_hashConstruct2986); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_114.add(char_literal218);

            	    pushFollow(FOLLOW_hashElementPair_in_hashConstruct2988);
            	    hashElementPair219=hashElementPair();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_hashElementPair.add(hashElementPair219.getTree());

            	    }
            	    break;

            	default :
            	    break loop28;
                }
            } while (true);

            char_literal220=(Token)match(input,117,FOLLOW_117_in_hashConstruct2992); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_117.add(char_literal220);



            // AST REWRITE
            // elements: hashElementPair
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 400:9: -> ^( HASH ( hashElementPair )+ )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:400:12: ^( HASH ( hashElementPair )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(HASH, "HASH"), root_1);

                if ( !(stream_hashElementPair.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_hashElementPair.hasNext() ) {
                    adaptor.addChild(root_1, stream_hashElementPair.nextTree());

                }
                stream_hashElementPair.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "hashConstruct"

    public static class hashElementPair_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "hashElementPair"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:403:1: hashElementPair : rowKey ':' value -> ^( PAIR rowKey value ) ;
    public final CliParser.hashElementPair_return hashElementPair() throws RecognitionException {
        CliParser.hashElementPair_return retval = new CliParser.hashElementPair_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal222=null;
        CliParser.rowKey_return rowKey221 = null;

        CliParser.value_return value223 = null;


        CommonTree char_literal222_tree=null;
        RewriteRuleTokenStream stream_118=new RewriteRuleTokenStream(adaptor,"token 118");
        RewriteRuleSubtreeStream stream_value=new RewriteRuleSubtreeStream(adaptor,"rule value");
        RewriteRuleSubtreeStream stream_rowKey=new RewriteRuleSubtreeStream(adaptor,"rule rowKey");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:404:5: ( rowKey ':' value -> ^( PAIR rowKey value ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:404:7: rowKey ':' value
            {
            pushFollow(FOLLOW_rowKey_in_hashElementPair3028);
            rowKey221=rowKey();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_rowKey.add(rowKey221.getTree());
            char_literal222=(Token)match(input,118,FOLLOW_118_in_hashElementPair3030); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_118.add(char_literal222);

            pushFollow(FOLLOW_value_in_hashElementPair3032);
            value223=value();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_value.add(value223.getTree());


            // AST REWRITE
            // elements: value, rowKey
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 405:9: -> ^( PAIR rowKey value )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:405:12: ^( PAIR rowKey value )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(PAIR, "PAIR"), root_1);

                adaptor.addChild(root_1, stream_rowKey.nextTree());
                adaptor.addChild(root_1, stream_value.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "hashElementPair"

    public static class columnFamilyExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnFamilyExpr"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:408:1: columnFamilyExpr : columnFamily '[' rowKey ']' ( '[' column= columnOrSuperColumn ']' ( '[' super_column= columnOrSuperColumn ']' )? )? -> ^( NODE_COLUMN_ACCESS columnFamily rowKey ( $column ( $super_column)? )? ) ;
    public final CliParser.columnFamilyExpr_return columnFamilyExpr() throws RecognitionException {
        CliParser.columnFamilyExpr_return retval = new CliParser.columnFamilyExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal225=null;
        Token char_literal227=null;
        Token char_literal228=null;
        Token char_literal229=null;
        Token char_literal230=null;
        Token char_literal231=null;
        CliParser.columnOrSuperColumn_return column = null;

        CliParser.columnOrSuperColumn_return super_column = null;

        CliParser.columnFamily_return columnFamily224 = null;

        CliParser.rowKey_return rowKey226 = null;


        CommonTree char_literal225_tree=null;
        CommonTree char_literal227_tree=null;
        CommonTree char_literal228_tree=null;
        CommonTree char_literal229_tree=null;
        CommonTree char_literal230_tree=null;
        CommonTree char_literal231_tree=null;
        RewriteRuleTokenStream stream_115=new RewriteRuleTokenStream(adaptor,"token 115");
        RewriteRuleTokenStream stream_113=new RewriteRuleTokenStream(adaptor,"token 113");
        RewriteRuleSubtreeStream stream_columnFamily=new RewriteRuleSubtreeStream(adaptor,"rule columnFamily");
        RewriteRuleSubtreeStream stream_rowKey=new RewriteRuleSubtreeStream(adaptor,"rule rowKey");
        RewriteRuleSubtreeStream stream_columnOrSuperColumn=new RewriteRuleSubtreeStream(adaptor,"rule columnOrSuperColumn");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:409:5: ( columnFamily '[' rowKey ']' ( '[' column= columnOrSuperColumn ']' ( '[' super_column= columnOrSuperColumn ']' )? )? -> ^( NODE_COLUMN_ACCESS columnFamily rowKey ( $column ( $super_column)? )? ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:409:7: columnFamily '[' rowKey ']' ( '[' column= columnOrSuperColumn ']' ( '[' super_column= columnOrSuperColumn ']' )? )?
            {
            pushFollow(FOLLOW_columnFamily_in_columnFamilyExpr3067);
            columnFamily224=columnFamily();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_columnFamily.add(columnFamily224.getTree());
            char_literal225=(Token)match(input,113,FOLLOW_113_in_columnFamilyExpr3069); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_113.add(char_literal225);

            pushFollow(FOLLOW_rowKey_in_columnFamilyExpr3071);
            rowKey226=rowKey();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_rowKey.add(rowKey226.getTree());
            char_literal227=(Token)match(input,115,FOLLOW_115_in_columnFamilyExpr3073); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_115.add(char_literal227);

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:410:9: ( '[' column= columnOrSuperColumn ']' ( '[' super_column= columnOrSuperColumn ']' )? )?
            int alt30=2;
            int LA30_0 = input.LA(1);

            if ( (LA30_0==113) ) {
                alt30=1;
            }
            switch (alt30) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:410:11: '[' column= columnOrSuperColumn ']' ( '[' super_column= columnOrSuperColumn ']' )?
                    {
                    char_literal228=(Token)match(input,113,FOLLOW_113_in_columnFamilyExpr3086); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_113.add(char_literal228);

                    pushFollow(FOLLOW_columnOrSuperColumn_in_columnFamilyExpr3090);
                    column=columnOrSuperColumn();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_columnOrSuperColumn.add(column.getTree());
                    char_literal229=(Token)match(input,115,FOLLOW_115_in_columnFamilyExpr3092); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_115.add(char_literal229);

                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:411:13: ( '[' super_column= columnOrSuperColumn ']' )?
                    int alt29=2;
                    int LA29_0 = input.LA(1);

                    if ( (LA29_0==113) ) {
                        alt29=1;
                    }
                    switch (alt29) {
                        case 1 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:411:14: '[' super_column= columnOrSuperColumn ']'
                            {
                            char_literal230=(Token)match(input,113,FOLLOW_113_in_columnFamilyExpr3108); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_113.add(char_literal230);

                            pushFollow(FOLLOW_columnOrSuperColumn_in_columnFamilyExpr3112);
                            super_column=columnOrSuperColumn();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_columnOrSuperColumn.add(super_column.getTree());
                            char_literal231=(Token)match(input,115,FOLLOW_115_in_columnFamilyExpr3114); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_115.add(char_literal231);


                            }
                            break;

                    }


                    }
                    break;

            }



            // AST REWRITE
            // elements: columnFamily, super_column, column, rowKey
            // token labels: 
            // rule labels: retval, column, super_column
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_column=new RewriteRuleSubtreeStream(adaptor,"rule column",column!=null?column.tree:null);
            RewriteRuleSubtreeStream stream_super_column=new RewriteRuleSubtreeStream(adaptor,"rule super_column",super_column!=null?super_column.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 413:7: -> ^( NODE_COLUMN_ACCESS columnFamily rowKey ( $column ( $super_column)? )? )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:413:10: ^( NODE_COLUMN_ACCESS columnFamily rowKey ( $column ( $super_column)? )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_COLUMN_ACCESS, "NODE_COLUMN_ACCESS"), root_1);

                adaptor.addChild(root_1, stream_columnFamily.nextTree());
                adaptor.addChild(root_1, stream_rowKey.nextTree());
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:413:51: ( $column ( $super_column)? )?
                if ( stream_super_column.hasNext()||stream_column.hasNext() ) {
                    adaptor.addChild(root_1, stream_column.nextTree());
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:413:60: ( $super_column)?
                    if ( stream_super_column.hasNext() ) {
                        adaptor.addChild(root_1, stream_super_column.nextTree());

                    }
                    stream_super_column.reset();

                }
                stream_super_column.reset();
                stream_column.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnFamilyExpr"

    public static class keyRangeExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "keyRangeExpr"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:416:1: keyRangeExpr : '[' ( ( startKey )? ':' ( endKey )? )? ']' -> ^( NODE_KEY_RANGE ( startKey )? ( endKey )? ) ;
    public final CliParser.keyRangeExpr_return keyRangeExpr() throws RecognitionException {
        CliParser.keyRangeExpr_return retval = new CliParser.keyRangeExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal232=null;
        Token char_literal234=null;
        Token char_literal236=null;
        CliParser.startKey_return startKey233 = null;

        CliParser.endKey_return endKey235 = null;


        CommonTree char_literal232_tree=null;
        CommonTree char_literal234_tree=null;
        CommonTree char_literal236_tree=null;
        RewriteRuleTokenStream stream_115=new RewriteRuleTokenStream(adaptor,"token 115");
        RewriteRuleTokenStream stream_113=new RewriteRuleTokenStream(adaptor,"token 113");
        RewriteRuleTokenStream stream_118=new RewriteRuleTokenStream(adaptor,"token 118");
        RewriteRuleSubtreeStream stream_endKey=new RewriteRuleSubtreeStream(adaptor,"rule endKey");
        RewriteRuleSubtreeStream stream_startKey=new RewriteRuleSubtreeStream(adaptor,"rule startKey");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:417:5: ( '[' ( ( startKey )? ':' ( endKey )? )? ']' -> ^( NODE_KEY_RANGE ( startKey )? ( endKey )? ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:417:10: '[' ( ( startKey )? ':' ( endKey )? )? ']'
            {
            char_literal232=(Token)match(input,113,FOLLOW_113_in_keyRangeExpr3177); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_113.add(char_literal232);

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:417:14: ( ( startKey )? ':' ( endKey )? )?
            int alt33=2;
            int LA33_0 = input.LA(1);

            if ( ((LA33_0>=Identifier && LA33_0<=StringLiteral)||LA33_0==118) ) {
                alt33=1;
            }
            switch (alt33) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:417:16: ( startKey )? ':' ( endKey )?
                    {
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:417:16: ( startKey )?
                    int alt31=2;
                    int LA31_0 = input.LA(1);

                    if ( ((LA31_0>=Identifier && LA31_0<=StringLiteral)) ) {
                        alt31=1;
                    }
                    switch (alt31) {
                        case 1 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:0:0: startKey
                            {
                            pushFollow(FOLLOW_startKey_in_keyRangeExpr3181);
                            startKey233=startKey();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_startKey.add(startKey233.getTree());

                            }
                            break;

                    }

                    char_literal234=(Token)match(input,118,FOLLOW_118_in_keyRangeExpr3184); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_118.add(char_literal234);

                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:417:30: ( endKey )?
                    int alt32=2;
                    int LA32_0 = input.LA(1);

                    if ( ((LA32_0>=Identifier && LA32_0<=StringLiteral)) ) {
                        alt32=1;
                    }
                    switch (alt32) {
                        case 1 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:0:0: endKey
                            {
                            pushFollow(FOLLOW_endKey_in_keyRangeExpr3186);
                            endKey235=endKey();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_endKey.add(endKey235.getTree());

                            }
                            break;

                    }


                    }
                    break;

            }

            char_literal236=(Token)match(input,115,FOLLOW_115_in_keyRangeExpr3192); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_115.add(char_literal236);



            // AST REWRITE
            // elements: endKey, startKey
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 418:7: -> ^( NODE_KEY_RANGE ( startKey )? ( endKey )? )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:418:10: ^( NODE_KEY_RANGE ( startKey )? ( endKey )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_KEY_RANGE, "NODE_KEY_RANGE"), root_1);

                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:418:27: ( startKey )?
                if ( stream_startKey.hasNext() ) {
                    adaptor.addChild(root_1, stream_startKey.nextTree());

                }
                stream_startKey.reset();
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:418:37: ( endKey )?
                if ( stream_endKey.hasNext() ) {
                    adaptor.addChild(root_1, stream_endKey.nextTree());

                }
                stream_endKey.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "keyRangeExpr"

    public static class columnName_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnName"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:421:1: columnName : Identifier ;
    public final CliParser.columnName_return columnName() throws RecognitionException {
        CliParser.columnName_return retval = new CliParser.columnName_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier237=null;

        CommonTree Identifier237_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:422:2: ( Identifier )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:422:4: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier237=(Token)match(input,Identifier,FOLLOW_Identifier_in_columnName3224); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            Identifier237_tree = (CommonTree)adaptor.create(Identifier237);
            adaptor.addChild(root_0, Identifier237_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnName"

    public static class attr_name_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "attr_name"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:425:1: attr_name : Identifier ;
    public final CliParser.attr_name_return attr_name() throws RecognitionException {
        CliParser.attr_name_return retval = new CliParser.attr_name_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier238=null;

        CommonTree Identifier238_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:426:2: ( Identifier )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:426:4: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier238=(Token)match(input,Identifier,FOLLOW_Identifier_in_attr_name3235); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            Identifier238_tree = (CommonTree)adaptor.create(Identifier238);
            adaptor.addChild(root_0, Identifier238_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "attr_name"

    public static class attrValueString_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "attrValueString"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:429:1: attrValueString : ( Identifier | StringLiteral ) ;
    public final CliParser.attrValueString_return attrValueString() throws RecognitionException {
        CliParser.attrValueString_return retval = new CliParser.attrValueString_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set239=null;

        CommonTree set239_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:430:2: ( ( Identifier | StringLiteral ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:430:4: ( Identifier | StringLiteral )
            {
            root_0 = (CommonTree)adaptor.nil();

            set239=(Token)input.LT(1);
            if ( (input.LA(1)>=Identifier && input.LA(1)<=StringLiteral) ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set239));
                state.errorRecovery=false;state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "attrValueString"

    public static class attrValueInt_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "attrValueInt"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:433:1: attrValueInt : ( IntegerPositiveLiteral | IntegerNegativeLiteral );
    public final CliParser.attrValueInt_return attrValueInt() throws RecognitionException {
        CliParser.attrValueInt_return retval = new CliParser.attrValueInt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set240=null;

        CommonTree set240_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:434:2: ( IntegerPositiveLiteral | IntegerNegativeLiteral )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:
            {
            root_0 = (CommonTree)adaptor.nil();

            set240=(Token)input.LT(1);
            if ( input.LA(1)==IntegerPositiveLiteral||input.LA(1)==IntegerNegativeLiteral ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set240));
                state.errorRecovery=false;state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "attrValueInt"

    public static class attrValueDouble_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "attrValueDouble"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:438:1: attrValueDouble : DoubleLiteral ;
    public final CliParser.attrValueDouble_return attrValueDouble() throws RecognitionException {
        CliParser.attrValueDouble_return retval = new CliParser.attrValueDouble_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DoubleLiteral241=null;

        CommonTree DoubleLiteral241_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:439:2: ( DoubleLiteral )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:439:4: DoubleLiteral
            {
            root_0 = (CommonTree)adaptor.nil();

            DoubleLiteral241=(Token)match(input,DoubleLiteral,FOLLOW_DoubleLiteral_in_attrValueDouble3286); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            DoubleLiteral241_tree = (CommonTree)adaptor.create(DoubleLiteral241);
            adaptor.addChild(root_0, DoubleLiteral241_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "attrValueDouble"

    public static class objectName_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "objectName"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:442:1: objectName : Identifier ;
    public final CliParser.objectName_return objectName() throws RecognitionException {
        CliParser.objectName_return retval = new CliParser.objectName_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier242=null;

        CommonTree Identifier242_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:443:2: ( Identifier )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:443:4: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier242=(Token)match(input,Identifier,FOLLOW_Identifier_in_objectName3299); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            Identifier242_tree = (CommonTree)adaptor.create(Identifier242);
            adaptor.addChild(root_0, Identifier242_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "objectName"

    public static class keyspace_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "keyspace"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:446:1: keyspace : Identifier ;
    public final CliParser.keyspace_return keyspace() throws RecognitionException {
        CliParser.keyspace_return retval = new CliParser.keyspace_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier243=null;

        CommonTree Identifier243_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:447:2: ( Identifier )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:447:4: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier243=(Token)match(input,Identifier,FOLLOW_Identifier_in_keyspace3310); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            Identifier243_tree = (CommonTree)adaptor.create(Identifier243);
            adaptor.addChild(root_0, Identifier243_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "keyspace"

    public static class replica_placement_strategy_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "replica_placement_strategy"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:450:1: replica_placement_strategy : StringLiteral ;
    public final CliParser.replica_placement_strategy_return replica_placement_strategy() throws RecognitionException {
        CliParser.replica_placement_strategy_return retval = new CliParser.replica_placement_strategy_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token StringLiteral244=null;

        CommonTree StringLiteral244_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:451:2: ( StringLiteral )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:451:4: StringLiteral
            {
            root_0 = (CommonTree)adaptor.nil();

            StringLiteral244=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_replica_placement_strategy3321); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            StringLiteral244_tree = (CommonTree)adaptor.create(StringLiteral244);
            adaptor.addChild(root_0, StringLiteral244_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "replica_placement_strategy"

    public static class keyspaceNewName_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "keyspaceNewName"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:454:1: keyspaceNewName : Identifier ;
    public final CliParser.keyspaceNewName_return keyspaceNewName() throws RecognitionException {
        CliParser.keyspaceNewName_return retval = new CliParser.keyspaceNewName_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier245=null;

        CommonTree Identifier245_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:455:2: ( Identifier )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:455:4: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier245=(Token)match(input,Identifier,FOLLOW_Identifier_in_keyspaceNewName3332); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            Identifier245_tree = (CommonTree)adaptor.create(Identifier245);
            adaptor.addChild(root_0, Identifier245_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "keyspaceNewName"

    public static class comparator_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "comparator"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:458:1: comparator : StringLiteral ;
    public final CliParser.comparator_return comparator() throws RecognitionException {
        CliParser.comparator_return retval = new CliParser.comparator_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token StringLiteral246=null;

        CommonTree StringLiteral246_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:459:2: ( StringLiteral )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:459:4: StringLiteral
            {
            root_0 = (CommonTree)adaptor.nil();

            StringLiteral246=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_comparator3343); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            StringLiteral246_tree = (CommonTree)adaptor.create(StringLiteral246);
            adaptor.addChild(root_0, StringLiteral246_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "comparator"

    public static class command_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "command"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:462:1: command : Identifier ;
    public final CliParser.command_return command() throws RecognitionException {
        CliParser.command_return retval = new CliParser.command_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier247=null;

        CommonTree Identifier247_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:462:9: ( Identifier )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:462:11: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier247=(Token)match(input,Identifier,FOLLOW_Identifier_in_command3359); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            Identifier247_tree = (CommonTree)adaptor.create(Identifier247);
            adaptor.addChild(root_0, Identifier247_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "command"

    public static class newColumnFamily_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "newColumnFamily"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:465:1: newColumnFamily : Identifier ;
    public final CliParser.newColumnFamily_return newColumnFamily() throws RecognitionException {
        CliParser.newColumnFamily_return retval = new CliParser.newColumnFamily_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier248=null;

        CommonTree Identifier248_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:466:2: ( Identifier )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:466:4: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier248=(Token)match(input,Identifier,FOLLOW_Identifier_in_newColumnFamily3370); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            Identifier248_tree = (CommonTree)adaptor.create(Identifier248);
            adaptor.addChild(root_0, Identifier248_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "newColumnFamily"

    public static class username_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "username"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:469:1: username : Identifier ;
    public final CliParser.username_return username() throws RecognitionException {
        CliParser.username_return retval = new CliParser.username_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier249=null;

        CommonTree Identifier249_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:469:9: ( Identifier )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:469:11: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier249=(Token)match(input,Identifier,FOLLOW_Identifier_in_username3379); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            Identifier249_tree = (CommonTree)adaptor.create(Identifier249);
            adaptor.addChild(root_0, Identifier249_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "username"

    public static class password_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "password"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:472:1: password : StringLiteral ;
    public final CliParser.password_return password() throws RecognitionException {
        CliParser.password_return retval = new CliParser.password_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token StringLiteral250=null;

        CommonTree StringLiteral250_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:472:9: ( StringLiteral )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:472:11: StringLiteral
            {
            root_0 = (CommonTree)adaptor.nil();

            StringLiteral250=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_password3388); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            StringLiteral250_tree = (CommonTree)adaptor.create(StringLiteral250);
            adaptor.addChild(root_0, StringLiteral250_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "password"

    public static class columnFamily_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnFamily"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:475:1: columnFamily : Identifier ;
    public final CliParser.columnFamily_return columnFamily() throws RecognitionException {
        CliParser.columnFamily_return retval = new CliParser.columnFamily_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier251=null;

        CommonTree Identifier251_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:476:2: ( Identifier )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:476:4: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier251=(Token)match(input,Identifier,FOLLOW_Identifier_in_columnFamily3399); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            Identifier251_tree = (CommonTree)adaptor.create(Identifier251);
            adaptor.addChild(root_0, Identifier251_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnFamily"

    public static class rowKey_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "rowKey"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:479:1: rowKey : ( Identifier | StringLiteral | IntegerPositiveLiteral | IntegerNegativeLiteral | functionCall ) ;
    public final CliParser.rowKey_return rowKey() throws RecognitionException {
        CliParser.rowKey_return retval = new CliParser.rowKey_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier252=null;
        Token StringLiteral253=null;
        Token IntegerPositiveLiteral254=null;
        Token IntegerNegativeLiteral255=null;
        CliParser.functionCall_return functionCall256 = null;


        CommonTree Identifier252_tree=null;
        CommonTree StringLiteral253_tree=null;
        CommonTree IntegerPositiveLiteral254_tree=null;
        CommonTree IntegerNegativeLiteral255_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:480:5: ( ( Identifier | StringLiteral | IntegerPositiveLiteral | IntegerNegativeLiteral | functionCall ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:480:8: ( Identifier | StringLiteral | IntegerPositiveLiteral | IntegerNegativeLiteral | functionCall )
            {
            root_0 = (CommonTree)adaptor.nil();

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:480:8: ( Identifier | StringLiteral | IntegerPositiveLiteral | IntegerNegativeLiteral | functionCall )
            int alt34=5;
            switch ( input.LA(1) ) {
            case Identifier:
                {
                int LA34_1 = input.LA(2);

                if ( (LA34_1==119) ) {
                    alt34=5;
                }
                else if ( (LA34_1==115||LA34_1==118) ) {
                    alt34=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 34, 1, input);

                    throw nvae;
                }
                }
                break;
            case StringLiteral:
                {
                alt34=2;
                }
                break;
            case IntegerPositiveLiteral:
                {
                alt34=3;
                }
                break;
            case IntegerNegativeLiteral:
                {
                alt34=4;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 34, 0, input);

                throw nvae;
            }

            switch (alt34) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:480:9: Identifier
                    {
                    Identifier252=(Token)match(input,Identifier,FOLLOW_Identifier_in_rowKey3416); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    Identifier252_tree = (CommonTree)adaptor.create(Identifier252);
                    adaptor.addChild(root_0, Identifier252_tree);
                    }

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:480:22: StringLiteral
                    {
                    StringLiteral253=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_rowKey3420); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    StringLiteral253_tree = (CommonTree)adaptor.create(StringLiteral253);
                    adaptor.addChild(root_0, StringLiteral253_tree);
                    }

                    }
                    break;
                case 3 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:480:38: IntegerPositiveLiteral
                    {
                    IntegerPositiveLiteral254=(Token)match(input,IntegerPositiveLiteral,FOLLOW_IntegerPositiveLiteral_in_rowKey3424); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    IntegerPositiveLiteral254_tree = (CommonTree)adaptor.create(IntegerPositiveLiteral254);
                    adaptor.addChild(root_0, IntegerPositiveLiteral254_tree);
                    }

                    }
                    break;
                case 4 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:480:63: IntegerNegativeLiteral
                    {
                    IntegerNegativeLiteral255=(Token)match(input,IntegerNegativeLiteral,FOLLOW_IntegerNegativeLiteral_in_rowKey3428); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    IntegerNegativeLiteral255_tree = (CommonTree)adaptor.create(IntegerNegativeLiteral255);
                    adaptor.addChild(root_0, IntegerNegativeLiteral255_tree);
                    }

                    }
                    break;
                case 5 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:480:88: functionCall
                    {
                    pushFollow(FOLLOW_functionCall_in_rowKey3432);
                    functionCall256=functionCall();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, functionCall256.getTree());

                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "rowKey"

    public static class value_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "value"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:483:1: value : ( Identifier | IntegerPositiveLiteral | IntegerNegativeLiteral | StringLiteral | functionCall ) ;
    public final CliParser.value_return value() throws RecognitionException {
        CliParser.value_return retval = new CliParser.value_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier257=null;
        Token IntegerPositiveLiteral258=null;
        Token IntegerNegativeLiteral259=null;
        Token StringLiteral260=null;
        CliParser.functionCall_return functionCall261 = null;


        CommonTree Identifier257_tree=null;
        CommonTree IntegerPositiveLiteral258_tree=null;
        CommonTree IntegerNegativeLiteral259_tree=null;
        CommonTree StringLiteral260_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:484:5: ( ( Identifier | IntegerPositiveLiteral | IntegerNegativeLiteral | StringLiteral | functionCall ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:484:7: ( Identifier | IntegerPositiveLiteral | IntegerNegativeLiteral | StringLiteral | functionCall )
            {
            root_0 = (CommonTree)adaptor.nil();

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:484:7: ( Identifier | IntegerPositiveLiteral | IntegerNegativeLiteral | StringLiteral | functionCall )
            int alt35=5;
            switch ( input.LA(1) ) {
            case Identifier:
                {
                int LA35_1 = input.LA(2);

                if ( (LA35_1==119) ) {
                    alt35=5;
                }
                else if ( (LA35_1==EOF||LA35_1==SEMICOLON||LA35_1==WITH||LA35_1==AND||LA35_1==LIMIT||LA35_1==114||LA35_1==117) ) {
                    alt35=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 35, 1, input);

                    throw nvae;
                }
                }
                break;
            case IntegerPositiveLiteral:
                {
                alt35=2;
                }
                break;
            case IntegerNegativeLiteral:
                {
                alt35=3;
                }
                break;
            case StringLiteral:
                {
                alt35=4;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 35, 0, input);

                throw nvae;
            }

            switch (alt35) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:484:8: Identifier
                    {
                    Identifier257=(Token)match(input,Identifier,FOLLOW_Identifier_in_value3449); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    Identifier257_tree = (CommonTree)adaptor.create(Identifier257);
                    adaptor.addChild(root_0, Identifier257_tree);
                    }

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:484:21: IntegerPositiveLiteral
                    {
                    IntegerPositiveLiteral258=(Token)match(input,IntegerPositiveLiteral,FOLLOW_IntegerPositiveLiteral_in_value3453); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    IntegerPositiveLiteral258_tree = (CommonTree)adaptor.create(IntegerPositiveLiteral258);
                    adaptor.addChild(root_0, IntegerPositiveLiteral258_tree);
                    }

                    }
                    break;
                case 3 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:484:46: IntegerNegativeLiteral
                    {
                    IntegerNegativeLiteral259=(Token)match(input,IntegerNegativeLiteral,FOLLOW_IntegerNegativeLiteral_in_value3457); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    IntegerNegativeLiteral259_tree = (CommonTree)adaptor.create(IntegerNegativeLiteral259);
                    adaptor.addChild(root_0, IntegerNegativeLiteral259_tree);
                    }

                    }
                    break;
                case 4 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:484:71: StringLiteral
                    {
                    StringLiteral260=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_value3461); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    StringLiteral260_tree = (CommonTree)adaptor.create(StringLiteral260);
                    adaptor.addChild(root_0, StringLiteral260_tree);
                    }

                    }
                    break;
                case 5 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:484:87: functionCall
                    {
                    pushFollow(FOLLOW_functionCall_in_value3465);
                    functionCall261=functionCall();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, functionCall261.getTree());

                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "value"

    public static class functionCall_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "functionCall"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:487:1: functionCall : functionName= Identifier '(' ( functionArgument )? ')' -> ^( FUNCTION_CALL $functionName ( functionArgument )? ) ;
    public final CliParser.functionCall_return functionCall() throws RecognitionException {
        CliParser.functionCall_return retval = new CliParser.functionCall_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token functionName=null;
        Token char_literal262=null;
        Token char_literal264=null;
        CliParser.functionArgument_return functionArgument263 = null;


        CommonTree functionName_tree=null;
        CommonTree char_literal262_tree=null;
        CommonTree char_literal264_tree=null;
        RewriteRuleTokenStream stream_120=new RewriteRuleTokenStream(adaptor,"token 120");
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleTokenStream stream_119=new RewriteRuleTokenStream(adaptor,"token 119");
        RewriteRuleSubtreeStream stream_functionArgument=new RewriteRuleSubtreeStream(adaptor,"rule functionArgument");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:488:5: (functionName= Identifier '(' ( functionArgument )? ')' -> ^( FUNCTION_CALL $functionName ( functionArgument )? ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:488:7: functionName= Identifier '(' ( functionArgument )? ')'
            {
            functionName=(Token)match(input,Identifier,FOLLOW_Identifier_in_functionCall3483); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_Identifier.add(functionName);

            char_literal262=(Token)match(input,119,FOLLOW_119_in_functionCall3485); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_119.add(char_literal262);

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:488:35: ( functionArgument )?
            int alt36=2;
            int LA36_0 = input.LA(1);

            if ( ((LA36_0>=IntegerPositiveLiteral && LA36_0<=StringLiteral)||LA36_0==IntegerNegativeLiteral) ) {
                alt36=1;
            }
            switch (alt36) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:0:0: functionArgument
                    {
                    pushFollow(FOLLOW_functionArgument_in_functionCall3487);
                    functionArgument263=functionArgument();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_functionArgument.add(functionArgument263.getTree());

                    }
                    break;

            }

            char_literal264=(Token)match(input,120,FOLLOW_120_in_functionCall3490); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_120.add(char_literal264);



            // AST REWRITE
            // elements: functionArgument, functionName
            // token labels: functionName
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleTokenStream stream_functionName=new RewriteRuleTokenStream(adaptor,"token functionName",functionName);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 489:9: -> ^( FUNCTION_CALL $functionName ( functionArgument )? )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:489:12: ^( FUNCTION_CALL $functionName ( functionArgument )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(FUNCTION_CALL, "FUNCTION_CALL"), root_1);

                adaptor.addChild(root_1, stream_functionName.nextNode());
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:489:42: ( functionArgument )?
                if ( stream_functionArgument.hasNext() ) {
                    adaptor.addChild(root_1, stream_functionArgument.nextTree());

                }
                stream_functionArgument.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "functionCall"

    public static class functionArgument_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "functionArgument"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:492:1: functionArgument : ( Identifier | StringLiteral | IntegerPositiveLiteral | IntegerNegativeLiteral );
    public final CliParser.functionArgument_return functionArgument() throws RecognitionException {
        CliParser.functionArgument_return retval = new CliParser.functionArgument_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set265=null;

        CommonTree set265_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:493:5: ( Identifier | StringLiteral | IntegerPositiveLiteral | IntegerNegativeLiteral )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:
            {
            root_0 = (CommonTree)adaptor.nil();

            set265=(Token)input.LT(1);
            if ( (input.LA(1)>=IntegerPositiveLiteral && input.LA(1)<=StringLiteral)||input.LA(1)==IntegerNegativeLiteral ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set265));
                state.errorRecovery=false;state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "functionArgument"

    public static class startKey_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "startKey"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:496:1: startKey : ( Identifier | StringLiteral ) ;
    public final CliParser.startKey_return startKey() throws RecognitionException {
        CliParser.startKey_return retval = new CliParser.startKey_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set266=null;

        CommonTree set266_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:497:5: ( ( Identifier | StringLiteral ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:497:7: ( Identifier | StringLiteral )
            {
            root_0 = (CommonTree)adaptor.nil();

            set266=(Token)input.LT(1);
            if ( (input.LA(1)>=Identifier && input.LA(1)<=StringLiteral) ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set266));
                state.errorRecovery=false;state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "startKey"

    public static class endKey_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "endKey"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:500:1: endKey : ( Identifier | StringLiteral ) ;
    public final CliParser.endKey_return endKey() throws RecognitionException {
        CliParser.endKey_return retval = new CliParser.endKey_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set267=null;

        CommonTree set267_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:501:5: ( ( Identifier | StringLiteral ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:501:7: ( Identifier | StringLiteral )
            {
            root_0 = (CommonTree)adaptor.nil();

            set267=(Token)input.LT(1);
            if ( (input.LA(1)>=Identifier && input.LA(1)<=StringLiteral) ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set267));
                state.errorRecovery=false;state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "endKey"

    public static class columnOrSuperColumn_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnOrSuperColumn"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:504:1: columnOrSuperColumn : ( Identifier | IntegerPositiveLiteral | IntegerNegativeLiteral | StringLiteral | functionCall ) ;
    public final CliParser.columnOrSuperColumn_return columnOrSuperColumn() throws RecognitionException {
        CliParser.columnOrSuperColumn_return retval = new CliParser.columnOrSuperColumn_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier268=null;
        Token IntegerPositiveLiteral269=null;
        Token IntegerNegativeLiteral270=null;
        Token StringLiteral271=null;
        CliParser.functionCall_return functionCall272 = null;


        CommonTree Identifier268_tree=null;
        CommonTree IntegerPositiveLiteral269_tree=null;
        CommonTree IntegerNegativeLiteral270_tree=null;
        CommonTree StringLiteral271_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:505:2: ( ( Identifier | IntegerPositiveLiteral | IntegerNegativeLiteral | StringLiteral | functionCall ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:505:4: ( Identifier | IntegerPositiveLiteral | IntegerNegativeLiteral | StringLiteral | functionCall )
            {
            root_0 = (CommonTree)adaptor.nil();

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:505:4: ( Identifier | IntegerPositiveLiteral | IntegerNegativeLiteral | StringLiteral | functionCall )
            int alt37=5;
            switch ( input.LA(1) ) {
            case Identifier:
                {
                int LA37_1 = input.LA(2);

                if ( (LA37_1==119) ) {
                    alt37=5;
                }
                else if ( ((LA37_1>=107 && LA37_1<=111)||LA37_1==115) ) {
                    alt37=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 37, 1, input);

                    throw nvae;
                }
                }
                break;
            case IntegerPositiveLiteral:
                {
                alt37=2;
                }
                break;
            case IntegerNegativeLiteral:
                {
                alt37=3;
                }
                break;
            case StringLiteral:
                {
                alt37=4;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 37, 0, input);

                throw nvae;
            }

            switch (alt37) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:505:5: Identifier
                    {
                    Identifier268=(Token)match(input,Identifier,FOLLOW_Identifier_in_columnOrSuperColumn3596); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    Identifier268_tree = (CommonTree)adaptor.create(Identifier268);
                    adaptor.addChild(root_0, Identifier268_tree);
                    }

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:505:18: IntegerPositiveLiteral
                    {
                    IntegerPositiveLiteral269=(Token)match(input,IntegerPositiveLiteral,FOLLOW_IntegerPositiveLiteral_in_columnOrSuperColumn3600); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    IntegerPositiveLiteral269_tree = (CommonTree)adaptor.create(IntegerPositiveLiteral269);
                    adaptor.addChild(root_0, IntegerPositiveLiteral269_tree);
                    }

                    }
                    break;
                case 3 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:505:43: IntegerNegativeLiteral
                    {
                    IntegerNegativeLiteral270=(Token)match(input,IntegerNegativeLiteral,FOLLOW_IntegerNegativeLiteral_in_columnOrSuperColumn3604); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    IntegerNegativeLiteral270_tree = (CommonTree)adaptor.create(IntegerNegativeLiteral270);
                    adaptor.addChild(root_0, IntegerNegativeLiteral270_tree);
                    }

                    }
                    break;
                case 4 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:505:68: StringLiteral
                    {
                    StringLiteral271=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_columnOrSuperColumn3608); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    StringLiteral271_tree = (CommonTree)adaptor.create(StringLiteral271);
                    adaptor.addChild(root_0, StringLiteral271_tree);
                    }

                    }
                    break;
                case 5 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:505:84: functionCall
                    {
                    pushFollow(FOLLOW_functionCall_in_columnOrSuperColumn3612);
                    functionCall272=functionCall();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, functionCall272.getTree());

                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnOrSuperColumn"

    public static class host_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "host"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:508:1: host : host_name -> ^( NODE_ID_LIST host_name ) ;
    public final CliParser.host_return host() throws RecognitionException {
        CliParser.host_return retval = new CliParser.host_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CliParser.host_name_return host_name273 = null;


        RewriteRuleSubtreeStream stream_host_name=new RewriteRuleSubtreeStream(adaptor,"rule host_name");
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:509:5: ( host_name -> ^( NODE_ID_LIST host_name ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:509:7: host_name
            {
            pushFollow(FOLLOW_host_name_in_host3628);
            host_name273=host_name();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_host_name.add(host_name273.getTree());


            // AST REWRITE
            // elements: host_name
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 510:9: -> ^( NODE_ID_LIST host_name )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:510:12: ^( NODE_ID_LIST host_name )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_ID_LIST, "NODE_ID_LIST"), root_1);

                adaptor.addChild(root_1, stream_host_name.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "host"

    public static class host_name_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "host_name"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:513:1: host_name : Identifier ( '.' Identifier )* ;
    public final CliParser.host_name_return host_name() throws RecognitionException {
        CliParser.host_name_return retval = new CliParser.host_name_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier274=null;
        Token char_literal275=null;
        Token Identifier276=null;

        CommonTree Identifier274_tree=null;
        CommonTree char_literal275_tree=null;
        CommonTree Identifier276_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:514:2: ( Identifier ( '.' Identifier )* )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:514:4: Identifier ( '.' Identifier )*
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier274=(Token)match(input,Identifier,FOLLOW_Identifier_in_host_name3655); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            Identifier274_tree = (CommonTree)adaptor.create(Identifier274);
            adaptor.addChild(root_0, Identifier274_tree);
            }
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:514:15: ( '.' Identifier )*
            loop38:
            do {
                int alt38=2;
                int LA38_0 = input.LA(1);

                if ( (LA38_0==112) ) {
                    alt38=1;
                }


                switch (alt38) {
            	case 1 :
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:514:16: '.' Identifier
            	    {
            	    char_literal275=(Token)match(input,112,FOLLOW_112_in_host_name3658); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    char_literal275_tree = (CommonTree)adaptor.create(char_literal275);
            	    adaptor.addChild(root_0, char_literal275_tree);
            	    }
            	    Identifier276=(Token)match(input,Identifier,FOLLOW_Identifier_in_host_name3660); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    Identifier276_tree = (CommonTree)adaptor.create(Identifier276);
            	    adaptor.addChild(root_0, Identifier276_tree);
            	    }

            	    }
            	    break;

            	default :
            	    break loop38;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "host_name"

    public static class ip_address_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "ip_address"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:517:1: ip_address : IP_ADDRESS -> ^( NODE_ID_LIST IP_ADDRESS ) ;
    public final CliParser.ip_address_return ip_address() throws RecognitionException {
        CliParser.ip_address_return retval = new CliParser.ip_address_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token IP_ADDRESS277=null;

        CommonTree IP_ADDRESS277_tree=null;
        RewriteRuleTokenStream stream_IP_ADDRESS=new RewriteRuleTokenStream(adaptor,"token IP_ADDRESS");

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:518:2: ( IP_ADDRESS -> ^( NODE_ID_LIST IP_ADDRESS ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:518:4: IP_ADDRESS
            {
            IP_ADDRESS277=(Token)match(input,IP_ADDRESS,FOLLOW_IP_ADDRESS_in_ip_address3674); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_IP_ADDRESS.add(IP_ADDRESS277);



            // AST REWRITE
            // elements: IP_ADDRESS
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 519:6: -> ^( NODE_ID_LIST IP_ADDRESS )
            {
                // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:519:9: ^( NODE_ID_LIST IP_ADDRESS )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(NODE_ID_LIST, "NODE_ID_LIST"), root_1);

                adaptor.addChild(root_1, stream_IP_ADDRESS.nextNode());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "ip_address"

    public static class port_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "port"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:523:1: port : IntegerPositiveLiteral ;
    public final CliParser.port_return port() throws RecognitionException {
        CliParser.port_return retval = new CliParser.port_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token IntegerPositiveLiteral278=null;

        CommonTree IntegerPositiveLiteral278_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:524:5: ( IntegerPositiveLiteral )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:524:7: IntegerPositiveLiteral
            {
            root_0 = (CommonTree)adaptor.nil();

            IntegerPositiveLiteral278=(Token)match(input,IntegerPositiveLiteral,FOLLOW_IntegerPositiveLiteral_in_port3704); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            IntegerPositiveLiteral278_tree = (CommonTree)adaptor.create(IntegerPositiveLiteral278);
            adaptor.addChild(root_0, IntegerPositiveLiteral278_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "port"

    public static class incrementValue_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "incrementValue"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:527:1: incrementValue : ( IntegerPositiveLiteral | IntegerNegativeLiteral );
    public final CliParser.incrementValue_return incrementValue() throws RecognitionException {
        CliParser.incrementValue_return retval = new CliParser.incrementValue_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set279=null;

        CommonTree set279_tree=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:528:5: ( IntegerPositiveLiteral | IntegerNegativeLiteral )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cli/Cli.g:
            {
            root_0 = (CommonTree)adaptor.nil();

            set279=(Token)input.LT(1);
            if ( input.LA(1)==IntegerPositiveLiteral||input.LA(1)==IntegerNegativeLiteral ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, (CommonTree)adaptor.create(set279));
                state.errorRecovery=false;state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "incrementValue"

    // Delegated rules


    protected DFA2 dfa2 = new DFA2(this);
    protected DFA6 dfa6 = new DFA6(this);
    static final String DFA2_eotS =
        "\35\uffff";
    static final String DFA2_eofS =
        "\1\23\34\uffff";
    static final String DFA2_minS =
        "\1\55\3\uffff\4\62\25\uffff";
    static final String DFA2_maxS =
        "\1\150\3\uffff\1\146\2\72\1\75\25\uffff";
    static final String DFA2_acceptS =
        "\1\uffff\1\1\1\2\1\3\4\uffff\1\14\1\15\1\16\1\17\1\20\1\21\1\22"+
        "\1\23\1\24\1\25\1\26\1\30\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1"+
        "\27";
    static final String DFA2_specialS =
        "\35\uffff}>";
    static final String[] DFA2_transitionS = {
            "\1\23\1\1\1\13\1\10\1\4\1\uffff\2\2\1\16\2\uffff\1\5\1\6\2\uffff"+
            "\1\7\1\uffff\1\12\1\14\2\15\1\11\1\3\1\17\1\20\1\21\1\22\40"+
            "\uffff\1\13",
            "",
            "",
            "",
            "\1\24\63\uffff\1\25",
            "\1\26\7\uffff\1\27",
            "\1\30\7\uffff\1\31",
            "\1\33\7\uffff\1\32\2\uffff\1\34",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA2_eot = DFA.unpackEncodedString(DFA2_eotS);
    static final short[] DFA2_eof = DFA.unpackEncodedString(DFA2_eofS);
    static final char[] DFA2_min = DFA.unpackEncodedStringToUnsignedChars(DFA2_minS);
    static final char[] DFA2_max = DFA.unpackEncodedStringToUnsignedChars(DFA2_maxS);
    static final short[] DFA2_accept = DFA.unpackEncodedString(DFA2_acceptS);
    static final short[] DFA2_special = DFA.unpackEncodedString(DFA2_specialS);
    static final short[][] DFA2_transition;

    static {
        int numStates = DFA2_transitionS.length;
        DFA2_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA2_transition[i] = DFA.unpackEncodedString(DFA2_transitionS[i]);
        }
    }

    class DFA2 extends DFA {

        public DFA2(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 2;
            this.eot = DFA2_eot;
            this.eof = DFA2_eof;
            this.min = DFA2_min;
            this.max = DFA2_max;
            this.accept = DFA2_accept;
            this.special = DFA2_special;
            this.transition = DFA2_transition;
        }
        public String getDescription() {
            return "141:1: statement : ( connectStatement | exitStatement | countStatement | describeTable | describeCluster | addKeyspace | addColumnFamily | updateKeyspace | updateColumnFamily | delColumnFamily | delKeyspace | useKeyspace | delStatement | getStatement | helpStatement | setStatement | incrStatement | showStatement | listStatement | truncateStatement | assumeStatement | consistencyLevelStatement | dropIndex | -> ^( NODE_NO_OP ) );";
        }
    }
    static final String DFA6_eotS =
        "\44\uffff";
    static final String DFA6_eofS =
        "\1\uffff\1\27\42\uffff";
    static final String DFA6_minS =
        "\1\57\1\55\4\uffff\1\62\2\uffff\1\66\3\62\27\uffff";
    static final String DFA6_maxS =
        "\1\150\1\107\4\uffff\1\146\2\uffff\1\147\2\72\1\75\27\uffff";
    static final String DFA6_acceptS =
        "\2\uffff\1\35\1\1\1\2\1\3\1\uffff\1\6\1\7\4\uffff\1\22\1\23\1\24"+
        "\1\25\1\26\1\27\1\30\1\31\1\32\1\33\1\34\1\4\1\5\1\10\1\11\1\12"+
        "\1\13\1\15\1\14\1\16\1\17\1\20\1\21";
    static final String DFA6_specialS =
        "\44\uffff}>";
    static final String[] DFA6_transitionS = {
            "\1\1\70\uffff\1\2",
            "\1\27\1\4\1\3\1\5\1\6\1\uffff\1\7\1\10\1\11\2\uffff\1\12\1"+
            "\13\2\uffff\1\14\1\uffff\1\15\1\16\1\17\1\20\1\21\1\22\1\23"+
            "\1\24\1\25\1\26",
            "",
            "",
            "",
            "",
            "\1\30\63\uffff\1\31",
            "",
            "",
            "\1\33\1\34\57\uffff\1\32",
            "\1\35\7\uffff\1\36",
            "\1\37\7\uffff\1\40",
            "\1\41\7\uffff\1\42\2\uffff\1\43",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA6_eot = DFA.unpackEncodedString(DFA6_eotS);
    static final short[] DFA6_eof = DFA.unpackEncodedString(DFA6_eofS);
    static final char[] DFA6_min = DFA.unpackEncodedStringToUnsignedChars(DFA6_minS);
    static final char[] DFA6_max = DFA.unpackEncodedStringToUnsignedChars(DFA6_maxS);
    static final short[] DFA6_accept = DFA.unpackEncodedString(DFA6_acceptS);
    static final short[] DFA6_special = DFA.unpackEncodedString(DFA6_specialS);
    static final short[][] DFA6_transition;

    static {
        int numStates = DFA6_transitionS.length;
        DFA6_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA6_transition[i] = DFA.unpackEncodedString(DFA6_transitionS[i]);
        }
    }

    class DFA6 extends DFA {

        public DFA6(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 6;
            this.eot = DFA6_eot;
            this.eof = DFA6_eof;
            this.min = DFA6_min;
            this.max = DFA6_max;
            this.accept = DFA6_accept;
            this.special = DFA6_special;
            this.transition = DFA6_transition;
        }
        public String getDescription() {
            return "175:1: helpStatement : ( HELP HELP -> ^( NODE_HELP NODE_HELP ) | HELP CONNECT -> ^( NODE_HELP NODE_CONNECT ) | HELP USE -> ^( NODE_HELP NODE_USE_TABLE ) | HELP DESCRIBE KEYSPACE -> ^( NODE_HELP NODE_DESCRIBE_TABLE ) | HELP DESCRIBE 'CLUSTER' -> ^( NODE_HELP NODE_DESCRIBE_CLUSTER ) | HELP EXIT -> ^( NODE_HELP NODE_EXIT ) | HELP QUIT -> ^( NODE_HELP NODE_EXIT ) | HELP SHOW 'CLUSTER NAME' -> ^( NODE_HELP NODE_SHOW_CLUSTER_NAME ) | HELP SHOW KEYSPACES -> ^( NODE_HELP NODE_SHOW_KEYSPACES ) | HELP SHOW API_VERSION -> ^( NODE_HELP NODE_SHOW_VERSION ) | HELP CREATE KEYSPACE -> ^( NODE_HELP NODE_ADD_KEYSPACE ) | HELP UPDATE KEYSPACE -> ^( NODE_HELP NODE_UPDATE_KEYSPACE ) | HELP CREATE COLUMN FAMILY -> ^( NODE_HELP NODE_ADD_COLUMN_FAMILY ) | HELP UPDATE COLUMN FAMILY -> ^( NODE_HELP NODE_UPDATE_COLUMN_FAMILY ) | HELP DROP KEYSPACE -> ^( NODE_HELP NODE_DEL_KEYSPACE ) | HELP DROP COLUMN FAMILY -> ^( NODE_HELP NODE_DEL_COLUMN_FAMILY ) | HELP DROP INDEX -> ^( NODE_HELP NODE_DROP_INDEX ) | HELP GET -> ^( NODE_HELP NODE_THRIFT_GET ) | HELP SET -> ^( NODE_HELP NODE_THRIFT_SET ) | HELP INCR -> ^( NODE_HELP NODE_THRIFT_INCR ) | HELP DECR -> ^( NODE_HELP NODE_THRIFT_DECR ) | HELP DEL -> ^( NODE_HELP NODE_THRIFT_DEL ) | HELP COUNT -> ^( NODE_HELP NODE_THRIFT_COUNT ) | HELP LIST -> ^( NODE_HELP NODE_LIST ) | HELP TRUNCATE -> ^( NODE_HELP NODE_TRUNCATE ) | HELP ASSUME -> ^( NODE_HELP NODE_ASSUME ) | HELP CONSISTENCYLEVEL -> ^( NODE_HELP NODE_CONSISTENCY_LEVEL ) | HELP -> ^( NODE_HELP ) | '?' -> ^( NODE_HELP ) );";
        }
    }
 

    public static final BitSet FOLLOW_statement_in_root414 = new BitSet(new long[]{0x0000200000000000L});
    public static final BitSet FOLLOW_SEMICOLON_in_root416 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_root419 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_connectStatement_in_statement435 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_exitStatement_in_statement443 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_countStatement_in_statement451 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_describeTable_in_statement459 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_describeCluster_in_statement467 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_addKeyspace_in_statement475 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_addColumnFamily_in_statement483 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_updateKeyspace_in_statement491 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_updateColumnFamily_in_statement499 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_delColumnFamily_in_statement507 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_delKeyspace_in_statement515 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_useKeyspace_in_statement523 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_delStatement_in_statement531 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_getStatement_in_statement539 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_helpStatement_in_statement547 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_setStatement_in_statement555 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_incrStatement_in_statement563 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_showStatement_in_statement571 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_listStatement_in_statement579 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_truncateStatement_in_statement587 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_assumeStatement_in_statement595 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_consistencyLevelStatement_in_statement603 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropIndex_in_statement611 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_CONNECT_in_connectStatement640 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_host_in_connectStatement642 = new BitSet(new long[]{0x0000000000000000L,0x0000002000000000L});
    public static final BitSet FOLLOW_101_in_connectStatement644 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_port_in_connectStatement646 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000200L});
    public static final BitSet FOLLOW_username_in_connectStatement649 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000400L});
    public static final BitSet FOLLOW_password_in_connectStatement651 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_CONNECT_in_connectStatement686 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_ip_address_in_connectStatement688 = new BitSet(new long[]{0x0000000000000000L,0x0000002000000000L});
    public static final BitSet FOLLOW_101_in_connectStatement690 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_port_in_connectStatement692 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000200L});
    public static final BitSet FOLLOW_username_in_connectStatement695 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000400L});
    public static final BitSet FOLLOW_password_in_connectStatement697 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement741 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_HELP_in_helpStatement743 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement768 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_CONNECT_in_helpStatement770 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement795 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_USE_in_helpStatement797 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement822 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_DESCRIBE_in_helpStatement824 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_KEYSPACE_in_helpStatement826 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement851 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_DESCRIBE_in_helpStatement853 = new BitSet(new long[]{0x0000000000000000L,0x0000004000000000L});
    public static final BitSet FOLLOW_102_in_helpStatement855 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement879 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_EXIT_in_helpStatement881 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement906 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_QUIT_in_helpStatement908 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement933 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_SHOW_in_helpStatement935 = new BitSet(new long[]{0x0000000000000000L,0x0000008000000000L});
    public static final BitSet FOLLOW_103_in_helpStatement937 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement961 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_SHOW_in_helpStatement963 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_KEYSPACES_in_helpStatement965 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement990 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_SHOW_in_helpStatement992 = new BitSet(new long[]{0x0080000000000000L});
    public static final BitSet FOLLOW_API_VERSION_in_helpStatement994 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1018 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_CREATE_in_helpStatement1020 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_KEYSPACE_in_helpStatement1022 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1047 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_UPDATE_in_helpStatement1049 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_KEYSPACE_in_helpStatement1051 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1075 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_CREATE_in_helpStatement1077 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_COLUMN_in_helpStatement1079 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_FAMILY_in_helpStatement1081 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1106 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_UPDATE_in_helpStatement1108 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_COLUMN_in_helpStatement1110 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_FAMILY_in_helpStatement1112 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1136 = new BitSet(new long[]{0x1000000000000000L});
    public static final BitSet FOLLOW_DROP_in_helpStatement1138 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_KEYSPACE_in_helpStatement1140 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1165 = new BitSet(new long[]{0x1000000000000000L});
    public static final BitSet FOLLOW_DROP_in_helpStatement1167 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_COLUMN_in_helpStatement1169 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_FAMILY_in_helpStatement1171 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1196 = new BitSet(new long[]{0x1000000000000000L});
    public static final BitSet FOLLOW_DROP_in_helpStatement1198 = new BitSet(new long[]{0x2000000000000000L});
    public static final BitSet FOLLOW_INDEX_in_helpStatement1200 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1224 = new BitSet(new long[]{0x4000000000000000L});
    public static final BitSet FOLLOW_GET_in_helpStatement1226 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1251 = new BitSet(new long[]{0x8000000000000000L});
    public static final BitSet FOLLOW_SET_in_helpStatement1253 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1278 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_INCR_in_helpStatement1280 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1304 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000002L});
    public static final BitSet FOLLOW_DECR_in_helpStatement1306 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1330 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_DEL_in_helpStatement1332 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1357 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000008L});
    public static final BitSet FOLLOW_COUNT_in_helpStatement1359 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1384 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_LIST_in_helpStatement1386 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1411 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000020L});
    public static final BitSet FOLLOW_TRUNCATE_in_helpStatement1413 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1437 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_ASSUME_in_helpStatement1439 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1463 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_CONSISTENCYLEVEL_in_helpStatement1465 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_HELP_in_helpStatement1489 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_104_in_helpStatement1512 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUIT_in_exitStatement1547 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_EXIT_in_exitStatement1561 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_GET_in_getStatement1584 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_columnFamilyExpr_in_getStatement1586 = new BitSet(new long[]{0x0000000000000002L,0x0000020000200000L});
    public static final BitSet FOLLOW_105_in_getStatement1589 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000700L});
    public static final BitSet FOLLOW_typeIdentifier_in_getStatement1591 = new BitSet(new long[]{0x0000000000000002L,0x0000000000200000L});
    public static final BitSet FOLLOW_LIMIT_in_getStatement1596 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_IntegerPositiveLiteral_in_getStatement1600 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_GET_in_getStatement1645 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_columnFamily_in_getStatement1647 = new BitSet(new long[]{0x0000000000000000L,0x0000040000000000L});
    public static final BitSet FOLLOW_106_in_getStatement1649 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010700L});
    public static final BitSet FOLLOW_getCondition_in_getStatement1651 = new BitSet(new long[]{0x0000000000000002L,0x0000000000208000L});
    public static final BitSet FOLLOW_AND_in_getStatement1654 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010700L});
    public static final BitSet FOLLOW_getCondition_in_getStatement1656 = new BitSet(new long[]{0x0000000000000002L,0x0000000000208000L});
    public static final BitSet FOLLOW_LIMIT_in_getStatement1661 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_IntegerPositiveLiteral_in_getStatement1665 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_columnOrSuperColumn_in_getCondition1716 = new BitSet(new long[]{0x0000000000000000L,0x0000F80000000000L});
    public static final BitSet FOLLOW_operator_in_getCondition1718 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010700L});
    public static final BitSet FOLLOW_value_in_getCondition1720 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_operator0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_typeIdentifier0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SET_in_setStatement1816 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_columnFamilyExpr_in_setStatement1818 = new BitSet(new long[]{0x0000000000000000L,0x0000080000000000L});
    public static final BitSet FOLLOW_107_in_setStatement1820 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010700L});
    public static final BitSet FOLLOW_value_in_setStatement1824 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000800L});
    public static final BitSet FOLLOW_WITH_in_setStatement1827 = new BitSet(new long[]{0x0000000000000000L,0x0000000000001000L});
    public static final BitSet FOLLOW_TTL_in_setStatement1829 = new BitSet(new long[]{0x0000000000000000L,0x0000080000000000L});
    public static final BitSet FOLLOW_107_in_setStatement1831 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_IntegerPositiveLiteral_in_setStatement1835 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INCR_in_incrStatement1881 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_columnFamilyExpr_in_incrStatement1883 = new BitSet(new long[]{0x0000000000000002L,0x0000000000002000L});
    public static final BitSet FOLLOW_BY_in_incrStatement1886 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010100L});
    public static final BitSet FOLLOW_incrementValue_in_incrStatement1890 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DECR_in_incrStatement1924 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_columnFamilyExpr_in_incrStatement1926 = new BitSet(new long[]{0x0000000000000002L,0x0000000000002000L});
    public static final BitSet FOLLOW_BY_in_incrStatement1929 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010100L});
    public static final BitSet FOLLOW_incrementValue_in_incrStatement1933 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_COUNT_in_countStatement1976 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_columnFamilyExpr_in_countStatement1978 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DEL_in_delStatement2012 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_columnFamilyExpr_in_delStatement2014 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_showClusterName_in_showStatement2048 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_showVersion_in_showStatement2056 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_showKeyspaces_in_showStatement2064 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LIST_in_listStatement2081 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_columnFamily_in_listStatement2083 = new BitSet(new long[]{0x0000000000000002L,0x0002000000200000L});
    public static final BitSet FOLLOW_keyRangeExpr_in_listStatement2085 = new BitSet(new long[]{0x0000000000000002L,0x0000000000200000L});
    public static final BitSet FOLLOW_LIMIT_in_listStatement2089 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_IntegerPositiveLiteral_in_listStatement2093 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_TRUNCATE_in_truncateStatement2139 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_columnFamily_in_truncateStatement2141 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ASSUME_in_assumeStatement2174 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_columnFamily_in_assumeStatement2176 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_Identifier_in_assumeStatement2180 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
    public static final BitSet FOLLOW_105_in_assumeStatement2182 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_Identifier_in_assumeStatement2186 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_CONSISTENCYLEVEL_in_consistencyLevelStatement2225 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
    public static final BitSet FOLLOW_105_in_consistencyLevelStatement2227 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_Identifier_in_consistencyLevelStatement2231 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SHOW_in_showClusterName2265 = new BitSet(new long[]{0x0000000000000000L,0x0000008000000000L});
    public static final BitSet FOLLOW_103_in_showClusterName2267 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_CREATE_in_addKeyspace2298 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_KEYSPACE_in_addKeyspace2300 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_keyValuePairExpr_in_addKeyspace2302 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_CREATE_in_addColumnFamily2336 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_COLUMN_in_addColumnFamily2338 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_FAMILY_in_addColumnFamily2340 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_keyValuePairExpr_in_addColumnFamily2342 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UPDATE_in_updateKeyspace2376 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_KEYSPACE_in_updateKeyspace2378 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_keyValuePairExpr_in_updateKeyspace2380 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UPDATE_in_updateColumnFamily2413 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_COLUMN_in_updateColumnFamily2415 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_FAMILY_in_updateColumnFamily2417 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_keyValuePairExpr_in_updateColumnFamily2419 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DROP_in_delKeyspace2452 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_KEYSPACE_in_delKeyspace2454 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_keyspace_in_delKeyspace2456 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DROP_in_delColumnFamily2490 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_COLUMN_in_delColumnFamily2492 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_FAMILY_in_delColumnFamily2494 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_columnFamily_in_delColumnFamily2496 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DROP_in_dropIndex2530 = new BitSet(new long[]{0x2000000000000000L});
    public static final BitSet FOLLOW_INDEX_in_dropIndex2532 = new BitSet(new long[]{0x0000000000000000L,0x0000000000004000L});
    public static final BitSet FOLLOW_ON_in_dropIndex2534 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_columnFamily_in_dropIndex2536 = new BitSet(new long[]{0x0000000000000000L,0x0001000000000000L});
    public static final BitSet FOLLOW_112_in_dropIndex2538 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_columnName_in_dropIndex2540 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SHOW_in_showVersion2575 = new BitSet(new long[]{0x0080000000000000L});
    public static final BitSet FOLLOW_API_VERSION_in_showVersion2577 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SHOW_in_showKeyspaces2608 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_KEYSPACES_in_showKeyspaces2610 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DESCRIBE_in_describeTable2642 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_KEYSPACE_in_describeTable2644 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000200L});
    public static final BitSet FOLLOW_keyspace_in_describeTable2647 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DESCRIBE_in_describeCluster2689 = new BitSet(new long[]{0x0000000000000000L,0x0000004000000000L});
    public static final BitSet FOLLOW_102_in_describeCluster2691 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_USE_in_useKeyspace2722 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_keyspace_in_useKeyspace2724 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000600L});
    public static final BitSet FOLLOW_username_in_useKeyspace2728 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000400L});
    public static final BitSet FOLLOW_password_in_useKeyspace2735 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_objectName_in_keyValuePairExpr2787 = new BitSet(new long[]{0x0000000000000002L,0x0000000000008800L});
    public static final BitSet FOLLOW_AND_in_keyValuePairExpr2792 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_WITH_in_keyValuePairExpr2796 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_keyValuePair_in_keyValuePairExpr2799 = new BitSet(new long[]{0x0000000000000002L,0x0000000000008800L});
    public static final BitSet FOLLOW_attr_name_in_keyValuePair2857 = new BitSet(new long[]{0x0000000000000000L,0x0000080000000000L});
    public static final BitSet FOLLOW_107_in_keyValuePair2859 = new BitSet(new long[]{0x0000000000000000L,0x0002000000030700L});
    public static final BitSet FOLLOW_attrValue_in_keyValuePair2861 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayConstruct_in_attrValue2890 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_attrValueString_in_attrValue2898 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_attrValueInt_in_attrValue2906 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_attrValueDouble_in_attrValue2914 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_113_in_arrayConstruct2933 = new BitSet(new long[]{0x0000000000000000L,0x0010000000000000L});
    public static final BitSet FOLLOW_hashConstruct_in_arrayConstruct2936 = new BitSet(new long[]{0x0000000000000000L,0x001C000000000000L});
    public static final BitSet FOLLOW_114_in_arrayConstruct2938 = new BitSet(new long[]{0x0000000000000000L,0x0018000000000000L});
    public static final BitSet FOLLOW_115_in_arrayConstruct2943 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_116_in_hashConstruct2981 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010700L});
    public static final BitSet FOLLOW_hashElementPair_in_hashConstruct2983 = new BitSet(new long[]{0x0000000000000000L,0x0024000000000000L});
    public static final BitSet FOLLOW_114_in_hashConstruct2986 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010700L});
    public static final BitSet FOLLOW_hashElementPair_in_hashConstruct2988 = new BitSet(new long[]{0x0000000000000000L,0x0024000000000000L});
    public static final BitSet FOLLOW_117_in_hashConstruct2992 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rowKey_in_hashElementPair3028 = new BitSet(new long[]{0x0000000000000000L,0x0040000000000000L});
    public static final BitSet FOLLOW_118_in_hashElementPair3030 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010700L});
    public static final BitSet FOLLOW_value_in_hashElementPair3032 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_columnFamily_in_columnFamilyExpr3067 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_columnFamilyExpr3069 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010700L});
    public static final BitSet FOLLOW_rowKey_in_columnFamilyExpr3071 = new BitSet(new long[]{0x0000000000000000L,0x0008000000000000L});
    public static final BitSet FOLLOW_115_in_columnFamilyExpr3073 = new BitSet(new long[]{0x0000000000000002L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_columnFamilyExpr3086 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010700L});
    public static final BitSet FOLLOW_columnOrSuperColumn_in_columnFamilyExpr3090 = new BitSet(new long[]{0x0000000000000000L,0x0008000000000000L});
    public static final BitSet FOLLOW_115_in_columnFamilyExpr3092 = new BitSet(new long[]{0x0000000000000002L,0x0002000000000000L});
    public static final BitSet FOLLOW_113_in_columnFamilyExpr3108 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010700L});
    public static final BitSet FOLLOW_columnOrSuperColumn_in_columnFamilyExpr3112 = new BitSet(new long[]{0x0000000000000000L,0x0008000000000000L});
    public static final BitSet FOLLOW_115_in_columnFamilyExpr3114 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_113_in_keyRangeExpr3177 = new BitSet(new long[]{0x0000000000000000L,0x0048000000000600L});
    public static final BitSet FOLLOW_startKey_in_keyRangeExpr3181 = new BitSet(new long[]{0x0000000000000000L,0x0040000000000000L});
    public static final BitSet FOLLOW_118_in_keyRangeExpr3184 = new BitSet(new long[]{0x0000000000000000L,0x0008000000000600L});
    public static final BitSet FOLLOW_endKey_in_keyRangeExpr3186 = new BitSet(new long[]{0x0000000000000000L,0x0008000000000000L});
    public static final BitSet FOLLOW_115_in_keyRangeExpr3192 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_columnName3224 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_attr_name3235 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_attrValueString3246 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_attrValueInt0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DoubleLiteral_in_attrValueDouble3286 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_objectName3299 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_keyspace3310 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_replica_placement_strategy3321 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_keyspaceNewName3332 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_comparator3343 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_command3359 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_newColumnFamily3370 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_username3379 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_password3388 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_columnFamily3399 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_rowKey3416 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_rowKey3420 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IntegerPositiveLiteral_in_rowKey3424 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IntegerNegativeLiteral_in_rowKey3428 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionCall_in_rowKey3432 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_value3449 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IntegerPositiveLiteral_in_value3453 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IntegerNegativeLiteral_in_value3457 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_value3461 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionCall_in_value3465 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_functionCall3483 = new BitSet(new long[]{0x0000000000000000L,0x0080000000000000L});
    public static final BitSet FOLLOW_119_in_functionCall3485 = new BitSet(new long[]{0x0000000000000000L,0x0100000000010700L});
    public static final BitSet FOLLOW_functionArgument_in_functionCall3487 = new BitSet(new long[]{0x0000000000000000L,0x0100000000000000L});
    public static final BitSet FOLLOW_120_in_functionCall3490 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_functionArgument0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_startKey3557 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_endKey3578 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_columnOrSuperColumn3596 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IntegerPositiveLiteral_in_columnOrSuperColumn3600 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IntegerNegativeLiteral_in_columnOrSuperColumn3604 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_columnOrSuperColumn3608 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionCall_in_columnOrSuperColumn3612 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_host_name_in_host3628 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_host_name3655 = new BitSet(new long[]{0x0000000000000002L,0x0001000000000000L});
    public static final BitSet FOLLOW_112_in_host_name3658 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_Identifier_in_host_name3660 = new BitSet(new long[]{0x0000000000000002L,0x0001000000000000L});
    public static final BitSet FOLLOW_IP_ADDRESS_in_ip_address3674 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IntegerPositiveLiteral_in_port3704 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_incrementValue0 = new BitSet(new long[]{0x0000000000000002L});

}