// $ANTLR 3.0.1 /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g 2008-10-29 16:05:52

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
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "NODE_CONNECT", "NODE_DESCRIBE_TABLE", "NODE_EXIT", "NODE_HELP", "NODE_NO_OP", "NODE_SHOW_CLUSTER_NAME", "NODE_SHOW_CONFIG_FILE", "NODE_SHOW_VERSION", "NODE_SHOW_TABLES", "NODE_THRIFT_GET", "NODE_THRIFT_SET", "NODE_COLUMN_ACCESS", "NODE_ID_LIST", "SEMICOLON", "K_CONNECT", "SLASH", "K_HELP", "K_QUIT", "K_EXIT", "K_THRIFT", "K_GET", "K_SET", "K_SHOW", "K_CLUSTER", "K_NAME", "K_CONFIG", "K_FILE", "K_VERSION", "K_TABLES", "K_DESCRIBE", "K_TABLE", "DOT", "Identifier", "StringLiteral", "IntegerLiteral", "Letter", "Digit", "WS", "COMMENT", "'?'", "'='", "'['", "']'"
    };
    public static final int NODE_SHOW_CONFIG_FILE=10;
    public static final int K_TABLES=32;
    public static final int K_VERSION=31;
    public static final int K_EXIT=22;
    public static final int NODE_EXIT=6;
    public static final int K_FILE=30;
    public static final int K_GET=24;
    public static final int K_CONNECT=18;
    public static final int K_CONFIG=29;
    public static final int SEMICOLON=17;
    public static final int Digit=40;
    public static final int EOF=-1;
    public static final int Identifier=36;
    public static final int NODE_THRIFT_GET=13;
    public static final int K_SET=25;
    public static final int StringLiteral=37;
    public static final int NODE_HELP=7;
    public static final int NODE_NO_OP=8;
    public static final int NODE_THRIFT_SET=14;
    public static final int K_DESCRIBE=33;
    public static final int NODE_SHOW_VERSION=11;
    public static final int NODE_ID_LIST=16;
    public static final int WS=41;
    public static final int NODE_CONNECT=4;
    public static final int SLASH=19;
    public static final int K_THRIFT=23;
    public static final int NODE_SHOW_TABLES=12;
    public static final int K_CLUSTER=27;
    public static final int K_HELP=20;
    public static final int K_SHOW=26;
    public static final int NODE_DESCRIBE_TABLE=5;
    public static final int K_TABLE=34;
    public static final int IntegerLiteral=38;
    public static final int NODE_SHOW_CLUSTER_NAME=9;
    public static final int COMMENT=42;
    public static final int DOT=35;
    public static final int K_NAME=28;
    public static final int Letter=39;
    public static final int NODE_COLUMN_ACCESS=15;
    public static final int K_QUIT=21;

        public CliParser(TokenStream input) {
            super(input);
            ruleMemo = new HashMap[37+1];
         }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return tokenNames; }
    public String getGrammarFileName() { return "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g"; }


    public static class root_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start root
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:73:1: root : stmt ( SEMICOLON )? EOF -> stmt ;
    public final root_return root() throws RecognitionException {
        root_return retval = new root_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token SEMICOLON2=null;
        Token EOF3=null;
        stmt_return stmt1 = null;


        CommonTree SEMICOLON2_tree=null;
        CommonTree EOF3_tree=null;
        RewriteRuleTokenStream stream_SEMICOLON=new RewriteRuleTokenStream(adaptor,"token SEMICOLON");
        RewriteRuleTokenStream stream_EOF=new RewriteRuleTokenStream(adaptor,"token EOF");
        RewriteRuleSubtreeStream stream_stmt=new RewriteRuleSubtreeStream(adaptor,"rule stmt");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:73:5: ( stmt ( SEMICOLON )? EOF -> stmt )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:73:7: stmt ( SEMICOLON )? EOF
            {
            pushFollow(FOLLOW_stmt_in_root200);
            stmt1=stmt();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_stmt.add(stmt1.getTree());
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:73:12: ( SEMICOLON )?
            int alt1=2;
            int LA1_0 = input.LA(1);

            if ( (LA1_0==SEMICOLON) ) {
                alt1=1;
            }
            switch (alt1) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:0:0: SEMICOLON
                    {
                    SEMICOLON2=(Token)input.LT(1);
                    match(input,SEMICOLON,FOLLOW_SEMICOLON_in_root202); if (failed) return retval;
                    if ( backtracking==0 ) stream_SEMICOLON.add(SEMICOLON2);


                    }
                    break;

            }

            EOF3=(Token)input.LT(1);
            match(input,EOF,FOLLOW_EOF_in_root205); if (failed) return retval;
            if ( backtracking==0 ) stream_EOF.add(EOF3);


            // AST REWRITE
            // elements: stmt
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 73:27: -> stmt
            {
                adaptor.addChild(root_0, stream_stmt.next());

            }

            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end root

    public static class stmt_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start stmt
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:75:1: stmt : ( connectStmt | exitStmt | describeTable | getStmt | helpStmt | setStmt | showStmt | -> ^( NODE_NO_OP ) );
    public final stmt_return stmt() throws RecognitionException {
        stmt_return retval = new stmt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        connectStmt_return connectStmt4 = null;

        exitStmt_return exitStmt5 = null;

        describeTable_return describeTable6 = null;

        getStmt_return getStmt7 = null;

        helpStmt_return helpStmt8 = null;

        setStmt_return setStmt9 = null;

        showStmt_return showStmt10 = null;



        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:76:5: ( connectStmt | exitStmt | describeTable | getStmt | helpStmt | setStmt | showStmt | -> ^( NODE_NO_OP ) )
            int alt2=8;
            switch ( input.LA(1) ) {
            case K_CONNECT:
                {
                alt2=1;
                }
                break;
            case K_QUIT:
            case K_EXIT:
                {
                alt2=2;
                }
                break;
            case K_DESCRIBE:
                {
                alt2=3;
                }
                break;
            case K_THRIFT:
                {
                int LA2_4 = input.LA(2);

                if ( (LA2_4==K_SET) ) {
                    alt2=6;
                }
                else if ( (LA2_4==K_GET) ) {
                    alt2=4;
                }
                else {
                    if (backtracking>0) {failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("75:1: stmt : ( connectStmt | exitStmt | describeTable | getStmt | helpStmt | setStmt | showStmt | -> ^( NODE_NO_OP ) );", 2, 4, input);

                    throw nvae;
                }
                }
                break;
            case K_HELP:
            case 43:
                {
                alt2=5;
                }
                break;
            case K_SHOW:
                {
                alt2=7;
                }
                break;
            case EOF:
            case SEMICOLON:
                {
                alt2=8;
                }
                break;
            default:
                if (backtracking>0) {failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("75:1: stmt : ( connectStmt | exitStmt | describeTable | getStmt | helpStmt | setStmt | showStmt | -> ^( NODE_NO_OP ) );", 2, 0, input);

                throw nvae;
            }

            switch (alt2) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:76:7: connectStmt
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_connectStmt_in_stmt221);
                    connectStmt4=connectStmt();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, connectStmt4.getTree());

                    }
                    break;
                case 2 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:77:7: exitStmt
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_exitStmt_in_stmt229);
                    exitStmt5=exitStmt();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, exitStmt5.getTree());

                    }
                    break;
                case 3 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:78:7: describeTable
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_describeTable_in_stmt237);
                    describeTable6=describeTable();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, describeTable6.getTree());

                    }
                    break;
                case 4 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:79:7: getStmt
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_getStmt_in_stmt245);
                    getStmt7=getStmt();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, getStmt7.getTree());

                    }
                    break;
                case 5 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:80:7: helpStmt
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_helpStmt_in_stmt253);
                    helpStmt8=helpStmt();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, helpStmt8.getTree());

                    }
                    break;
                case 6 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:81:7: setStmt
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_setStmt_in_stmt261);
                    setStmt9=setStmt();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, setStmt9.getTree());

                    }
                    break;
                case 7 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:82:7: showStmt
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_showStmt_in_stmt269);
                    showStmt10=showStmt();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, showStmt10.getTree());

                    }
                    break;
                case 8 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:83:7: 
                    {

                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    if ( backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 83:7: -> ^( NODE_NO_OP )
                    {
                        // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:83:10: ^( NODE_NO_OP )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_NO_OP, "NODE_NO_OP"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end stmt

    public static class connectStmt_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start connectStmt
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:86:1: connectStmt : K_CONNECT host SLASH port -> ^( NODE_CONNECT host port ) ;
    public final connectStmt_return connectStmt() throws RecognitionException {
        connectStmt_return retval = new connectStmt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_CONNECT11=null;
        Token SLASH13=null;
        host_return host12 = null;

        port_return port14 = null;


        CommonTree K_CONNECT11_tree=null;
        CommonTree SLASH13_tree=null;
        RewriteRuleTokenStream stream_SLASH=new RewriteRuleTokenStream(adaptor,"token SLASH");
        RewriteRuleTokenStream stream_K_CONNECT=new RewriteRuleTokenStream(adaptor,"token K_CONNECT");
        RewriteRuleSubtreeStream stream_port=new RewriteRuleSubtreeStream(adaptor,"rule port");
        RewriteRuleSubtreeStream stream_host=new RewriteRuleSubtreeStream(adaptor,"rule host");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:87:5: ( K_CONNECT host SLASH port -> ^( NODE_CONNECT host port ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:87:7: K_CONNECT host SLASH port
            {
            K_CONNECT11=(Token)input.LT(1);
            match(input,K_CONNECT,FOLLOW_K_CONNECT_in_connectStmt298); if (failed) return retval;
            if ( backtracking==0 ) stream_K_CONNECT.add(K_CONNECT11);

            pushFollow(FOLLOW_host_in_connectStmt300);
            host12=host();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_host.add(host12.getTree());
            SLASH13=(Token)input.LT(1);
            match(input,SLASH,FOLLOW_SLASH_in_connectStmt302); if (failed) return retval;
            if ( backtracking==0 ) stream_SLASH.add(SLASH13);

            pushFollow(FOLLOW_port_in_connectStmt304);
            port14=port();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_port.add(port14.getTree());

            // AST REWRITE
            // elements: host, port
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 87:33: -> ^( NODE_CONNECT host port )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:87:36: ^( NODE_CONNECT host port )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_CONNECT, "NODE_CONNECT"), root_1);

                adaptor.addChild(root_1, stream_host.next());
                adaptor.addChild(root_1, stream_port.next());

                adaptor.addChild(root_0, root_1);
                }

            }

            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end connectStmt

    public static class helpStmt_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start helpStmt
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:90:1: helpStmt : ( K_HELP -> ^( NODE_HELP ) | '?' -> ^( NODE_HELP ) );
    public final helpStmt_return helpStmt() throws RecognitionException {
        helpStmt_return retval = new helpStmt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_HELP15=null;
        Token char_literal16=null;

        CommonTree K_HELP15_tree=null;
        CommonTree char_literal16_tree=null;
        RewriteRuleTokenStream stream_K_HELP=new RewriteRuleTokenStream(adaptor,"token K_HELP");
        RewriteRuleTokenStream stream_43=new RewriteRuleTokenStream(adaptor,"token 43");

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:91:5: ( K_HELP -> ^( NODE_HELP ) | '?' -> ^( NODE_HELP ) )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==K_HELP) ) {
                alt3=1;
            }
            else if ( (LA3_0==43) ) {
                alt3=2;
            }
            else {
                if (backtracking>0) {failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("90:1: helpStmt : ( K_HELP -> ^( NODE_HELP ) | '?' -> ^( NODE_HELP ) );", 3, 0, input);

                throw nvae;
            }
            switch (alt3) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:91:7: K_HELP
                    {
                    K_HELP15=(Token)input.LT(1);
                    match(input,K_HELP,FOLLOW_K_HELP_in_helpStmt331); if (failed) return retval;
                    if ( backtracking==0 ) stream_K_HELP.add(K_HELP15);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    if ( backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 91:14: -> ^( NODE_HELP )
                    {
                        // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:91:17: ^( NODE_HELP )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    }

                    }
                    break;
                case 2 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:92:7: '?'
                    {
                    char_literal16=(Token)input.LT(1);
                    match(input,43,FOLLOW_43_in_helpStmt345); if (failed) return retval;
                    if ( backtracking==0 ) stream_43.add(char_literal16);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    if ( backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 92:14: -> ^( NODE_HELP )
                    {
                        // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:92:17: ^( NODE_HELP )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_HELP, "NODE_HELP"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end helpStmt

    public static class exitStmt_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start exitStmt
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:95:1: exitStmt : ( K_QUIT -> ^( NODE_EXIT ) | K_EXIT -> ^( NODE_EXIT ) );
    public final exitStmt_return exitStmt() throws RecognitionException {
        exitStmt_return retval = new exitStmt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_QUIT17=null;
        Token K_EXIT18=null;

        CommonTree K_QUIT17_tree=null;
        CommonTree K_EXIT18_tree=null;
        RewriteRuleTokenStream stream_K_EXIT=new RewriteRuleTokenStream(adaptor,"token K_EXIT");
        RewriteRuleTokenStream stream_K_QUIT=new RewriteRuleTokenStream(adaptor,"token K_QUIT");

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:96:5: ( K_QUIT -> ^( NODE_EXIT ) | K_EXIT -> ^( NODE_EXIT ) )
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==K_QUIT) ) {
                alt4=1;
            }
            else if ( (LA4_0==K_EXIT) ) {
                alt4=2;
            }
            else {
                if (backtracking>0) {failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("95:1: exitStmt : ( K_QUIT -> ^( NODE_EXIT ) | K_EXIT -> ^( NODE_EXIT ) );", 4, 0, input);

                throw nvae;
            }
            switch (alt4) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:96:7: K_QUIT
                    {
                    K_QUIT17=(Token)input.LT(1);
                    match(input,K_QUIT,FOLLOW_K_QUIT_in_exitStmt371); if (failed) return retval;
                    if ( backtracking==0 ) stream_K_QUIT.add(K_QUIT17);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    if ( backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 96:14: -> ^( NODE_EXIT )
                    {
                        // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:96:17: ^( NODE_EXIT )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_EXIT, "NODE_EXIT"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    }

                    }
                    break;
                case 2 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:97:7: K_EXIT
                    {
                    K_EXIT18=(Token)input.LT(1);
                    match(input,K_EXIT,FOLLOW_K_EXIT_in_exitStmt385); if (failed) return retval;
                    if ( backtracking==0 ) stream_K_EXIT.add(K_EXIT18);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    if ( backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 97:14: -> ^( NODE_EXIT )
                    {
                        // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:97:17: ^( NODE_EXIT )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_EXIT, "NODE_EXIT"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end exitStmt

    public static class getStmt_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start getStmt
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:100:1: getStmt : K_THRIFT K_GET columnFamilyExpr -> ^( NODE_THRIFT_GET columnFamilyExpr ) ;
    public final getStmt_return getStmt() throws RecognitionException {
        getStmt_return retval = new getStmt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_THRIFT19=null;
        Token K_GET20=null;
        columnFamilyExpr_return columnFamilyExpr21 = null;


        CommonTree K_THRIFT19_tree=null;
        CommonTree K_GET20_tree=null;
        RewriteRuleTokenStream stream_K_THRIFT=new RewriteRuleTokenStream(adaptor,"token K_THRIFT");
        RewriteRuleTokenStream stream_K_GET=new RewriteRuleTokenStream(adaptor,"token K_GET");
        RewriteRuleSubtreeStream stream_columnFamilyExpr=new RewriteRuleSubtreeStream(adaptor,"rule columnFamilyExpr");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:101:5: ( K_THRIFT K_GET columnFamilyExpr -> ^( NODE_THRIFT_GET columnFamilyExpr ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:101:7: K_THRIFT K_GET columnFamilyExpr
            {
            K_THRIFT19=(Token)input.LT(1);
            match(input,K_THRIFT,FOLLOW_K_THRIFT_in_getStmt408); if (failed) return retval;
            if ( backtracking==0 ) stream_K_THRIFT.add(K_THRIFT19);

            K_GET20=(Token)input.LT(1);
            match(input,K_GET,FOLLOW_K_GET_in_getStmt410); if (failed) return retval;
            if ( backtracking==0 ) stream_K_GET.add(K_GET20);

            pushFollow(FOLLOW_columnFamilyExpr_in_getStmt412);
            columnFamilyExpr21=columnFamilyExpr();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_columnFamilyExpr.add(columnFamilyExpr21.getTree());

            // AST REWRITE
            // elements: columnFamilyExpr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 101:39: -> ^( NODE_THRIFT_GET columnFamilyExpr )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:101:42: ^( NODE_THRIFT_GET columnFamilyExpr )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_THRIFT_GET, "NODE_THRIFT_GET"), root_1);

                adaptor.addChild(root_1, stream_columnFamilyExpr.next());

                adaptor.addChild(root_0, root_1);
                }

            }

            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end getStmt

    public static class setStmt_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start setStmt
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:104:1: setStmt : K_THRIFT K_SET columnFamilyExpr '=' value -> ^( NODE_THRIFT_SET columnFamilyExpr value ) ;
    public final setStmt_return setStmt() throws RecognitionException {
        setStmt_return retval = new setStmt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_THRIFT22=null;
        Token K_SET23=null;
        Token char_literal25=null;
        columnFamilyExpr_return columnFamilyExpr24 = null;

        value_return value26 = null;


        CommonTree K_THRIFT22_tree=null;
        CommonTree K_SET23_tree=null;
        CommonTree char_literal25_tree=null;
        RewriteRuleTokenStream stream_44=new RewriteRuleTokenStream(adaptor,"token 44");
        RewriteRuleTokenStream stream_K_THRIFT=new RewriteRuleTokenStream(adaptor,"token K_THRIFT");
        RewriteRuleTokenStream stream_K_SET=new RewriteRuleTokenStream(adaptor,"token K_SET");
        RewriteRuleSubtreeStream stream_columnFamilyExpr=new RewriteRuleSubtreeStream(adaptor,"rule columnFamilyExpr");
        RewriteRuleSubtreeStream stream_value=new RewriteRuleSubtreeStream(adaptor,"rule value");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:105:5: ( K_THRIFT K_SET columnFamilyExpr '=' value -> ^( NODE_THRIFT_SET columnFamilyExpr value ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:105:7: K_THRIFT K_SET columnFamilyExpr '=' value
            {
            K_THRIFT22=(Token)input.LT(1);
            match(input,K_THRIFT,FOLLOW_K_THRIFT_in_setStmt437); if (failed) return retval;
            if ( backtracking==0 ) stream_K_THRIFT.add(K_THRIFT22);

            K_SET23=(Token)input.LT(1);
            match(input,K_SET,FOLLOW_K_SET_in_setStmt439); if (failed) return retval;
            if ( backtracking==0 ) stream_K_SET.add(K_SET23);

            pushFollow(FOLLOW_columnFamilyExpr_in_setStmt441);
            columnFamilyExpr24=columnFamilyExpr();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_columnFamilyExpr.add(columnFamilyExpr24.getTree());
            char_literal25=(Token)input.LT(1);
            match(input,44,FOLLOW_44_in_setStmt443); if (failed) return retval;
            if ( backtracking==0 ) stream_44.add(char_literal25);

            pushFollow(FOLLOW_value_in_setStmt445);
            value26=value();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_value.add(value26.getTree());

            // AST REWRITE
            // elements: value, columnFamilyExpr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 105:49: -> ^( NODE_THRIFT_SET columnFamilyExpr value )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:105:52: ^( NODE_THRIFT_SET columnFamilyExpr value )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_THRIFT_SET, "NODE_THRIFT_SET"), root_1);

                adaptor.addChild(root_1, stream_columnFamilyExpr.next());
                adaptor.addChild(root_1, stream_value.next());

                adaptor.addChild(root_0, root_1);
                }

            }

            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end setStmt

    public static class showStmt_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start showStmt
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:108:1: showStmt : ( showClusterName | showVersion | showConfigFile | showTables );
    public final showStmt_return showStmt() throws RecognitionException {
        showStmt_return retval = new showStmt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        showClusterName_return showClusterName27 = null;

        showVersion_return showVersion28 = null;

        showConfigFile_return showConfigFile29 = null;

        showTables_return showTables30 = null;



        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:109:5: ( showClusterName | showVersion | showConfigFile | showTables )
            int alt5=4;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==K_SHOW) ) {
                switch ( input.LA(2) ) {
                case K_CONFIG:
                    {
                    alt5=3;
                    }
                    break;
                case K_VERSION:
                    {
                    alt5=2;
                    }
                    break;
                case K_TABLES:
                    {
                    alt5=4;
                    }
                    break;
                case K_CLUSTER:
                    {
                    alt5=1;
                    }
                    break;
                default:
                    if (backtracking>0) {failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("108:1: showStmt : ( showClusterName | showVersion | showConfigFile | showTables );", 5, 1, input);

                    throw nvae;
                }

            }
            else {
                if (backtracking>0) {failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("108:1: showStmt : ( showClusterName | showVersion | showConfigFile | showTables );", 5, 0, input);

                throw nvae;
            }
            switch (alt5) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:109:7: showClusterName
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_showClusterName_in_showStmt472);
                    showClusterName27=showClusterName();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, showClusterName27.getTree());

                    }
                    break;
                case 2 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:110:7: showVersion
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_showVersion_in_showStmt480);
                    showVersion28=showVersion();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, showVersion28.getTree());

                    }
                    break;
                case 3 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:111:7: showConfigFile
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_showConfigFile_in_showStmt488);
                    showConfigFile29=showConfigFile();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, showConfigFile29.getTree());

                    }
                    break;
                case 4 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:112:7: showTables
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_showTables_in_showStmt496);
                    showTables30=showTables();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, showTables30.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end showStmt

    public static class showClusterName_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start showClusterName
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:115:1: showClusterName : K_SHOW K_CLUSTER K_NAME -> ^( NODE_SHOW_CLUSTER_NAME ) ;
    public final showClusterName_return showClusterName() throws RecognitionException {
        showClusterName_return retval = new showClusterName_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_SHOW31=null;
        Token K_CLUSTER32=null;
        Token K_NAME33=null;

        CommonTree K_SHOW31_tree=null;
        CommonTree K_CLUSTER32_tree=null;
        CommonTree K_NAME33_tree=null;
        RewriteRuleTokenStream stream_K_SHOW=new RewriteRuleTokenStream(adaptor,"token K_SHOW");
        RewriteRuleTokenStream stream_K_NAME=new RewriteRuleTokenStream(adaptor,"token K_NAME");
        RewriteRuleTokenStream stream_K_CLUSTER=new RewriteRuleTokenStream(adaptor,"token K_CLUSTER");

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:116:5: ( K_SHOW K_CLUSTER K_NAME -> ^( NODE_SHOW_CLUSTER_NAME ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:116:7: K_SHOW K_CLUSTER K_NAME
            {
            K_SHOW31=(Token)input.LT(1);
            match(input,K_SHOW,FOLLOW_K_SHOW_in_showClusterName513); if (failed) return retval;
            if ( backtracking==0 ) stream_K_SHOW.add(K_SHOW31);

            K_CLUSTER32=(Token)input.LT(1);
            match(input,K_CLUSTER,FOLLOW_K_CLUSTER_in_showClusterName515); if (failed) return retval;
            if ( backtracking==0 ) stream_K_CLUSTER.add(K_CLUSTER32);

            K_NAME33=(Token)input.LT(1);
            match(input,K_NAME,FOLLOW_K_NAME_in_showClusterName517); if (failed) return retval;
            if ( backtracking==0 ) stream_K_NAME.add(K_NAME33);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 116:31: -> ^( NODE_SHOW_CLUSTER_NAME )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:116:34: ^( NODE_SHOW_CLUSTER_NAME )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_SHOW_CLUSTER_NAME, "NODE_SHOW_CLUSTER_NAME"), root_1);

                adaptor.addChild(root_0, root_1);
                }

            }

            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end showClusterName

    public static class showConfigFile_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start showConfigFile
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:119:1: showConfigFile : K_SHOW K_CONFIG K_FILE -> ^( NODE_SHOW_CONFIG_FILE ) ;
    public final showConfigFile_return showConfigFile() throws RecognitionException {
        showConfigFile_return retval = new showConfigFile_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_SHOW34=null;
        Token K_CONFIG35=null;
        Token K_FILE36=null;

        CommonTree K_SHOW34_tree=null;
        CommonTree K_CONFIG35_tree=null;
        CommonTree K_FILE36_tree=null;
        RewriteRuleTokenStream stream_K_SHOW=new RewriteRuleTokenStream(adaptor,"token K_SHOW");
        RewriteRuleTokenStream stream_K_FILE=new RewriteRuleTokenStream(adaptor,"token K_FILE");
        RewriteRuleTokenStream stream_K_CONFIG=new RewriteRuleTokenStream(adaptor,"token K_CONFIG");

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:120:5: ( K_SHOW K_CONFIG K_FILE -> ^( NODE_SHOW_CONFIG_FILE ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:120:7: K_SHOW K_CONFIG K_FILE
            {
            K_SHOW34=(Token)input.LT(1);
            match(input,K_SHOW,FOLLOW_K_SHOW_in_showConfigFile540); if (failed) return retval;
            if ( backtracking==0 ) stream_K_SHOW.add(K_SHOW34);

            K_CONFIG35=(Token)input.LT(1);
            match(input,K_CONFIG,FOLLOW_K_CONFIG_in_showConfigFile542); if (failed) return retval;
            if ( backtracking==0 ) stream_K_CONFIG.add(K_CONFIG35);

            K_FILE36=(Token)input.LT(1);
            match(input,K_FILE,FOLLOW_K_FILE_in_showConfigFile544); if (failed) return retval;
            if ( backtracking==0 ) stream_K_FILE.add(K_FILE36);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 120:30: -> ^( NODE_SHOW_CONFIG_FILE )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:120:33: ^( NODE_SHOW_CONFIG_FILE )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_SHOW_CONFIG_FILE, "NODE_SHOW_CONFIG_FILE"), root_1);

                adaptor.addChild(root_0, root_1);
                }

            }

            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end showConfigFile

    public static class showVersion_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start showVersion
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:123:1: showVersion : K_SHOW K_VERSION -> ^( NODE_SHOW_VERSION ) ;
    public final showVersion_return showVersion() throws RecognitionException {
        showVersion_return retval = new showVersion_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_SHOW37=null;
        Token K_VERSION38=null;

        CommonTree K_SHOW37_tree=null;
        CommonTree K_VERSION38_tree=null;
        RewriteRuleTokenStream stream_K_SHOW=new RewriteRuleTokenStream(adaptor,"token K_SHOW");
        RewriteRuleTokenStream stream_K_VERSION=new RewriteRuleTokenStream(adaptor,"token K_VERSION");

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:124:5: ( K_SHOW K_VERSION -> ^( NODE_SHOW_VERSION ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:124:7: K_SHOW K_VERSION
            {
            K_SHOW37=(Token)input.LT(1);
            match(input,K_SHOW,FOLLOW_K_SHOW_in_showVersion567); if (failed) return retval;
            if ( backtracking==0 ) stream_K_SHOW.add(K_SHOW37);

            K_VERSION38=(Token)input.LT(1);
            match(input,K_VERSION,FOLLOW_K_VERSION_in_showVersion569); if (failed) return retval;
            if ( backtracking==0 ) stream_K_VERSION.add(K_VERSION38);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 124:24: -> ^( NODE_SHOW_VERSION )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:124:27: ^( NODE_SHOW_VERSION )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_SHOW_VERSION, "NODE_SHOW_VERSION"), root_1);

                adaptor.addChild(root_0, root_1);
                }

            }

            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end showVersion

    public static class showTables_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start showTables
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:127:1: showTables : K_SHOW K_TABLES -> ^( NODE_SHOW_TABLES ) ;
    public final showTables_return showTables() throws RecognitionException {
        showTables_return retval = new showTables_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_SHOW39=null;
        Token K_TABLES40=null;

        CommonTree K_SHOW39_tree=null;
        CommonTree K_TABLES40_tree=null;
        RewriteRuleTokenStream stream_K_SHOW=new RewriteRuleTokenStream(adaptor,"token K_SHOW");
        RewriteRuleTokenStream stream_K_TABLES=new RewriteRuleTokenStream(adaptor,"token K_TABLES");

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:128:5: ( K_SHOW K_TABLES -> ^( NODE_SHOW_TABLES ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:128:7: K_SHOW K_TABLES
            {
            K_SHOW39=(Token)input.LT(1);
            match(input,K_SHOW,FOLLOW_K_SHOW_in_showTables592); if (failed) return retval;
            if ( backtracking==0 ) stream_K_SHOW.add(K_SHOW39);

            K_TABLES40=(Token)input.LT(1);
            match(input,K_TABLES,FOLLOW_K_TABLES_in_showTables594); if (failed) return retval;
            if ( backtracking==0 ) stream_K_TABLES.add(K_TABLES40);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 128:23: -> ^( NODE_SHOW_TABLES )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:128:26: ^( NODE_SHOW_TABLES )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_SHOW_TABLES, "NODE_SHOW_TABLES"), root_1);

                adaptor.addChild(root_0, root_1);
                }

            }

            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end showTables

    public static class describeTable_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start describeTable
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:131:1: describeTable : K_DESCRIBE K_TABLE table -> ^( NODE_DESCRIBE_TABLE table ) ;
    public final describeTable_return describeTable() throws RecognitionException {
        describeTable_return retval = new describeTable_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_DESCRIBE41=null;
        Token K_TABLE42=null;
        table_return table43 = null;


        CommonTree K_DESCRIBE41_tree=null;
        CommonTree K_TABLE42_tree=null;
        RewriteRuleTokenStream stream_K_DESCRIBE=new RewriteRuleTokenStream(adaptor,"token K_DESCRIBE");
        RewriteRuleTokenStream stream_K_TABLE=new RewriteRuleTokenStream(adaptor,"token K_TABLE");
        RewriteRuleSubtreeStream stream_table=new RewriteRuleSubtreeStream(adaptor,"rule table");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:132:5: ( K_DESCRIBE K_TABLE table -> ^( NODE_DESCRIBE_TABLE table ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:132:7: K_DESCRIBE K_TABLE table
            {
            K_DESCRIBE41=(Token)input.LT(1);
            match(input,K_DESCRIBE,FOLLOW_K_DESCRIBE_in_describeTable617); if (failed) return retval;
            if ( backtracking==0 ) stream_K_DESCRIBE.add(K_DESCRIBE41);

            K_TABLE42=(Token)input.LT(1);
            match(input,K_TABLE,FOLLOW_K_TABLE_in_describeTable619); if (failed) return retval;
            if ( backtracking==0 ) stream_K_TABLE.add(K_TABLE42);

            pushFollow(FOLLOW_table_in_describeTable621);
            table43=table();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_table.add(table43.getTree());

            // AST REWRITE
            // elements: table
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 132:32: -> ^( NODE_DESCRIBE_TABLE table )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:132:35: ^( NODE_DESCRIBE_TABLE table )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_DESCRIBE_TABLE, "NODE_DESCRIBE_TABLE"), root_1);

                adaptor.addChild(root_1, stream_table.next());

                adaptor.addChild(root_0, root_1);
                }

            }

            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end describeTable

    public static class columnFamilyExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start columnFamilyExpr
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:134:1: columnFamilyExpr : table DOT columnFamily '[' rowKey ']' ( '[' a+= columnOrSuperColumn ']' ( '[' a+= columnOrSuperColumn ']' )? )? -> ^( NODE_COLUMN_ACCESS table columnFamily rowKey ( ( $a)+ )? ) ;
    public final columnFamilyExpr_return columnFamilyExpr() throws RecognitionException {
        columnFamilyExpr_return retval = new columnFamilyExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DOT45=null;
        Token char_literal47=null;
        Token char_literal49=null;
        Token char_literal50=null;
        Token char_literal51=null;
        Token char_literal52=null;
        Token char_literal53=null;
        List list_a=null;
        table_return table44 = null;

        columnFamily_return columnFamily46 = null;

        rowKey_return rowKey48 = null;

        RuleReturnScope a = null;
        CommonTree DOT45_tree=null;
        CommonTree char_literal47_tree=null;
        CommonTree char_literal49_tree=null;
        CommonTree char_literal50_tree=null;
        CommonTree char_literal51_tree=null;
        CommonTree char_literal52_tree=null;
        CommonTree char_literal53_tree=null;
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleSubtreeStream stream_columnFamily=new RewriteRuleSubtreeStream(adaptor,"rule columnFamily");
        RewriteRuleSubtreeStream stream_rowKey=new RewriteRuleSubtreeStream(adaptor,"rule rowKey");
        RewriteRuleSubtreeStream stream_table=new RewriteRuleSubtreeStream(adaptor,"rule table");
        RewriteRuleSubtreeStream stream_columnOrSuperColumn=new RewriteRuleSubtreeStream(adaptor,"rule columnOrSuperColumn");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:135:5: ( table DOT columnFamily '[' rowKey ']' ( '[' a+= columnOrSuperColumn ']' ( '[' a+= columnOrSuperColumn ']' )? )? -> ^( NODE_COLUMN_ACCESS table columnFamily rowKey ( ( $a)+ )? ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:135:7: table DOT columnFamily '[' rowKey ']' ( '[' a+= columnOrSuperColumn ']' ( '[' a+= columnOrSuperColumn ']' )? )?
            {
            pushFollow(FOLLOW_table_in_columnFamilyExpr641);
            table44=table();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_table.add(table44.getTree());
            DOT45=(Token)input.LT(1);
            match(input,DOT,FOLLOW_DOT_in_columnFamilyExpr643); if (failed) return retval;
            if ( backtracking==0 ) stream_DOT.add(DOT45);

            pushFollow(FOLLOW_columnFamily_in_columnFamilyExpr645);
            columnFamily46=columnFamily();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_columnFamily.add(columnFamily46.getTree());
            char_literal47=(Token)input.LT(1);
            match(input,45,FOLLOW_45_in_columnFamilyExpr647); if (failed) return retval;
            if ( backtracking==0 ) stream_45.add(char_literal47);

            pushFollow(FOLLOW_rowKey_in_columnFamilyExpr649);
            rowKey48=rowKey();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_rowKey.add(rowKey48.getTree());
            char_literal49=(Token)input.LT(1);
            match(input,46,FOLLOW_46_in_columnFamilyExpr651); if (failed) return retval;
            if ( backtracking==0 ) stream_46.add(char_literal49);

            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:136:9: ( '[' a+= columnOrSuperColumn ']' ( '[' a+= columnOrSuperColumn ']' )? )?
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==45) ) {
                alt7=1;
            }
            switch (alt7) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:136:11: '[' a+= columnOrSuperColumn ']' ( '[' a+= columnOrSuperColumn ']' )?
                    {
                    char_literal50=(Token)input.LT(1);
                    match(input,45,FOLLOW_45_in_columnFamilyExpr664); if (failed) return retval;
                    if ( backtracking==0 ) stream_45.add(char_literal50);

                    pushFollow(FOLLOW_columnOrSuperColumn_in_columnFamilyExpr668);
                    a=columnOrSuperColumn();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) stream_columnOrSuperColumn.add(a.getTree());
                    if (list_a==null) list_a=new ArrayList();
                    list_a.add(a);

                    char_literal51=(Token)input.LT(1);
                    match(input,46,FOLLOW_46_in_columnFamilyExpr670); if (failed) return retval;
                    if ( backtracking==0 ) stream_46.add(char_literal51);

                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:137:13: ( '[' a+= columnOrSuperColumn ']' )?
                    int alt6=2;
                    int LA6_0 = input.LA(1);

                    if ( (LA6_0==45) ) {
                        alt6=1;
                    }
                    switch (alt6) {
                        case 1 :
                            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:137:14: '[' a+= columnOrSuperColumn ']'
                            {
                            char_literal52=(Token)input.LT(1);
                            match(input,45,FOLLOW_45_in_columnFamilyExpr686); if (failed) return retval;
                            if ( backtracking==0 ) stream_45.add(char_literal52);

                            pushFollow(FOLLOW_columnOrSuperColumn_in_columnFamilyExpr690);
                            a=columnOrSuperColumn();
                            _fsp--;
                            if (failed) return retval;
                            if ( backtracking==0 ) stream_columnOrSuperColumn.add(a.getTree());
                            if (list_a==null) list_a=new ArrayList();
                            list_a.add(a);

                            char_literal53=(Token)input.LT(1);
                            match(input,46,FOLLOW_46_in_columnFamilyExpr692); if (failed) return retval;
                            if ( backtracking==0 ) stream_46.add(char_literal53);


                            }
                            break;

                    }


                    }
                    break;

            }


            // AST REWRITE
            // elements: a, table, rowKey, columnFamily
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: a
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"token a",list_a);
            root_0 = (CommonTree)adaptor.nil();
            // 139:7: -> ^( NODE_COLUMN_ACCESS table columnFamily rowKey ( ( $a)+ )? )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:139:10: ^( NODE_COLUMN_ACCESS table columnFamily rowKey ( ( $a)+ )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_COLUMN_ACCESS, "NODE_COLUMN_ACCESS"), root_1);

                adaptor.addChild(root_1, stream_table.next());
                adaptor.addChild(root_1, stream_columnFamily.next());
                adaptor.addChild(root_1, stream_rowKey.next());
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:139:57: ( ( $a)+ )?
                if ( stream_a.hasNext() ) {
                    if ( !(stream_a.hasNext()) ) {
                        throw new RewriteEarlyExitException();
                    }
                    while ( stream_a.hasNext() ) {
                        adaptor.addChild(root_1, ((ParserRuleReturnScope)stream_a.next()).getTree());

                    }
                    stream_a.reset();

                }
                stream_a.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end columnFamilyExpr

    public static class table_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start table
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:142:1: table : Identifier ;
    public final table_return table() throws RecognitionException {
        table_return retval = new table_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier54=null;

        CommonTree Identifier54_tree=null;

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:142:6: ( Identifier )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:142:8: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier54=(Token)input.LT(1);
            match(input,Identifier,FOLLOW_Identifier_in_table743); if (failed) return retval;
            if ( backtracking==0 ) {
            Identifier54_tree = (CommonTree)adaptor.create(Identifier54);
            adaptor.addChild(root_0, Identifier54_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end table

    public static class columnFamily_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start columnFamily
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:144:1: columnFamily : Identifier ;
    public final columnFamily_return columnFamily() throws RecognitionException {
        columnFamily_return retval = new columnFamily_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier55=null;

        CommonTree Identifier55_tree=null;

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:144:13: ( Identifier )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:144:15: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier55=(Token)input.LT(1);
            match(input,Identifier,FOLLOW_Identifier_in_columnFamily750); if (failed) return retval;
            if ( backtracking==0 ) {
            Identifier55_tree = (CommonTree)adaptor.create(Identifier55);
            adaptor.addChild(root_0, Identifier55_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end columnFamily

    public static class rowKey_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start rowKey
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:146:1: rowKey : StringLiteral ;
    public final rowKey_return rowKey() throws RecognitionException {
        rowKey_return retval = new rowKey_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token StringLiteral56=null;

        CommonTree StringLiteral56_tree=null;

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:146:7: ( StringLiteral )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:146:11: StringLiteral
            {
            root_0 = (CommonTree)adaptor.nil();

            StringLiteral56=(Token)input.LT(1);
            match(input,StringLiteral,FOLLOW_StringLiteral_in_rowKey759); if (failed) return retval;
            if ( backtracking==0 ) {
            StringLiteral56_tree = (CommonTree)adaptor.create(StringLiteral56);
            adaptor.addChild(root_0, StringLiteral56_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end rowKey

    public static class value_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start value
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:148:1: value : StringLiteral ;
    public final value_return value() throws RecognitionException {
        value_return retval = new value_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token StringLiteral57=null;

        CommonTree StringLiteral57_tree=null;

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:148:6: ( StringLiteral )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:148:8: StringLiteral
            {
            root_0 = (CommonTree)adaptor.nil();

            StringLiteral57=(Token)input.LT(1);
            match(input,StringLiteral,FOLLOW_StringLiteral_in_value766); if (failed) return retval;
            if ( backtracking==0 ) {
            StringLiteral57_tree = (CommonTree)adaptor.create(StringLiteral57);
            adaptor.addChild(root_0, StringLiteral57_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end value

    public static class columnOrSuperColumn_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start columnOrSuperColumn
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:150:1: columnOrSuperColumn : StringLiteral ;
    public final columnOrSuperColumn_return columnOrSuperColumn() throws RecognitionException {
        columnOrSuperColumn_return retval = new columnOrSuperColumn_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token StringLiteral58=null;

        CommonTree StringLiteral58_tree=null;

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:150:20: ( StringLiteral )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:150:22: StringLiteral
            {
            root_0 = (CommonTree)adaptor.nil();

            StringLiteral58=(Token)input.LT(1);
            match(input,StringLiteral,FOLLOW_StringLiteral_in_columnOrSuperColumn773); if (failed) return retval;
            if ( backtracking==0 ) {
            StringLiteral58_tree = (CommonTree)adaptor.create(StringLiteral58);
            adaptor.addChild(root_0, StringLiteral58_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end columnOrSuperColumn

    public static class host_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start host
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:152:1: host : id+= Identifier (id+= DOT id+= Identifier )* -> ^( NODE_ID_LIST ( $id)+ ) ;
    public final host_return host() throws RecognitionException {
        host_return retval = new host_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token id=null;
        List list_id=null;

        CommonTree id_tree=null;
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:152:5: (id+= Identifier (id+= DOT id+= Identifier )* -> ^( NODE_ID_LIST ( $id)+ ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:152:7: id+= Identifier (id+= DOT id+= Identifier )*
            {
            id=(Token)input.LT(1);
            match(input,Identifier,FOLLOW_Identifier_in_host782); if (failed) return retval;
            if ( backtracking==0 ) stream_Identifier.add(id);

            if (list_id==null) list_id=new ArrayList();
            list_id.add(id);

            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:152:22: (id+= DOT id+= Identifier )*
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( (LA8_0==DOT) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:152:23: id+= DOT id+= Identifier
            	    {
            	    id=(Token)input.LT(1);
            	    match(input,DOT,FOLLOW_DOT_in_host787); if (failed) return retval;
            	    if ( backtracking==0 ) stream_DOT.add(id);

            	    if (list_id==null) list_id=new ArrayList();
            	    list_id.add(id);

            	    id=(Token)input.LT(1);
            	    match(input,Identifier,FOLLOW_Identifier_in_host791); if (failed) return retval;
            	    if ( backtracking==0 ) stream_Identifier.add(id);

            	    if (list_id==null) list_id=new ArrayList();
            	    list_id.add(id);


            	    }
            	    break;

            	default :
            	    break loop8;
                }
            } while (true);


            // AST REWRITE
            // elements: id
            // token labels: 
            // rule labels: retval
            // token list labels: id
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleTokenStream stream_id=new RewriteRuleTokenStream(adaptor,"token id", list_id);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 152:48: -> ^( NODE_ID_LIST ( $id)+ )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:152:51: ^( NODE_ID_LIST ( $id)+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(NODE_ID_LIST, "NODE_ID_LIST"), root_1);

                if ( !(stream_id.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_id.hasNext() ) {
                    adaptor.addChild(root_1, stream_id.next());

                }
                stream_id.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end host

    public static class port_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start port
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:154:1: port : IntegerLiteral ;
    public final port_return port() throws RecognitionException {
        port_return retval = new port_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token IntegerLiteral59=null;

        CommonTree IntegerLiteral59_tree=null;

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:154:5: ( IntegerLiteral )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g:154:7: IntegerLiteral
            {
            root_0 = (CommonTree)adaptor.nil();

            IntegerLiteral59=(Token)input.LT(1);
            match(input,IntegerLiteral,FOLLOW_IntegerLiteral_in_port810); if (failed) return retval;
            if ( backtracking==0 ) {
            IntegerLiteral59_tree = (CommonTree)adaptor.create(IntegerLiteral59);
            adaptor.addChild(root_0, IntegerLiteral59_tree);
            }

            }

            retval.stop = input.LT(-1);

            if ( backtracking==0 ) {
                retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end port


 

    public static final BitSet FOLLOW_stmt_in_root200 = new BitSet(new long[]{0x0000000000020000L});
    public static final BitSet FOLLOW_SEMICOLON_in_root202 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_root205 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_connectStmt_in_stmt221 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_exitStmt_in_stmt229 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_describeTable_in_stmt237 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_getStmt_in_stmt245 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_helpStmt_in_stmt253 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_setStmt_in_stmt261 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_showStmt_in_stmt269 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CONNECT_in_connectStmt298 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_host_in_connectStmt300 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_SLASH_in_connectStmt302 = new BitSet(new long[]{0x0000004000000000L});
    public static final BitSet FOLLOW_port_in_connectStmt304 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_HELP_in_helpStmt331 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_43_in_helpStmt345 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_QUIT_in_exitStmt371 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_EXIT_in_exitStmt385 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_THRIFT_in_getStmt408 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_K_GET_in_getStmt410 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_columnFamilyExpr_in_getStmt412 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_THRIFT_in_setStmt437 = new BitSet(new long[]{0x0000000002000000L});
    public static final BitSet FOLLOW_K_SET_in_setStmt439 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_columnFamilyExpr_in_setStmt441 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_44_in_setStmt443 = new BitSet(new long[]{0x0000002000000000L});
    public static final BitSet FOLLOW_value_in_setStmt445 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_showClusterName_in_showStmt472 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_showVersion_in_showStmt480 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_showConfigFile_in_showStmt488 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_showTables_in_showStmt496 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_SHOW_in_showClusterName513 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_K_CLUSTER_in_showClusterName515 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_K_NAME_in_showClusterName517 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_SHOW_in_showConfigFile540 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_K_CONFIG_in_showConfigFile542 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_K_FILE_in_showConfigFile544 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_SHOW_in_showVersion567 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_K_VERSION_in_showVersion569 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_SHOW_in_showTables592 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_K_TABLES_in_showTables594 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DESCRIBE_in_describeTable617 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_K_TABLE_in_describeTable619 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_table_in_describeTable621 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_table_in_columnFamilyExpr641 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_DOT_in_columnFamilyExpr643 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_columnFamily_in_columnFamilyExpr645 = new BitSet(new long[]{0x0000200000000000L});
    public static final BitSet FOLLOW_45_in_columnFamilyExpr647 = new BitSet(new long[]{0x0000002000000000L});
    public static final BitSet FOLLOW_rowKey_in_columnFamilyExpr649 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_46_in_columnFamilyExpr651 = new BitSet(new long[]{0x0000200000000002L});
    public static final BitSet FOLLOW_45_in_columnFamilyExpr664 = new BitSet(new long[]{0x0000002000000000L});
    public static final BitSet FOLLOW_columnOrSuperColumn_in_columnFamilyExpr668 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_46_in_columnFamilyExpr670 = new BitSet(new long[]{0x0000200000000002L});
    public static final BitSet FOLLOW_45_in_columnFamilyExpr686 = new BitSet(new long[]{0x0000002000000000L});
    public static final BitSet FOLLOW_columnOrSuperColumn_in_columnFamilyExpr690 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_46_in_columnFamilyExpr692 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_table743 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_columnFamily750 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_rowKey759 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_value766 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_StringLiteral_in_columnOrSuperColumn773 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_host782 = new BitSet(new long[]{0x0000000800000002L});
    public static final BitSet FOLLOW_DOT_in_host787 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_Identifier_in_host791 = new BitSet(new long[]{0x0000000800000002L});
    public static final BitSet FOLLOW_IntegerLiteral_in_port810 = new BitSet(new long[]{0x0000000000000002L});

}