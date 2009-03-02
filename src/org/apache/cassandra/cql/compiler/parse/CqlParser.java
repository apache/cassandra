// $ANTLR 3.0.1 /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g 2008-10-24 16:15:03

            package org.apache.cassandra.cql.compiler.parse;
        

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
public class CqlParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "A_DELETE", "A_GET", "A_SELECT", "A_SET", "A_EXPLAIN_PLAN", "A_COLUMN_ACCESS", "A_COLUMN_MAP_ENTRY", "A_COLUMN_MAP_VALUE", "A_FROM", "A_KEY_IN_LIST", "A_KEY_EXACT_MATCH", "A_LIMIT", "A_OFFSET", "A_ORDER_BY", "A_SUPERCOLUMN_MAP_ENTRY", "A_SUPERCOLUMN_MAP_VALUE", "A_SELECT_CLAUSE", "A_WHERE", "SEMICOLON", "K_EXPLAIN", "K_PLAN", "K_GET", "K_SET", "K_SELECT", "K_FROM", "K_WHERE", "K_IN", "K_LIMIT", "IntegerLiteral", "K_DELETE", "Identifier", "LEFT_BRACE", "COMMA", "RIGHT_BRACE", "ASSOC", "StringLiteral", "K_BY", "K_OFFSET", "K_ORDER", "Letter", "Digit", "WS", "COMMENT", "'='", "'('", "')'", "'['", "']'", "'.'", "'?'"
    };
    public static final int K_EXPLAIN=23;
    public static final int K_OFFSET=41;
    public static final int K_GET=25;
    public static final int K_DELETE=33;
    public static final int A_KEY_EXACT_MATCH=14;
    public static final int K_BY=40;
    public static final int A_SELECT=6;
    public static final int A_SUPERCOLUMN_MAP_VALUE=19;
    public static final int EOF=-1;
    public static final int K_SELECT=27;
    public static final int K_LIMIT=31;
    public static final int Identifier=34;
    public static final int K_SET=26;
    public static final int K_WHERE=29;
    public static final int COMMA=36;
    public static final int A_EXPLAIN_PLAN=8;
    public static final int A_LIMIT=15;
    public static final int COMMENT=46;
    public static final int K_ORDER=42;
    public static final int RIGHT_BRACE=37;
    public static final int A_COLUMN_MAP_VALUE=11;
    public static final int SEMICOLON=22;
    public static final int K_IN=30;
    public static final int Digit=44;
    public static final int A_OFFSET=16;
    public static final int A_WHERE=21;
    public static final int K_PLAN=24;
    public static final int A_ORDER_BY=17;
    public static final int K_FROM=28;
    public static final int StringLiteral=39;
    public static final int A_COLUMN_MAP_ENTRY=10;
    public static final int WS=45;
    public static final int A_FROM=12;
    public static final int A_GET=5;
    public static final int LEFT_BRACE=35;
    public static final int A_KEY_IN_LIST=13;
    public static final int A_COLUMN_ACCESS=9;
    public static final int A_SUPERCOLUMN_MAP_ENTRY=18;
    public static final int IntegerLiteral=32;
    public static final int ASSOC=38;
    public static final int A_SELECT_CLAUSE=20;
    public static final int Letter=43;
    public static final int A_DELETE=4;
    public static final int A_SET=7;

        public CqlParser(TokenStream input) {
            super(input);
            ruleMemo = new HashMap[53+1];
         }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return tokenNames; }
    public String getGrammarFileName() { return "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g"; }


    public static class root_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start root
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:112:1: root : ( stmt ( SEMICOLON )? EOF -> stmt | K_EXPLAIN K_PLAN stmt ( SEMICOLON )? EOF -> ^( A_EXPLAIN_PLAN stmt ) );
    public final root_return root() throws RecognitionException {
        root_return retval = new root_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token SEMICOLON2=null;
        Token EOF3=null;
        Token K_EXPLAIN4=null;
        Token K_PLAN5=null;
        Token SEMICOLON7=null;
        Token EOF8=null;
        stmt_return stmt1 = null;

        stmt_return stmt6 = null;


        CommonTree SEMICOLON2_tree=null;
        CommonTree EOF3_tree=null;
        CommonTree K_EXPLAIN4_tree=null;
        CommonTree K_PLAN5_tree=null;
        CommonTree SEMICOLON7_tree=null;
        CommonTree EOF8_tree=null;
        RewriteRuleTokenStream stream_K_EXPLAIN=new RewriteRuleTokenStream(adaptor,"token K_EXPLAIN");
        RewriteRuleTokenStream stream_SEMICOLON=new RewriteRuleTokenStream(adaptor,"token SEMICOLON");
        RewriteRuleTokenStream stream_EOF=new RewriteRuleTokenStream(adaptor,"token EOF");
        RewriteRuleTokenStream stream_K_PLAN=new RewriteRuleTokenStream(adaptor,"token K_PLAN");
        RewriteRuleSubtreeStream stream_stmt=new RewriteRuleSubtreeStream(adaptor,"rule stmt");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:113:5: ( stmt ( SEMICOLON )? EOF -> stmt | K_EXPLAIN K_PLAN stmt ( SEMICOLON )? EOF -> ^( A_EXPLAIN_PLAN stmt ) )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( ((LA3_0>=K_GET && LA3_0<=K_SELECT)||LA3_0==K_DELETE) ) {
                alt3=1;
            }
            else if ( (LA3_0==K_EXPLAIN) ) {
                alt3=2;
            }
            else {
                if (backtracking>0) {failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("112:1: root : ( stmt ( SEMICOLON )? EOF -> stmt | K_EXPLAIN K_PLAN stmt ( SEMICOLON )? EOF -> ^( A_EXPLAIN_PLAN stmt ) );", 3, 0, input);

                throw nvae;
            }
            switch (alt3) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:113:7: stmt ( SEMICOLON )? EOF
                    {
                    pushFollow(FOLLOW_stmt_in_root266);
                    stmt1=stmt();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) stream_stmt.add(stmt1.getTree());
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:113:12: ( SEMICOLON )?
                    int alt1=2;
                    int LA1_0 = input.LA(1);

                    if ( (LA1_0==SEMICOLON) ) {
                        alt1=1;
                    }
                    switch (alt1) {
                        case 1 :
                            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:0:0: SEMICOLON
                            {
                            SEMICOLON2=(Token)input.LT(1);
                            match(input,SEMICOLON,FOLLOW_SEMICOLON_in_root268); if (failed) return retval;
                            if ( backtracking==0 ) stream_SEMICOLON.add(SEMICOLON2);


                            }
                            break;

                    }

                    EOF3=(Token)input.LT(1);
                    match(input,EOF,FOLLOW_EOF_in_root271); if (failed) return retval;
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
                    // 113:27: -> stmt
                    {
                        adaptor.addChild(root_0, stream_stmt.next());

                    }

                    }

                    }
                    break;
                case 2 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:114:7: K_EXPLAIN K_PLAN stmt ( SEMICOLON )? EOF
                    {
                    K_EXPLAIN4=(Token)input.LT(1);
                    match(input,K_EXPLAIN,FOLLOW_K_EXPLAIN_in_root283); if (failed) return retval;
                    if ( backtracking==0 ) stream_K_EXPLAIN.add(K_EXPLAIN4);

                    K_PLAN5=(Token)input.LT(1);
                    match(input,K_PLAN,FOLLOW_K_PLAN_in_root285); if (failed) return retval;
                    if ( backtracking==0 ) stream_K_PLAN.add(K_PLAN5);

                    pushFollow(FOLLOW_stmt_in_root287);
                    stmt6=stmt();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) stream_stmt.add(stmt6.getTree());
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:114:29: ( SEMICOLON )?
                    int alt2=2;
                    int LA2_0 = input.LA(1);

                    if ( (LA2_0==SEMICOLON) ) {
                        alt2=1;
                    }
                    switch (alt2) {
                        case 1 :
                            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:0:0: SEMICOLON
                            {
                            SEMICOLON7=(Token)input.LT(1);
                            match(input,SEMICOLON,FOLLOW_SEMICOLON_in_root289); if (failed) return retval;
                            if ( backtracking==0 ) stream_SEMICOLON.add(SEMICOLON7);


                            }
                            break;

                    }

                    EOF8=(Token)input.LT(1);
                    match(input,EOF,FOLLOW_EOF_in_root292); if (failed) return retval;
                    if ( backtracking==0 ) stream_EOF.add(EOF8);


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
                    // 114:44: -> ^( A_EXPLAIN_PLAN stmt )
                    {
                        // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:114:47: ^( A_EXPLAIN_PLAN stmt )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_EXPLAIN_PLAN, "A_EXPLAIN_PLAN"), root_1);

                        adaptor.addChild(root_1, stream_stmt.next());

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
    // $ANTLR end root

    public static class stmt_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start stmt
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:117:1: stmt : ( deleteStmt | getStmt | selectStmt | setStmt );
    public final stmt_return stmt() throws RecognitionException {
        stmt_return retval = new stmt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        deleteStmt_return deleteStmt9 = null;

        getStmt_return getStmt10 = null;

        selectStmt_return selectStmt11 = null;

        setStmt_return setStmt12 = null;



        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:118:5: ( deleteStmt | getStmt | selectStmt | setStmt )
            int alt4=4;
            switch ( input.LA(1) ) {
            case K_DELETE:
                {
                alt4=1;
                }
                break;
            case K_GET:
                {
                alt4=2;
                }
                break;
            case K_SELECT:
                {
                alt4=3;
                }
                break;
            case K_SET:
                {
                alt4=4;
                }
                break;
            default:
                if (backtracking>0) {failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("117:1: stmt : ( deleteStmt | getStmt | selectStmt | setStmt );", 4, 0, input);

                throw nvae;
            }

            switch (alt4) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:118:7: deleteStmt
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_deleteStmt_in_stmt317);
                    deleteStmt9=deleteStmt();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, deleteStmt9.getTree());

                    }
                    break;
                case 2 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:119:7: getStmt
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_getStmt_in_stmt325);
                    getStmt10=getStmt();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, getStmt10.getTree());

                    }
                    break;
                case 3 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:120:7: selectStmt
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_selectStmt_in_stmt333);
                    selectStmt11=selectStmt();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, selectStmt11.getTree());

                    }
                    break;
                case 4 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:121:7: setStmt
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_setStmt_in_stmt341);
                    setStmt12=setStmt();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, setStmt12.getTree());

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

    public static class getStmt_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start getStmt
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:124:1: getStmt : K_GET columnSpec -> ^( A_GET columnSpec ) ;
    public final getStmt_return getStmt() throws RecognitionException {
        getStmt_return retval = new getStmt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_GET13=null;
        columnSpec_return columnSpec14 = null;


        CommonTree K_GET13_tree=null;
        RewriteRuleTokenStream stream_K_GET=new RewriteRuleTokenStream(adaptor,"token K_GET");
        RewriteRuleSubtreeStream stream_columnSpec=new RewriteRuleSubtreeStream(adaptor,"rule columnSpec");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:125:5: ( K_GET columnSpec -> ^( A_GET columnSpec ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:125:7: K_GET columnSpec
            {
            K_GET13=(Token)input.LT(1);
            match(input,K_GET,FOLLOW_K_GET_in_getStmt358); if (failed) return retval;
            if ( backtracking==0 ) stream_K_GET.add(K_GET13);

            pushFollow(FOLLOW_columnSpec_in_getStmt360);
            columnSpec14=columnSpec();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_columnSpec.add(columnSpec14.getTree());

            // AST REWRITE
            // elements: columnSpec
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 125:25: -> ^( A_GET columnSpec )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:125:28: ^( A_GET columnSpec )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_GET, "A_GET"), root_1);

                adaptor.addChild(root_1, stream_columnSpec.next());

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
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:128:1: setStmt : K_SET columnSpec '=' valueExpr -> ^( A_SET columnSpec valueExpr ) ;
    public final setStmt_return setStmt() throws RecognitionException {
        setStmt_return retval = new setStmt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_SET15=null;
        Token char_literal17=null;
        columnSpec_return columnSpec16 = null;

        valueExpr_return valueExpr18 = null;


        CommonTree K_SET15_tree=null;
        CommonTree char_literal17_tree=null;
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_K_SET=new RewriteRuleTokenStream(adaptor,"token K_SET");
        RewriteRuleSubtreeStream stream_valueExpr=new RewriteRuleSubtreeStream(adaptor,"rule valueExpr");
        RewriteRuleSubtreeStream stream_columnSpec=new RewriteRuleSubtreeStream(adaptor,"rule columnSpec");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:129:5: ( K_SET columnSpec '=' valueExpr -> ^( A_SET columnSpec valueExpr ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:129:7: K_SET columnSpec '=' valueExpr
            {
            K_SET15=(Token)input.LT(1);
            match(input,K_SET,FOLLOW_K_SET_in_setStmt386); if (failed) return retval;
            if ( backtracking==0 ) stream_K_SET.add(K_SET15);

            pushFollow(FOLLOW_columnSpec_in_setStmt388);
            columnSpec16=columnSpec();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_columnSpec.add(columnSpec16.getTree());
            char_literal17=(Token)input.LT(1);
            match(input,47,FOLLOW_47_in_setStmt390); if (failed) return retval;
            if ( backtracking==0 ) stream_47.add(char_literal17);

            pushFollow(FOLLOW_valueExpr_in_setStmt392);
            valueExpr18=valueExpr();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_valueExpr.add(valueExpr18.getTree());

            // AST REWRITE
            // elements: valueExpr, columnSpec
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 129:38: -> ^( A_SET columnSpec valueExpr )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:129:41: ^( A_SET columnSpec valueExpr )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_SET, "A_SET"), root_1);

                adaptor.addChild(root_1, stream_columnSpec.next());
                adaptor.addChild(root_1, stream_valueExpr.next());

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

    public static class selectStmt_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start selectStmt
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:132:1: selectStmt : selectClause ( fromClause )? ( whereClause )? ( limitClause )? -> ^( A_SELECT selectClause ( fromClause )? ( whereClause )? ( limitClause )? ) ;
    public final selectStmt_return selectStmt() throws RecognitionException {
        selectStmt_return retval = new selectStmt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        selectClause_return selectClause19 = null;

        fromClause_return fromClause20 = null;

        whereClause_return whereClause21 = null;

        limitClause_return limitClause22 = null;


        RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");
        RewriteRuleSubtreeStream stream_limitClause=new RewriteRuleSubtreeStream(adaptor,"rule limitClause");
        RewriteRuleSubtreeStream stream_selectClause=new RewriteRuleSubtreeStream(adaptor,"rule selectClause");
        RewriteRuleSubtreeStream stream_fromClause=new RewriteRuleSubtreeStream(adaptor,"rule fromClause");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:133:5: ( selectClause ( fromClause )? ( whereClause )? ( limitClause )? -> ^( A_SELECT selectClause ( fromClause )? ( whereClause )? ( limitClause )? ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:133:7: selectClause ( fromClause )? ( whereClause )? ( limitClause )?
            {
            pushFollow(FOLLOW_selectClause_in_selectStmt419);
            selectClause19=selectClause();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_selectClause.add(selectClause19.getTree());
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:134:9: ( fromClause )?
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==K_FROM) ) {
                alt5=1;
            }
            switch (alt5) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:0:0: fromClause
                    {
                    pushFollow(FOLLOW_fromClause_in_selectStmt429);
                    fromClause20=fromClause();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) stream_fromClause.add(fromClause20.getTree());

                    }
                    break;

            }

            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:135:9: ( whereClause )?
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0==K_WHERE) ) {
                alt6=1;
            }
            switch (alt6) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:0:0: whereClause
                    {
                    pushFollow(FOLLOW_whereClause_in_selectStmt440);
                    whereClause21=whereClause();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) stream_whereClause.add(whereClause21.getTree());

                    }
                    break;

            }

            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:136:9: ( limitClause )?
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==K_LIMIT) ) {
                alt7=1;
            }
            switch (alt7) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:0:0: limitClause
                    {
                    pushFollow(FOLLOW_limitClause_in_selectStmt451);
                    limitClause22=limitClause();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) stream_limitClause.add(limitClause22.getTree());

                    }
                    break;

            }


            // AST REWRITE
            // elements: selectClause, fromClause, limitClause, whereClause
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 136:22: -> ^( A_SELECT selectClause ( fromClause )? ( whereClause )? ( limitClause )? )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:136:25: ^( A_SELECT selectClause ( fromClause )? ( whereClause )? ( limitClause )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_SELECT, "A_SELECT"), root_1);

                adaptor.addChild(root_1, stream_selectClause.next());
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:136:49: ( fromClause )?
                if ( stream_fromClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_fromClause.next());

                }
                stream_fromClause.reset();
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:136:61: ( whereClause )?
                if ( stream_whereClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_whereClause.next());

                }
                stream_whereClause.reset();
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:136:74: ( limitClause )?
                if ( stream_limitClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_limitClause.next());

                }
                stream_limitClause.reset();

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
    // $ANTLR end selectStmt

    public static class selectClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start selectClause
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:139:1: selectClause : K_SELECT selectList -> ^( A_SELECT_CLAUSE selectList ) ;
    public final selectClause_return selectClause() throws RecognitionException {
        selectClause_return retval = new selectClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_SELECT23=null;
        selectList_return selectList24 = null;


        CommonTree K_SELECT23_tree=null;
        RewriteRuleTokenStream stream_K_SELECT=new RewriteRuleTokenStream(adaptor,"token K_SELECT");
        RewriteRuleSubtreeStream stream_selectList=new RewriteRuleSubtreeStream(adaptor,"rule selectList");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:140:2: ( K_SELECT selectList -> ^( A_SELECT_CLAUSE selectList ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:140:4: K_SELECT selectList
            {
            K_SELECT23=(Token)input.LT(1);
            match(input,K_SELECT,FOLLOW_K_SELECT_in_selectClause483); if (failed) return retval;
            if ( backtracking==0 ) stream_K_SELECT.add(K_SELECT23);

            pushFollow(FOLLOW_selectList_in_selectClause485);
            selectList24=selectList();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_selectList.add(selectList24.getTree());

            // AST REWRITE
            // elements: selectList
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 140:24: -> ^( A_SELECT_CLAUSE selectList )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:140:27: ^( A_SELECT_CLAUSE selectList )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_SELECT_CLAUSE, "A_SELECT_CLAUSE"), root_1);

                adaptor.addChild(root_1, stream_selectList.next());

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
    // $ANTLR end selectClause

    public static class selectList_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start selectList
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:143:1: selectList : selectListItem ( ',' selectListItem )* ;
    public final selectList_return selectList() throws RecognitionException {
        selectList_return retval = new selectList_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal26=null;
        selectListItem_return selectListItem25 = null;

        selectListItem_return selectListItem27 = null;


        CommonTree char_literal26_tree=null;

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:144:2: ( selectListItem ( ',' selectListItem )* )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:144:4: selectListItem ( ',' selectListItem )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_selectListItem_in_selectList504);
            selectListItem25=selectListItem();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) adaptor.addChild(root_0, selectListItem25.getTree());
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:144:19: ( ',' selectListItem )*
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( (LA8_0==COMMA) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:144:20: ',' selectListItem
            	    {
            	    char_literal26=(Token)input.LT(1);
            	    match(input,COMMA,FOLLOW_COMMA_in_selectList507); if (failed) return retval;
            	    if ( backtracking==0 ) {
            	    char_literal26_tree = (CommonTree)adaptor.create(char_literal26);
            	    adaptor.addChild(root_0, char_literal26_tree);
            	    }
            	    pushFollow(FOLLOW_selectListItem_in_selectList509);
            	    selectListItem27=selectListItem();
            	    _fsp--;
            	    if (failed) return retval;
            	    if ( backtracking==0 ) adaptor.addChild(root_0, selectListItem27.getTree());

            	    }
            	    break;

            	default :
            	    break loop8;
                }
            } while (true);


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
    // $ANTLR end selectList

    public static class selectListItem_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start selectListItem
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:147:1: selectListItem : ( columnExpression | '(' selectStmt ')' -> ^( A_SELECT selectStmt ) );
    public final selectListItem_return selectListItem() throws RecognitionException {
        selectListItem_return retval = new selectListItem_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal29=null;
        Token char_literal31=null;
        columnExpression_return columnExpression28 = null;

        selectStmt_return selectStmt30 = null;


        CommonTree char_literal29_tree=null;
        CommonTree char_literal31_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleSubtreeStream stream_selectStmt=new RewriteRuleSubtreeStream(adaptor,"rule selectStmt");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:148:2: ( columnExpression | '(' selectStmt ')' -> ^( A_SELECT selectStmt ) )
            int alt9=2;
            int LA9_0 = input.LA(1);

            if ( (LA9_0==Identifier) ) {
                alt9=1;
            }
            else if ( (LA9_0==48) ) {
                alt9=2;
            }
            else {
                if (backtracking>0) {failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("147:1: selectListItem : ( columnExpression | '(' selectStmt ')' -> ^( A_SELECT selectStmt ) );", 9, 0, input);

                throw nvae;
            }
            switch (alt9) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:148:4: columnExpression
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_columnExpression_in_selectListItem522);
                    columnExpression28=columnExpression();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, columnExpression28.getTree());

                    }
                    break;
                case 2 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:149:4: '(' selectStmt ')'
                    {
                    char_literal29=(Token)input.LT(1);
                    match(input,48,FOLLOW_48_in_selectListItem527); if (failed) return retval;
                    if ( backtracking==0 ) stream_48.add(char_literal29);

                    pushFollow(FOLLOW_selectStmt_in_selectListItem529);
                    selectStmt30=selectStmt();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) stream_selectStmt.add(selectStmt30.getTree());
                    char_literal31=(Token)input.LT(1);
                    match(input,49,FOLLOW_49_in_selectListItem531); if (failed) return retval;
                    if ( backtracking==0 ) stream_49.add(char_literal31);


                    // AST REWRITE
                    // elements: selectStmt
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    if ( backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 149:23: -> ^( A_SELECT selectStmt )
                    {
                        // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:149:26: ^( A_SELECT selectStmt )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_SELECT, "A_SELECT"), root_1);

                        adaptor.addChild(root_1, stream_selectStmt.next());

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
    // $ANTLR end selectListItem

    public static class columnExpression_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start columnExpression
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:152:1: columnExpression : columnOrSuperColumnName columnExpressionRest ;
    public final columnExpression_return columnExpression() throws RecognitionException {
        columnExpression_return retval = new columnExpression_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        columnOrSuperColumnName_return columnOrSuperColumnName32 = null;

        columnExpressionRest_return columnExpressionRest33 = null;



        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:153:2: ( columnOrSuperColumnName columnExpressionRest )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:153:4: columnOrSuperColumnName columnExpressionRest
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_columnOrSuperColumnName_in_columnExpression551);
            columnOrSuperColumnName32=columnOrSuperColumnName();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) adaptor.addChild(root_0, columnOrSuperColumnName32.getTree());
            pushFollow(FOLLOW_columnExpressionRest_in_columnExpression553);
            columnExpressionRest33=columnExpressionRest();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) adaptor.addChild(root_0, columnExpressionRest33.getTree());

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
    // $ANTLR end columnExpression

    public static class columnExpressionRest_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start columnExpressionRest
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:155:1: columnExpressionRest : ( | '[' stringVal ']' columnExpressionRest );
    public final columnExpressionRest_return columnExpressionRest() throws RecognitionException {
        columnExpressionRest_return retval = new columnExpressionRest_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal34=null;
        Token char_literal36=null;
        stringVal_return stringVal35 = null;

        columnExpressionRest_return columnExpressionRest37 = null;


        CommonTree char_literal34_tree=null;
        CommonTree char_literal36_tree=null;

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:156:2: ( | '[' stringVal ']' columnExpressionRest )
            int alt10=2;
            int LA10_0 = input.LA(1);

            if ( (LA10_0==EOF||LA10_0==SEMICOLON||(LA10_0>=K_FROM && LA10_0<=K_WHERE)||LA10_0==K_LIMIT||LA10_0==COMMA||LA10_0==49) ) {
                alt10=1;
            }
            else if ( (LA10_0==50) ) {
                alt10=2;
            }
            else {
                if (backtracking>0) {failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("155:1: columnExpressionRest : ( | '[' stringVal ']' columnExpressionRest );", 10, 0, input);

                throw nvae;
            }
            switch (alt10) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:157:2: 
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    }
                    break;
                case 2 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:157:5: '[' stringVal ']' columnExpressionRest
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    char_literal34=(Token)input.LT(1);
                    match(input,50,FOLLOW_50_in_columnExpressionRest570); if (failed) return retval;
                    if ( backtracking==0 ) {
                    char_literal34_tree = (CommonTree)adaptor.create(char_literal34);
                    adaptor.addChild(root_0, char_literal34_tree);
                    }
                    pushFollow(FOLLOW_stringVal_in_columnExpressionRest572);
                    stringVal35=stringVal();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, stringVal35.getTree());
                    char_literal36=(Token)input.LT(1);
                    match(input,51,FOLLOW_51_in_columnExpressionRest574); if (failed) return retval;
                    if ( backtracking==0 ) {
                    char_literal36_tree = (CommonTree)adaptor.create(char_literal36);
                    adaptor.addChild(root_0, char_literal36_tree);
                    }
                    pushFollow(FOLLOW_columnExpressionRest_in_columnExpressionRest576);
                    columnExpressionRest37=columnExpressionRest();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, columnExpressionRest37.getTree());

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
    // $ANTLR end columnExpressionRest

    public static class tableExpression_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start tableExpression
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:160:1: tableExpression : tableName '.' columnFamilyName '[' stringVal ']' ;
    public final tableExpression_return tableExpression() throws RecognitionException {
        tableExpression_return retval = new tableExpression_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal39=null;
        Token char_literal41=null;
        Token char_literal43=null;
        tableName_return tableName38 = null;

        columnFamilyName_return columnFamilyName40 = null;

        stringVal_return stringVal42 = null;


        CommonTree char_literal39_tree=null;
        CommonTree char_literal41_tree=null;
        CommonTree char_literal43_tree=null;

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:161:5: ( tableName '.' columnFamilyName '[' stringVal ']' )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:161:7: tableName '.' columnFamilyName '[' stringVal ']'
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_tableName_in_tableExpression590);
            tableName38=tableName();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) adaptor.addChild(root_0, tableName38.getTree());
            char_literal39=(Token)input.LT(1);
            match(input,52,FOLLOW_52_in_tableExpression592); if (failed) return retval;
            if ( backtracking==0 ) {
            char_literal39_tree = (CommonTree)adaptor.create(char_literal39);
            adaptor.addChild(root_0, char_literal39_tree);
            }
            pushFollow(FOLLOW_columnFamilyName_in_tableExpression594);
            columnFamilyName40=columnFamilyName();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) adaptor.addChild(root_0, columnFamilyName40.getTree());
            char_literal41=(Token)input.LT(1);
            match(input,50,FOLLOW_50_in_tableExpression596); if (failed) return retval;
            if ( backtracking==0 ) {
            char_literal41_tree = (CommonTree)adaptor.create(char_literal41);
            adaptor.addChild(root_0, char_literal41_tree);
            }
            pushFollow(FOLLOW_stringVal_in_tableExpression598);
            stringVal42=stringVal();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) adaptor.addChild(root_0, stringVal42.getTree());
            char_literal43=(Token)input.LT(1);
            match(input,51,FOLLOW_51_in_tableExpression600); if (failed) return retval;
            if ( backtracking==0 ) {
            char_literal43_tree = (CommonTree)adaptor.create(char_literal43);
            adaptor.addChild(root_0, char_literal43_tree);
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
    // $ANTLR end tableExpression

    public static class fromClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start fromClause
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:163:1: fromClause : K_FROM tableExpression -> ^( A_FROM tableExpression ) ;
    public final fromClause_return fromClause() throws RecognitionException {
        fromClause_return retval = new fromClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_FROM44=null;
        tableExpression_return tableExpression45 = null;


        CommonTree K_FROM44_tree=null;
        RewriteRuleTokenStream stream_K_FROM=new RewriteRuleTokenStream(adaptor,"token K_FROM");
        RewriteRuleSubtreeStream stream_tableExpression=new RewriteRuleSubtreeStream(adaptor,"rule tableExpression");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:164:5: ( K_FROM tableExpression -> ^( A_FROM tableExpression ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:164:7: K_FROM tableExpression
            {
            K_FROM44=(Token)input.LT(1);
            match(input,K_FROM,FOLLOW_K_FROM_in_fromClause613); if (failed) return retval;
            if ( backtracking==0 ) stream_K_FROM.add(K_FROM44);

            pushFollow(FOLLOW_tableExpression_in_fromClause615);
            tableExpression45=tableExpression();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_tableExpression.add(tableExpression45.getTree());

            // AST REWRITE
            // elements: tableExpression
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 164:30: -> ^( A_FROM tableExpression )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:164:33: ^( A_FROM tableExpression )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_FROM, "A_FROM"), root_1);

                adaptor.addChild(root_1, stream_tableExpression.next());

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
    // $ANTLR end fromClause

    public static class whereClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start whereClause
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:167:1: whereClause : ( K_WHERE keyInClause -> ^( A_WHERE keyInClause ) | K_WHERE keyExactMatch -> ^( A_WHERE keyExactMatch ) );
    public final whereClause_return whereClause() throws RecognitionException {
        whereClause_return retval = new whereClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_WHERE46=null;
        Token K_WHERE48=null;
        keyInClause_return keyInClause47 = null;

        keyExactMatch_return keyExactMatch49 = null;


        CommonTree K_WHERE46_tree=null;
        CommonTree K_WHERE48_tree=null;
        RewriteRuleTokenStream stream_K_WHERE=new RewriteRuleTokenStream(adaptor,"token K_WHERE");
        RewriteRuleSubtreeStream stream_keyExactMatch=new RewriteRuleSubtreeStream(adaptor,"rule keyExactMatch");
        RewriteRuleSubtreeStream stream_keyInClause=new RewriteRuleSubtreeStream(adaptor,"rule keyInClause");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:168:5: ( K_WHERE keyInClause -> ^( A_WHERE keyInClause ) | K_WHERE keyExactMatch -> ^( A_WHERE keyExactMatch ) )
            int alt11=2;
            int LA11_0 = input.LA(1);

            if ( (LA11_0==K_WHERE) ) {
                int LA11_1 = input.LA(2);

                if ( (LA11_1==Identifier) ) {
                    int LA11_2 = input.LA(3);

                    if ( (LA11_2==47) ) {
                        alt11=2;
                    }
                    else if ( (LA11_2==K_IN) ) {
                        alt11=1;
                    }
                    else {
                        if (backtracking>0) {failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("167:1: whereClause : ( K_WHERE keyInClause -> ^( A_WHERE keyInClause ) | K_WHERE keyExactMatch -> ^( A_WHERE keyExactMatch ) );", 11, 2, input);

                        throw nvae;
                    }
                }
                else {
                    if (backtracking>0) {failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("167:1: whereClause : ( K_WHERE keyInClause -> ^( A_WHERE keyInClause ) | K_WHERE keyExactMatch -> ^( A_WHERE keyExactMatch ) );", 11, 1, input);

                    throw nvae;
                }
            }
            else {
                if (backtracking>0) {failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("167:1: whereClause : ( K_WHERE keyInClause -> ^( A_WHERE keyInClause ) | K_WHERE keyExactMatch -> ^( A_WHERE keyExactMatch ) );", 11, 0, input);

                throw nvae;
            }
            switch (alt11) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:168:7: K_WHERE keyInClause
                    {
                    K_WHERE46=(Token)input.LT(1);
                    match(input,K_WHERE,FOLLOW_K_WHERE_in_whereClause640); if (failed) return retval;
                    if ( backtracking==0 ) stream_K_WHERE.add(K_WHERE46);

                    pushFollow(FOLLOW_keyInClause_in_whereClause642);
                    keyInClause47=keyInClause();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) stream_keyInClause.add(keyInClause47.getTree());

                    // AST REWRITE
                    // elements: keyInClause
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    if ( backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 168:29: -> ^( A_WHERE keyInClause )
                    {
                        // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:168:32: ^( A_WHERE keyInClause )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_WHERE, "A_WHERE"), root_1);

                        adaptor.addChild(root_1, stream_keyInClause.next());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    }

                    }
                    break;
                case 2 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:169:7: K_WHERE keyExactMatch
                    {
                    K_WHERE48=(Token)input.LT(1);
                    match(input,K_WHERE,FOLLOW_K_WHERE_in_whereClause660); if (failed) return retval;
                    if ( backtracking==0 ) stream_K_WHERE.add(K_WHERE48);

                    pushFollow(FOLLOW_keyExactMatch_in_whereClause662);
                    keyExactMatch49=keyExactMatch();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) stream_keyExactMatch.add(keyExactMatch49.getTree());

                    // AST REWRITE
                    // elements: keyExactMatch
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    if ( backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 169:29: -> ^( A_WHERE keyExactMatch )
                    {
                        // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:169:32: ^( A_WHERE keyExactMatch )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_WHERE, "A_WHERE"), root_1);

                        adaptor.addChild(root_1, stream_keyExactMatch.next());

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
    // $ANTLR end whereClause

    public static class keyInClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start keyInClause
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:172:1: keyInClause : columnOrSuperColumnName K_IN '(' a+= stringVal ( ',' a+= stringVal )* ')' -> ^( A_KEY_IN_LIST columnOrSuperColumnName ( $a)+ ) ;
    public final keyInClause_return keyInClause() throws RecognitionException {
        keyInClause_return retval = new keyInClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_IN51=null;
        Token char_literal52=null;
        Token char_literal53=null;
        Token char_literal54=null;
        List list_a=null;
        columnOrSuperColumnName_return columnOrSuperColumnName50 = null;

        RuleReturnScope a = null;
        CommonTree K_IN51_tree=null;
        CommonTree char_literal52_tree=null;
        CommonTree char_literal53_tree=null;
        CommonTree char_literal54_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_K_IN=new RewriteRuleTokenStream(adaptor,"token K_IN");
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleSubtreeStream stream_columnOrSuperColumnName=new RewriteRuleSubtreeStream(adaptor,"rule columnOrSuperColumnName");
        RewriteRuleSubtreeStream stream_stringVal=new RewriteRuleSubtreeStream(adaptor,"rule stringVal");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:173:5: ( columnOrSuperColumnName K_IN '(' a+= stringVal ( ',' a+= stringVal )* ')' -> ^( A_KEY_IN_LIST columnOrSuperColumnName ( $a)+ ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:173:7: columnOrSuperColumnName K_IN '(' a+= stringVal ( ',' a+= stringVal )* ')'
            {
            pushFollow(FOLLOW_columnOrSuperColumnName_in_keyInClause687);
            columnOrSuperColumnName50=columnOrSuperColumnName();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_columnOrSuperColumnName.add(columnOrSuperColumnName50.getTree());
            K_IN51=(Token)input.LT(1);
            match(input,K_IN,FOLLOW_K_IN_in_keyInClause689); if (failed) return retval;
            if ( backtracking==0 ) stream_K_IN.add(K_IN51);

            char_literal52=(Token)input.LT(1);
            match(input,48,FOLLOW_48_in_keyInClause691); if (failed) return retval;
            if ( backtracking==0 ) stream_48.add(char_literal52);

            pushFollow(FOLLOW_stringVal_in_keyInClause695);
            a=stringVal();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_stringVal.add(a.getTree());
            if (list_a==null) list_a=new ArrayList();
            list_a.add(a);

            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:173:53: ( ',' a+= stringVal )*
            loop12:
            do {
                int alt12=2;
                int LA12_0 = input.LA(1);

                if ( (LA12_0==COMMA) ) {
                    alt12=1;
                }


                switch (alt12) {
            	case 1 :
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:173:54: ',' a+= stringVal
            	    {
            	    char_literal53=(Token)input.LT(1);
            	    match(input,COMMA,FOLLOW_COMMA_in_keyInClause698); if (failed) return retval;
            	    if ( backtracking==0 ) stream_COMMA.add(char_literal53);

            	    pushFollow(FOLLOW_stringVal_in_keyInClause702);
            	    a=stringVal();
            	    _fsp--;
            	    if (failed) return retval;
            	    if ( backtracking==0 ) stream_stringVal.add(a.getTree());
            	    if (list_a==null) list_a=new ArrayList();
            	    list_a.add(a);


            	    }
            	    break;

            	default :
            	    break loop12;
                }
            } while (true);

            char_literal54=(Token)input.LT(1);
            match(input,49,FOLLOW_49_in_keyInClause706); if (failed) return retval;
            if ( backtracking==0 ) stream_49.add(char_literal54);


            // AST REWRITE
            // elements: columnOrSuperColumnName, a
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: a
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"token a",list_a);
            root_0 = (CommonTree)adaptor.nil();
            // 174:6: -> ^( A_KEY_IN_LIST columnOrSuperColumnName ( $a)+ )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:174:9: ^( A_KEY_IN_LIST columnOrSuperColumnName ( $a)+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_KEY_IN_LIST, "A_KEY_IN_LIST"), root_1);

                adaptor.addChild(root_1, stream_columnOrSuperColumnName.next());
                if ( !(stream_a.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_a.hasNext() ) {
                    adaptor.addChild(root_1, ((ParserRuleReturnScope)stream_a.next()).getTree());

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
    // $ANTLR end keyInClause

    public static class keyExactMatch_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start keyExactMatch
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:177:1: keyExactMatch : columnOrSuperColumnName '=' stringVal -> ^( A_KEY_EXACT_MATCH columnOrSuperColumnName stringVal ) ;
    public final keyExactMatch_return keyExactMatch() throws RecognitionException {
        keyExactMatch_return retval = new keyExactMatch_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal56=null;
        columnOrSuperColumnName_return columnOrSuperColumnName55 = null;

        stringVal_return stringVal57 = null;


        CommonTree char_literal56_tree=null;
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleSubtreeStream stream_columnOrSuperColumnName=new RewriteRuleSubtreeStream(adaptor,"rule columnOrSuperColumnName");
        RewriteRuleSubtreeStream stream_stringVal=new RewriteRuleSubtreeStream(adaptor,"rule stringVal");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:178:5: ( columnOrSuperColumnName '=' stringVal -> ^( A_KEY_EXACT_MATCH columnOrSuperColumnName stringVal ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:178:7: columnOrSuperColumnName '=' stringVal
            {
            pushFollow(FOLLOW_columnOrSuperColumnName_in_keyExactMatch740);
            columnOrSuperColumnName55=columnOrSuperColumnName();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_columnOrSuperColumnName.add(columnOrSuperColumnName55.getTree());
            char_literal56=(Token)input.LT(1);
            match(input,47,FOLLOW_47_in_keyExactMatch742); if (failed) return retval;
            if ( backtracking==0 ) stream_47.add(char_literal56);

            pushFollow(FOLLOW_stringVal_in_keyExactMatch744);
            stringVal57=stringVal();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_stringVal.add(stringVal57.getTree());

            // AST REWRITE
            // elements: stringVal, columnOrSuperColumnName
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 179:6: -> ^( A_KEY_EXACT_MATCH columnOrSuperColumnName stringVal )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:179:9: ^( A_KEY_EXACT_MATCH columnOrSuperColumnName stringVal )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_KEY_EXACT_MATCH, "A_KEY_EXACT_MATCH"), root_1);

                adaptor.addChild(root_1, stream_columnOrSuperColumnName.next());
                adaptor.addChild(root_1, stream_stringVal.next());

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
    // $ANTLR end keyExactMatch

    public static class limitClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start limitClause
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:182:1: limitClause : K_LIMIT IntegerLiteral -> ^( A_LIMIT IntegerLiteral ) ;
    public final limitClause_return limitClause() throws RecognitionException {
        limitClause_return retval = new limitClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_LIMIT58=null;
        Token IntegerLiteral59=null;

        CommonTree K_LIMIT58_tree=null;
        CommonTree IntegerLiteral59_tree=null;
        RewriteRuleTokenStream stream_IntegerLiteral=new RewriteRuleTokenStream(adaptor,"token IntegerLiteral");
        RewriteRuleTokenStream stream_K_LIMIT=new RewriteRuleTokenStream(adaptor,"token K_LIMIT");

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:183:5: ( K_LIMIT IntegerLiteral -> ^( A_LIMIT IntegerLiteral ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:183:7: K_LIMIT IntegerLiteral
            {
            K_LIMIT58=(Token)input.LT(1);
            match(input,K_LIMIT,FOLLOW_K_LIMIT_in_limitClause776); if (failed) return retval;
            if ( backtracking==0 ) stream_K_LIMIT.add(K_LIMIT58);

            IntegerLiteral59=(Token)input.LT(1);
            match(input,IntegerLiteral,FOLLOW_IntegerLiteral_in_limitClause778); if (failed) return retval;
            if ( backtracking==0 ) stream_IntegerLiteral.add(IntegerLiteral59);


            // AST REWRITE
            // elements: IntegerLiteral
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 183:30: -> ^( A_LIMIT IntegerLiteral )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:183:33: ^( A_LIMIT IntegerLiteral )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_LIMIT, "A_LIMIT"), root_1);

                adaptor.addChild(root_1, stream_IntegerLiteral.next());

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
    // $ANTLR end limitClause

    public static class deleteStmt_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start deleteStmt
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:185:1: deleteStmt : K_DELETE columnSpec -> ^( A_DELETE columnSpec ) ;
    public final deleteStmt_return deleteStmt() throws RecognitionException {
        deleteStmt_return retval = new deleteStmt_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token K_DELETE60=null;
        columnSpec_return columnSpec61 = null;


        CommonTree K_DELETE60_tree=null;
        RewriteRuleTokenStream stream_K_DELETE=new RewriteRuleTokenStream(adaptor,"token K_DELETE");
        RewriteRuleSubtreeStream stream_columnSpec=new RewriteRuleSubtreeStream(adaptor,"rule columnSpec");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:186:5: ( K_DELETE columnSpec -> ^( A_DELETE columnSpec ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:186:7: K_DELETE columnSpec
            {
            K_DELETE60=(Token)input.LT(1);
            match(input,K_DELETE,FOLLOW_K_DELETE_in_deleteStmt800); if (failed) return retval;
            if ( backtracking==0 ) stream_K_DELETE.add(K_DELETE60);

            pushFollow(FOLLOW_columnSpec_in_deleteStmt802);
            columnSpec61=columnSpec();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_columnSpec.add(columnSpec61.getTree());

            // AST REWRITE
            // elements: columnSpec
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 186:27: -> ^( A_DELETE columnSpec )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:186:30: ^( A_DELETE columnSpec )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_DELETE, "A_DELETE"), root_1);

                adaptor.addChild(root_1, stream_columnSpec.next());

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
    // $ANTLR end deleteStmt

    public static class columnSpec_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start columnSpec
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:189:1: columnSpec : tableName '.' columnFamilyName '[' rowKey ']' ( '[' a+= columnOrSuperColumnKey ']' ( '[' a+= columnOrSuperColumnKey ']' )? )? -> ^( A_COLUMN_ACCESS tableName columnFamilyName rowKey ( ( $a)+ )? ) ;
    public final columnSpec_return columnSpec() throws RecognitionException {
        columnSpec_return retval = new columnSpec_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal63=null;
        Token char_literal65=null;
        Token char_literal67=null;
        Token char_literal68=null;
        Token char_literal69=null;
        Token char_literal70=null;
        Token char_literal71=null;
        List list_a=null;
        tableName_return tableName62 = null;

        columnFamilyName_return columnFamilyName64 = null;

        rowKey_return rowKey66 = null;

        RuleReturnScope a = null;
        CommonTree char_literal63_tree=null;
        CommonTree char_literal65_tree=null;
        CommonTree char_literal67_tree=null;
        CommonTree char_literal68_tree=null;
        CommonTree char_literal69_tree=null;
        CommonTree char_literal70_tree=null;
        CommonTree char_literal71_tree=null;
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleTokenStream stream_52=new RewriteRuleTokenStream(adaptor,"token 52");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_columnOrSuperColumnKey=new RewriteRuleSubtreeStream(adaptor,"rule columnOrSuperColumnKey");
        RewriteRuleSubtreeStream stream_columnFamilyName=new RewriteRuleSubtreeStream(adaptor,"rule columnFamilyName");
        RewriteRuleSubtreeStream stream_tableName=new RewriteRuleSubtreeStream(adaptor,"rule tableName");
        RewriteRuleSubtreeStream stream_rowKey=new RewriteRuleSubtreeStream(adaptor,"rule rowKey");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:190:5: ( tableName '.' columnFamilyName '[' rowKey ']' ( '[' a+= columnOrSuperColumnKey ']' ( '[' a+= columnOrSuperColumnKey ']' )? )? -> ^( A_COLUMN_ACCESS tableName columnFamilyName rowKey ( ( $a)+ )? ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:190:7: tableName '.' columnFamilyName '[' rowKey ']' ( '[' a+= columnOrSuperColumnKey ']' ( '[' a+= columnOrSuperColumnKey ']' )? )?
            {
            pushFollow(FOLLOW_tableName_in_columnSpec827);
            tableName62=tableName();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_tableName.add(tableName62.getTree());
            char_literal63=(Token)input.LT(1);
            match(input,52,FOLLOW_52_in_columnSpec829); if (failed) return retval;
            if ( backtracking==0 ) stream_52.add(char_literal63);

            pushFollow(FOLLOW_columnFamilyName_in_columnSpec831);
            columnFamilyName64=columnFamilyName();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_columnFamilyName.add(columnFamilyName64.getTree());
            char_literal65=(Token)input.LT(1);
            match(input,50,FOLLOW_50_in_columnSpec833); if (failed) return retval;
            if ( backtracking==0 ) stream_50.add(char_literal65);

            pushFollow(FOLLOW_rowKey_in_columnSpec835);
            rowKey66=rowKey();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_rowKey.add(rowKey66.getTree());
            char_literal67=(Token)input.LT(1);
            match(input,51,FOLLOW_51_in_columnSpec837); if (failed) return retval;
            if ( backtracking==0 ) stream_51.add(char_literal67);

            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:191:9: ( '[' a+= columnOrSuperColumnKey ']' ( '[' a+= columnOrSuperColumnKey ']' )? )?
            int alt14=2;
            int LA14_0 = input.LA(1);

            if ( (LA14_0==50) ) {
                alt14=1;
            }
            switch (alt14) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:191:11: '[' a+= columnOrSuperColumnKey ']' ( '[' a+= columnOrSuperColumnKey ']' )?
                    {
                    char_literal68=(Token)input.LT(1);
                    match(input,50,FOLLOW_50_in_columnSpec850); if (failed) return retval;
                    if ( backtracking==0 ) stream_50.add(char_literal68);

                    pushFollow(FOLLOW_columnOrSuperColumnKey_in_columnSpec854);
                    a=columnOrSuperColumnKey();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) stream_columnOrSuperColumnKey.add(a.getTree());
                    if (list_a==null) list_a=new ArrayList();
                    list_a.add(a);

                    char_literal69=(Token)input.LT(1);
                    match(input,51,FOLLOW_51_in_columnSpec856); if (failed) return retval;
                    if ( backtracking==0 ) stream_51.add(char_literal69);

                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:192:13: ( '[' a+= columnOrSuperColumnKey ']' )?
                    int alt13=2;
                    int LA13_0 = input.LA(1);

                    if ( (LA13_0==50) ) {
                        alt13=1;
                    }
                    switch (alt13) {
                        case 1 :
                            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:192:14: '[' a+= columnOrSuperColumnKey ']'
                            {
                            char_literal70=(Token)input.LT(1);
                            match(input,50,FOLLOW_50_in_columnSpec872); if (failed) return retval;
                            if ( backtracking==0 ) stream_50.add(char_literal70);

                            pushFollow(FOLLOW_columnOrSuperColumnKey_in_columnSpec876);
                            a=columnOrSuperColumnKey();
                            _fsp--;
                            if (failed) return retval;
                            if ( backtracking==0 ) stream_columnOrSuperColumnKey.add(a.getTree());
                            if (list_a==null) list_a=new ArrayList();
                            list_a.add(a);

                            char_literal71=(Token)input.LT(1);
                            match(input,51,FOLLOW_51_in_columnSpec878); if (failed) return retval;
                            if ( backtracking==0 ) stream_51.add(char_literal71);


                            }
                            break;

                    }


                    }
                    break;

            }


            // AST REWRITE
            // elements: tableName, a, columnFamilyName, rowKey
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: a
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"token a",list_a);
            root_0 = (CommonTree)adaptor.nil();
            // 194:9: -> ^( A_COLUMN_ACCESS tableName columnFamilyName rowKey ( ( $a)+ )? )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:194:12: ^( A_COLUMN_ACCESS tableName columnFamilyName rowKey ( ( $a)+ )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_COLUMN_ACCESS, "A_COLUMN_ACCESS"), root_1);

                adaptor.addChild(root_1, stream_tableName.next());
                adaptor.addChild(root_1, stream_columnFamilyName.next());
                adaptor.addChild(root_1, stream_rowKey.next());
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:194:64: ( ( $a)+ )?
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
    // $ANTLR end columnSpec

    public static class tableName_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start tableName
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:197:1: tableName : Identifier ;
    public final tableName_return tableName() throws RecognitionException {
        tableName_return retval = new tableName_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier72=null;

        CommonTree Identifier72_tree=null;

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:197:10: ( Identifier )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:197:12: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier72=(Token)input.LT(1);
            match(input,Identifier,FOLLOW_Identifier_in_tableName931); if (failed) return retval;
            if ( backtracking==0 ) {
            Identifier72_tree = (CommonTree)adaptor.create(Identifier72);
            adaptor.addChild(root_0, Identifier72_tree);
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
    // $ANTLR end tableName

    public static class columnFamilyName_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start columnFamilyName
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:199:1: columnFamilyName : Identifier ;
    public final columnFamilyName_return columnFamilyName() throws RecognitionException {
        columnFamilyName_return retval = new columnFamilyName_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier73=null;

        CommonTree Identifier73_tree=null;

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:199:17: ( Identifier )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:199:19: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier73=(Token)input.LT(1);
            match(input,Identifier,FOLLOW_Identifier_in_columnFamilyName938); if (failed) return retval;
            if ( backtracking==0 ) {
            Identifier73_tree = (CommonTree)adaptor.create(Identifier73);
            adaptor.addChild(root_0, Identifier73_tree);
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
    // $ANTLR end columnFamilyName

    public static class valueExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start valueExpr
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:201:1: valueExpr : ( cellValue | columnMapValue | superColumnMapValue );
    public final valueExpr_return valueExpr() throws RecognitionException {
        valueExpr_return retval = new valueExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        cellValue_return cellValue74 = null;

        columnMapValue_return columnMapValue75 = null;

        superColumnMapValue_return superColumnMapValue76 = null;



        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:202:4: ( cellValue | columnMapValue | superColumnMapValue )
            int alt15=3;
            int LA15_0 = input.LA(1);

            if ( (LA15_0==StringLiteral||LA15_0==53) ) {
                alt15=1;
            }
            else if ( (LA15_0==LEFT_BRACE) ) {
                int LA15_2 = input.LA(2);

                if ( (LA15_2==StringLiteral||LA15_2==53) ) {
                    int LA15_3 = input.LA(3);

                    if ( (LA15_3==ASSOC) ) {
                        int LA15_4 = input.LA(4);

                        if ( (LA15_4==LEFT_BRACE) ) {
                            alt15=3;
                        }
                        else if ( (LA15_4==StringLiteral||LA15_4==53) ) {
                            alt15=2;
                        }
                        else {
                            if (backtracking>0) {failed=true; return retval;}
                            NoViableAltException nvae =
                                new NoViableAltException("201:1: valueExpr : ( cellValue | columnMapValue | superColumnMapValue );", 15, 4, input);

                            throw nvae;
                        }
                    }
                    else {
                        if (backtracking>0) {failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("201:1: valueExpr : ( cellValue | columnMapValue | superColumnMapValue );", 15, 3, input);

                        throw nvae;
                    }
                }
                else {
                    if (backtracking>0) {failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("201:1: valueExpr : ( cellValue | columnMapValue | superColumnMapValue );", 15, 2, input);

                    throw nvae;
                }
            }
            else {
                if (backtracking>0) {failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("201:1: valueExpr : ( cellValue | columnMapValue | superColumnMapValue );", 15, 0, input);

                throw nvae;
            }
            switch (alt15) {
                case 1 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:202:7: cellValue
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_cellValue_in_valueExpr950);
                    cellValue74=cellValue();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, cellValue74.getTree());

                    }
                    break;
                case 2 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:203:7: columnMapValue
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_columnMapValue_in_valueExpr958);
                    columnMapValue75=columnMapValue();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, columnMapValue75.getTree());

                    }
                    break;
                case 3 :
                    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:204:7: superColumnMapValue
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_superColumnMapValue_in_valueExpr966);
                    superColumnMapValue76=superColumnMapValue();
                    _fsp--;
                    if (failed) return retval;
                    if ( backtracking==0 ) adaptor.addChild(root_0, superColumnMapValue76.getTree());

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
    // $ANTLR end valueExpr

    public static class cellValue_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start cellValue
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:207:1: cellValue : stringVal ;
    public final cellValue_return cellValue() throws RecognitionException {
        cellValue_return retval = new cellValue_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        stringVal_return stringVal77 = null;



        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:208:4: ( stringVal )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:208:6: stringVal
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_stringVal_in_cellValue981);
            stringVal77=stringVal();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) adaptor.addChild(root_0, stringVal77.getTree());

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
    // $ANTLR end cellValue

    public static class columnMapValue_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start columnMapValue
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:210:1: columnMapValue : LEFT_BRACE columnMapEntry ( COMMA columnMapEntry )* RIGHT_BRACE -> ^( A_COLUMN_MAP_VALUE ( columnMapEntry )+ ) ;
    public final columnMapValue_return columnMapValue() throws RecognitionException {
        columnMapValue_return retval = new columnMapValue_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token LEFT_BRACE78=null;
        Token COMMA80=null;
        Token RIGHT_BRACE82=null;
        columnMapEntry_return columnMapEntry79 = null;

        columnMapEntry_return columnMapEntry81 = null;


        CommonTree LEFT_BRACE78_tree=null;
        CommonTree COMMA80_tree=null;
        CommonTree RIGHT_BRACE82_tree=null;
        RewriteRuleTokenStream stream_RIGHT_BRACE=new RewriteRuleTokenStream(adaptor,"token RIGHT_BRACE");
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_LEFT_BRACE=new RewriteRuleTokenStream(adaptor,"token LEFT_BRACE");
        RewriteRuleSubtreeStream stream_columnMapEntry=new RewriteRuleSubtreeStream(adaptor,"rule columnMapEntry");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:211:4: ( LEFT_BRACE columnMapEntry ( COMMA columnMapEntry )* RIGHT_BRACE -> ^( A_COLUMN_MAP_VALUE ( columnMapEntry )+ ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:211:6: LEFT_BRACE columnMapEntry ( COMMA columnMapEntry )* RIGHT_BRACE
            {
            LEFT_BRACE78=(Token)input.LT(1);
            match(input,LEFT_BRACE,FOLLOW_LEFT_BRACE_in_columnMapValue992); if (failed) return retval;
            if ( backtracking==0 ) stream_LEFT_BRACE.add(LEFT_BRACE78);

            pushFollow(FOLLOW_columnMapEntry_in_columnMapValue994);
            columnMapEntry79=columnMapEntry();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_columnMapEntry.add(columnMapEntry79.getTree());
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:211:32: ( COMMA columnMapEntry )*
            loop16:
            do {
                int alt16=2;
                int LA16_0 = input.LA(1);

                if ( (LA16_0==COMMA) ) {
                    alt16=1;
                }


                switch (alt16) {
            	case 1 :
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:211:33: COMMA columnMapEntry
            	    {
            	    COMMA80=(Token)input.LT(1);
            	    match(input,COMMA,FOLLOW_COMMA_in_columnMapValue997); if (failed) return retval;
            	    if ( backtracking==0 ) stream_COMMA.add(COMMA80);

            	    pushFollow(FOLLOW_columnMapEntry_in_columnMapValue999);
            	    columnMapEntry81=columnMapEntry();
            	    _fsp--;
            	    if (failed) return retval;
            	    if ( backtracking==0 ) stream_columnMapEntry.add(columnMapEntry81.getTree());

            	    }
            	    break;

            	default :
            	    break loop16;
                }
            } while (true);

            RIGHT_BRACE82=(Token)input.LT(1);
            match(input,RIGHT_BRACE,FOLLOW_RIGHT_BRACE_in_columnMapValue1003); if (failed) return retval;
            if ( backtracking==0 ) stream_RIGHT_BRACE.add(RIGHT_BRACE82);


            // AST REWRITE
            // elements: columnMapEntry
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 212:6: -> ^( A_COLUMN_MAP_VALUE ( columnMapEntry )+ )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:212:9: ^( A_COLUMN_MAP_VALUE ( columnMapEntry )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_COLUMN_MAP_VALUE, "A_COLUMN_MAP_VALUE"), root_1);

                if ( !(stream_columnMapEntry.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_columnMapEntry.hasNext() ) {
                    adaptor.addChild(root_1, stream_columnMapEntry.next());

                }
                stream_columnMapEntry.reset();

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
    // $ANTLR end columnMapValue

    public static class superColumnMapValue_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start superColumnMapValue
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:215:1: superColumnMapValue : LEFT_BRACE superColumnMapEntry ( COMMA superColumnMapEntry )* RIGHT_BRACE -> ^( A_SUPERCOLUMN_MAP_VALUE ( superColumnMapEntry )+ ) ;
    public final superColumnMapValue_return superColumnMapValue() throws RecognitionException {
        superColumnMapValue_return retval = new superColumnMapValue_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token LEFT_BRACE83=null;
        Token COMMA85=null;
        Token RIGHT_BRACE87=null;
        superColumnMapEntry_return superColumnMapEntry84 = null;

        superColumnMapEntry_return superColumnMapEntry86 = null;


        CommonTree LEFT_BRACE83_tree=null;
        CommonTree COMMA85_tree=null;
        CommonTree RIGHT_BRACE87_tree=null;
        RewriteRuleTokenStream stream_RIGHT_BRACE=new RewriteRuleTokenStream(adaptor,"token RIGHT_BRACE");
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_LEFT_BRACE=new RewriteRuleTokenStream(adaptor,"token LEFT_BRACE");
        RewriteRuleSubtreeStream stream_superColumnMapEntry=new RewriteRuleSubtreeStream(adaptor,"rule superColumnMapEntry");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:216:4: ( LEFT_BRACE superColumnMapEntry ( COMMA superColumnMapEntry )* RIGHT_BRACE -> ^( A_SUPERCOLUMN_MAP_VALUE ( superColumnMapEntry )+ ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:216:6: LEFT_BRACE superColumnMapEntry ( COMMA superColumnMapEntry )* RIGHT_BRACE
            {
            LEFT_BRACE83=(Token)input.LT(1);
            match(input,LEFT_BRACE,FOLLOW_LEFT_BRACE_in_superColumnMapValue1032); if (failed) return retval;
            if ( backtracking==0 ) stream_LEFT_BRACE.add(LEFT_BRACE83);

            pushFollow(FOLLOW_superColumnMapEntry_in_superColumnMapValue1034);
            superColumnMapEntry84=superColumnMapEntry();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_superColumnMapEntry.add(superColumnMapEntry84.getTree());
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:216:37: ( COMMA superColumnMapEntry )*
            loop17:
            do {
                int alt17=2;
                int LA17_0 = input.LA(1);

                if ( (LA17_0==COMMA) ) {
                    alt17=1;
                }


                switch (alt17) {
            	case 1 :
            	    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:216:38: COMMA superColumnMapEntry
            	    {
            	    COMMA85=(Token)input.LT(1);
            	    match(input,COMMA,FOLLOW_COMMA_in_superColumnMapValue1037); if (failed) return retval;
            	    if ( backtracking==0 ) stream_COMMA.add(COMMA85);

            	    pushFollow(FOLLOW_superColumnMapEntry_in_superColumnMapValue1039);
            	    superColumnMapEntry86=superColumnMapEntry();
            	    _fsp--;
            	    if (failed) return retval;
            	    if ( backtracking==0 ) stream_superColumnMapEntry.add(superColumnMapEntry86.getTree());

            	    }
            	    break;

            	default :
            	    break loop17;
                }
            } while (true);

            RIGHT_BRACE87=(Token)input.LT(1);
            match(input,RIGHT_BRACE,FOLLOW_RIGHT_BRACE_in_superColumnMapValue1043); if (failed) return retval;
            if ( backtracking==0 ) stream_RIGHT_BRACE.add(RIGHT_BRACE87);


            // AST REWRITE
            // elements: superColumnMapEntry
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 217:6: -> ^( A_SUPERCOLUMN_MAP_VALUE ( superColumnMapEntry )+ )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:217:9: ^( A_SUPERCOLUMN_MAP_VALUE ( superColumnMapEntry )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_SUPERCOLUMN_MAP_VALUE, "A_SUPERCOLUMN_MAP_VALUE"), root_1);

                if ( !(stream_superColumnMapEntry.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_superColumnMapEntry.hasNext() ) {
                    adaptor.addChild(root_1, stream_superColumnMapEntry.next());

                }
                stream_superColumnMapEntry.reset();

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
    // $ANTLR end superColumnMapValue

    public static class columnMapEntry_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start columnMapEntry
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:220:1: columnMapEntry : columnKey ASSOC cellValue -> ^( A_COLUMN_MAP_ENTRY columnKey cellValue ) ;
    public final columnMapEntry_return columnMapEntry() throws RecognitionException {
        columnMapEntry_return retval = new columnMapEntry_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ASSOC89=null;
        columnKey_return columnKey88 = null;

        cellValue_return cellValue90 = null;


        CommonTree ASSOC89_tree=null;
        RewriteRuleTokenStream stream_ASSOC=new RewriteRuleTokenStream(adaptor,"token ASSOC");
        RewriteRuleSubtreeStream stream_columnKey=new RewriteRuleSubtreeStream(adaptor,"rule columnKey");
        RewriteRuleSubtreeStream stream_cellValue=new RewriteRuleSubtreeStream(adaptor,"rule cellValue");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:221:4: ( columnKey ASSOC cellValue -> ^( A_COLUMN_MAP_ENTRY columnKey cellValue ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:221:6: columnKey ASSOC cellValue
            {
            pushFollow(FOLLOW_columnKey_in_columnMapEntry1072);
            columnKey88=columnKey();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_columnKey.add(columnKey88.getTree());
            ASSOC89=(Token)input.LT(1);
            match(input,ASSOC,FOLLOW_ASSOC_in_columnMapEntry1074); if (failed) return retval;
            if ( backtracking==0 ) stream_ASSOC.add(ASSOC89);

            pushFollow(FOLLOW_cellValue_in_columnMapEntry1076);
            cellValue90=cellValue();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_cellValue.add(cellValue90.getTree());

            // AST REWRITE
            // elements: columnKey, cellValue
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 221:32: -> ^( A_COLUMN_MAP_ENTRY columnKey cellValue )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:221:35: ^( A_COLUMN_MAP_ENTRY columnKey cellValue )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_COLUMN_MAP_ENTRY, "A_COLUMN_MAP_ENTRY"), root_1);

                adaptor.addChild(root_1, stream_columnKey.next());
                adaptor.addChild(root_1, stream_cellValue.next());

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
    // $ANTLR end columnMapEntry

    public static class superColumnMapEntry_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start superColumnMapEntry
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:224:1: superColumnMapEntry : superColumnKey ASSOC columnMapValue -> ^( A_SUPERCOLUMN_MAP_ENTRY superColumnKey columnMapValue ) ;
    public final superColumnMapEntry_return superColumnMapEntry() throws RecognitionException {
        superColumnMapEntry_return retval = new superColumnMapEntry_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ASSOC92=null;
        superColumnKey_return superColumnKey91 = null;

        columnMapValue_return columnMapValue93 = null;


        CommonTree ASSOC92_tree=null;
        RewriteRuleTokenStream stream_ASSOC=new RewriteRuleTokenStream(adaptor,"token ASSOC");
        RewriteRuleSubtreeStream stream_columnMapValue=new RewriteRuleSubtreeStream(adaptor,"rule columnMapValue");
        RewriteRuleSubtreeStream stream_superColumnKey=new RewriteRuleSubtreeStream(adaptor,"rule superColumnKey");
        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:225:4: ( superColumnKey ASSOC columnMapValue -> ^( A_SUPERCOLUMN_MAP_ENTRY superColumnKey columnMapValue ) )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:225:6: superColumnKey ASSOC columnMapValue
            {
            pushFollow(FOLLOW_superColumnKey_in_superColumnMapEntry1101);
            superColumnKey91=superColumnKey();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_superColumnKey.add(superColumnKey91.getTree());
            ASSOC92=(Token)input.LT(1);
            match(input,ASSOC,FOLLOW_ASSOC_in_superColumnMapEntry1103); if (failed) return retval;
            if ( backtracking==0 ) stream_ASSOC.add(ASSOC92);

            pushFollow(FOLLOW_columnMapValue_in_superColumnMapEntry1105);
            columnMapValue93=columnMapValue();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) stream_columnMapValue.add(columnMapValue93.getTree());

            // AST REWRITE
            // elements: columnMapValue, superColumnKey
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            if ( backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"token retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 225:42: -> ^( A_SUPERCOLUMN_MAP_ENTRY superColumnKey columnMapValue )
            {
                // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:225:45: ^( A_SUPERCOLUMN_MAP_ENTRY superColumnKey columnMapValue )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(adaptor.create(A_SUPERCOLUMN_MAP_ENTRY, "A_SUPERCOLUMN_MAP_ENTRY"), root_1);

                adaptor.addChild(root_1, stream_superColumnKey.next());
                adaptor.addChild(root_1, stream_columnMapValue.next());

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
    // $ANTLR end superColumnMapEntry

    public static class columnOrSuperColumnName_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start columnOrSuperColumnName
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:228:1: columnOrSuperColumnName : Identifier ;
    public final columnOrSuperColumnName_return columnOrSuperColumnName() throws RecognitionException {
        columnOrSuperColumnName_return retval = new columnOrSuperColumnName_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token Identifier94=null;

        CommonTree Identifier94_tree=null;

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:228:24: ( Identifier )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:228:26: Identifier
            {
            root_0 = (CommonTree)adaptor.nil();

            Identifier94=(Token)input.LT(1);
            match(input,Identifier,FOLLOW_Identifier_in_columnOrSuperColumnName1126); if (failed) return retval;
            if ( backtracking==0 ) {
            Identifier94_tree = (CommonTree)adaptor.create(Identifier94);
            adaptor.addChild(root_0, Identifier94_tree);
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
    // $ANTLR end columnOrSuperColumnName

    public static class rowKey_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start rowKey
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:230:1: rowKey : stringVal ;
    public final rowKey_return rowKey() throws RecognitionException {
        rowKey_return retval = new rowKey_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        stringVal_return stringVal95 = null;



        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:230:7: ( stringVal )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:230:26: stringVal
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_stringVal_in_rowKey1151);
            stringVal95=stringVal();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) adaptor.addChild(root_0, stringVal95.getTree());

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

    public static class columnOrSuperColumnKey_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start columnOrSuperColumnKey
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:231:1: columnOrSuperColumnKey : stringVal ;
    public final columnOrSuperColumnKey_return columnOrSuperColumnKey() throws RecognitionException {
        columnOrSuperColumnKey_return retval = new columnOrSuperColumnKey_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        stringVal_return stringVal96 = null;



        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:231:23: ( stringVal )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:231:26: stringVal
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_stringVal_in_columnOrSuperColumnKey1158);
            stringVal96=stringVal();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) adaptor.addChild(root_0, stringVal96.getTree());

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
    // $ANTLR end columnOrSuperColumnKey

    public static class columnKey_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start columnKey
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:232:1: columnKey : stringVal ;
    public final columnKey_return columnKey() throws RecognitionException {
        columnKey_return retval = new columnKey_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        stringVal_return stringVal97 = null;



        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:232:10: ( stringVal )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:232:26: stringVal
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_stringVal_in_columnKey1178);
            stringVal97=stringVal();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) adaptor.addChild(root_0, stringVal97.getTree());

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
    // $ANTLR end columnKey

    public static class superColumnKey_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start superColumnKey
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:233:1: superColumnKey : stringVal ;
    public final superColumnKey_return superColumnKey() throws RecognitionException {
        superColumnKey_return retval = new superColumnKey_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        stringVal_return stringVal98 = null;



        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:233:15: ( stringVal )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:233:26: stringVal
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_stringVal_in_superColumnKey1193);
            stringVal98=stringVal();
            _fsp--;
            if (failed) return retval;
            if ( backtracking==0 ) adaptor.addChild(root_0, stringVal98.getTree());

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
    // $ANTLR end superColumnKey

    public static class stringVal_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start stringVal
    // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:237:1: stringVal : ( '?' | StringLiteral );
    public final stringVal_return stringVal() throws RecognitionException {
        stringVal_return retval = new stringVal_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set99=null;

        CommonTree set99_tree=null;

        try {
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:238:5: ( '?' | StringLiteral )
            // /home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g:
            {
            root_0 = (CommonTree)adaptor.nil();

            set99=(Token)input.LT(1);
            if ( input.LA(1)==StringLiteral||input.LA(1)==53 ) {
                input.consume();
                if ( backtracking==0 ) adaptor.addChild(root_0, adaptor.create(set99));
                errorRecovery=false;failed=false;
            }
            else {
                if (backtracking>0) {failed=true; return retval;}
                MismatchedSetException mse =
                    new MismatchedSetException(null,input);
                recoverFromMismatchedSet(input,mse,FOLLOW_set_in_stringVal0);    throw mse;
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
    // $ANTLR end stringVal


 

    public static final BitSet FOLLOW_stmt_in_root266 = new BitSet(new long[]{0x0000000000400000L});
    public static final BitSet FOLLOW_SEMICOLON_in_root268 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_root271 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_EXPLAIN_in_root283 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_K_PLAN_in_root285 = new BitSet(new long[]{0x000000020E000000L});
    public static final BitSet FOLLOW_stmt_in_root287 = new BitSet(new long[]{0x0000000000400000L});
    public static final BitSet FOLLOW_SEMICOLON_in_root289 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_root292 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteStmt_in_stmt317 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_getStmt_in_stmt325 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selectStmt_in_stmt333 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_setStmt_in_stmt341 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_GET_in_getStmt358 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_columnSpec_in_getStmt360 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_SET_in_setStmt386 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_columnSpec_in_setStmt388 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_setStmt390 = new BitSet(new long[]{0x0020008800000000L});
    public static final BitSet FOLLOW_valueExpr_in_setStmt392 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selectClause_in_selectStmt419 = new BitSet(new long[]{0x00000000B0000002L});
    public static final BitSet FOLLOW_fromClause_in_selectStmt429 = new BitSet(new long[]{0x00000000A0000002L});
    public static final BitSet FOLLOW_whereClause_in_selectStmt440 = new BitSet(new long[]{0x0000000080000002L});
    public static final BitSet FOLLOW_limitClause_in_selectStmt451 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_SELECT_in_selectClause483 = new BitSet(new long[]{0x0001000400000000L});
    public static final BitSet FOLLOW_selectList_in_selectClause485 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selectListItem_in_selectList504 = new BitSet(new long[]{0x0000001000000002L});
    public static final BitSet FOLLOW_COMMA_in_selectList507 = new BitSet(new long[]{0x0001000400000000L});
    public static final BitSet FOLLOW_selectListItem_in_selectList509 = new BitSet(new long[]{0x0000001000000002L});
    public static final BitSet FOLLOW_columnExpression_in_selectListItem522 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_48_in_selectListItem527 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_selectStmt_in_selectListItem529 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_selectListItem531 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_columnOrSuperColumnName_in_columnExpression551 = new BitSet(new long[]{0x0004000000000002L});
    public static final BitSet FOLLOW_columnExpressionRest_in_columnExpression553 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_50_in_columnExpressionRest570 = new BitSet(new long[]{0x0020008000000000L});
    public static final BitSet FOLLOW_stringVal_in_columnExpressionRest572 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_51_in_columnExpressionRest574 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_columnExpressionRest_in_columnExpressionRest576 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_tableName_in_tableExpression590 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_52_in_tableExpression592 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_columnFamilyName_in_tableExpression594 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_50_in_tableExpression596 = new BitSet(new long[]{0x0020008000000000L});
    public static final BitSet FOLLOW_stringVal_in_tableExpression598 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_51_in_tableExpression600 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_FROM_in_fromClause613 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_tableExpression_in_fromClause615 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_WHERE_in_whereClause640 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_keyInClause_in_whereClause642 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_WHERE_in_whereClause660 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_keyExactMatch_in_whereClause662 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_columnOrSuperColumnName_in_keyInClause687 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_K_IN_in_keyInClause689 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_keyInClause691 = new BitSet(new long[]{0x0020008000000000L});
    public static final BitSet FOLLOW_stringVal_in_keyInClause695 = new BitSet(new long[]{0x0002001000000000L});
    public static final BitSet FOLLOW_COMMA_in_keyInClause698 = new BitSet(new long[]{0x0020008000000000L});
    public static final BitSet FOLLOW_stringVal_in_keyInClause702 = new BitSet(new long[]{0x0002001000000000L});
    public static final BitSet FOLLOW_49_in_keyInClause706 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_columnOrSuperColumnName_in_keyExactMatch740 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_keyExactMatch742 = new BitSet(new long[]{0x0020008000000000L});
    public static final BitSet FOLLOW_stringVal_in_keyExactMatch744 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_LIMIT_in_limitClause776 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_IntegerLiteral_in_limitClause778 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DELETE_in_deleteStmt800 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_columnSpec_in_deleteStmt802 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_tableName_in_columnSpec827 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_52_in_columnSpec829 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_columnFamilyName_in_columnSpec831 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_50_in_columnSpec833 = new BitSet(new long[]{0x0020008000000000L});
    public static final BitSet FOLLOW_rowKey_in_columnSpec835 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_51_in_columnSpec837 = new BitSet(new long[]{0x0004000000000002L});
    public static final BitSet FOLLOW_50_in_columnSpec850 = new BitSet(new long[]{0x0020008000000000L});
    public static final BitSet FOLLOW_columnOrSuperColumnKey_in_columnSpec854 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_51_in_columnSpec856 = new BitSet(new long[]{0x0004000000000002L});
    public static final BitSet FOLLOW_50_in_columnSpec872 = new BitSet(new long[]{0x0020008000000000L});
    public static final BitSet FOLLOW_columnOrSuperColumnKey_in_columnSpec876 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_51_in_columnSpec878 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_tableName931 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_columnFamilyName938 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_cellValue_in_valueExpr950 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_columnMapValue_in_valueExpr958 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_superColumnMapValue_in_valueExpr966 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_stringVal_in_cellValue981 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LEFT_BRACE_in_columnMapValue992 = new BitSet(new long[]{0x0020008000000000L});
    public static final BitSet FOLLOW_columnMapEntry_in_columnMapValue994 = new BitSet(new long[]{0x0000003000000000L});
    public static final BitSet FOLLOW_COMMA_in_columnMapValue997 = new BitSet(new long[]{0x0020008000000000L});
    public static final BitSet FOLLOW_columnMapEntry_in_columnMapValue999 = new BitSet(new long[]{0x0000003000000000L});
    public static final BitSet FOLLOW_RIGHT_BRACE_in_columnMapValue1003 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LEFT_BRACE_in_superColumnMapValue1032 = new BitSet(new long[]{0x0020008000000000L});
    public static final BitSet FOLLOW_superColumnMapEntry_in_superColumnMapValue1034 = new BitSet(new long[]{0x0000003000000000L});
    public static final BitSet FOLLOW_COMMA_in_superColumnMapValue1037 = new BitSet(new long[]{0x0020008000000000L});
    public static final BitSet FOLLOW_superColumnMapEntry_in_superColumnMapValue1039 = new BitSet(new long[]{0x0000003000000000L});
    public static final BitSet FOLLOW_RIGHT_BRACE_in_superColumnMapValue1043 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_columnKey_in_columnMapEntry1072 = new BitSet(new long[]{0x0000004000000000L});
    public static final BitSet FOLLOW_ASSOC_in_columnMapEntry1074 = new BitSet(new long[]{0x0020008000000000L});
    public static final BitSet FOLLOW_cellValue_in_columnMapEntry1076 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_superColumnKey_in_superColumnMapEntry1101 = new BitSet(new long[]{0x0000004000000000L});
    public static final BitSet FOLLOW_ASSOC_in_superColumnMapEntry1103 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_columnMapValue_in_superColumnMapEntry1105 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_columnOrSuperColumnName1126 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_stringVal_in_rowKey1151 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_stringVal_in_columnOrSuperColumnKey1158 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_stringVal_in_columnKey1178 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_stringVal_in_superColumnKey1193 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_stringVal0 = new BitSet(new long[]{0x0000000000000002L});

}