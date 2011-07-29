// $ANTLR 3.2 Sep 23, 2009 12:02:23 /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g 2011-07-28 15:11:05

    package org.apache.cassandra.cql;
    import java.util.Map;
    import java.util.HashMap;
    import java.util.Collections;
    import java.util.List;
    import java.util.ArrayList;
    import org.apache.cassandra.thrift.ConsistencyLevel;
    import org.apache.cassandra.thrift.InvalidRequestException;

    import static org.apache.cassandra.cql.AlterTableStatement.OperationType;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class CqlParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "K_USE", "IDENT", "K_SELECT", "K_COUNT", "K_FROM", "STRING_LITERAL", "INTEGER", "K_USING", "K_CONSISTENCY", "K_LEVEL", "K_WHERE", "K_LIMIT", "K_FIRST", "K_REVERSED", "RANGEOP", "K_AND", "K_IN", "K_INSERT", "K_INTO", "K_VALUES", "K_TIMESTAMP", "K_TTL", "K_BEGIN", "K_BATCH", "K_APPLY", "K_UPDATE", "K_SET", "K_DELETE", "K_CREATE", "K_KEYSPACE", "K_WITH", "COMPIDENT", "K_COLUMNFAMILY", "K_PRIMARY", "K_KEY", "FLOAT", "K_INDEX", "K_ON", "K_DROP", "K_ALTER", "K_TYPE", "K_ADD", "UUID", "K_TRUNCATE", "S", "E", "L", "C", "T", "F", "R", "O", "M", "W", "H", "A", "N", "D", "K", "Y", "I", "U", "P", "G", "Q", "V", "B", "X", "J", "Z", "DIGIT", "LETTER", "HEX", "WS", "COMMENT", "MULTILINE_COMMENT", "'('", "')'", "','", "'\\*'", "';'", "'='", "'bytea'", "'ascii'", "'text'", "'varchar'", "'int'", "'varint'", "'bigint'", "'uuid'", "'counter'", "'boolean'", "'date'", "'float'", "'double'", "'+'", "'-'", "'<'", "'<='", "'>='", "'>'"
    };
    public static final int LETTER=75;
    public static final int K_CREATE=32;
    public static final int EOF=-1;
    public static final int K_PRIMARY=37;
    public static final int T__93=93;
    public static final int T__94=94;
    public static final int T__91=91;
    public static final int K_USE=4;
    public static final int T__92=92;
    public static final int K_VALUES=23;
    public static final int STRING_LITERAL=9;
    public static final int T__90=90;
    public static final int K_ON=41;
    public static final int K_USING=11;
    public static final int K_ADD=45;
    public static final int K_KEY=38;
    public static final int COMMENT=78;
    public static final int K_TRUNCATE=47;
    public static final int T__99=99;
    public static final int T__98=98;
    public static final int T__97=97;
    public static final int T__96=96;
    public static final int T__95=95;
    public static final int D=61;
    public static final int E=49;
    public static final int F=53;
    public static final int G=67;
    public static final int K_COUNT=7;
    public static final int T__80=80;
    public static final int K_KEYSPACE=33;
    public static final int K_TYPE=44;
    public static final int T__81=81;
    public static final int A=59;
    public static final int B=70;
    public static final int T__82=82;
    public static final int T__83=83;
    public static final int C=51;
    public static final int L=50;
    public static final int M=56;
    public static final int N=60;
    public static final int O=55;
    public static final int H=58;
    public static final int I=64;
    public static final int J=72;
    public static final int K_UPDATE=29;
    public static final int K=62;
    public static final int U=65;
    public static final int T=52;
    public static final int W=57;
    public static final int V=69;
    public static final int Q=68;
    public static final int P=66;
    public static final int S=48;
    public static final int R=54;
    public static final int T__85=85;
    public static final int T__84=84;
    public static final int T__87=87;
    public static final int T__86=86;
    public static final int K_TTL=25;
    public static final int T__89=89;
    public static final int Y=63;
    public static final int X=71;
    public static final int T__88=88;
    public static final int Z=73;
    public static final int K_INDEX=40;
    public static final int K_REVERSED=17;
    public static final int K_INSERT=21;
    public static final int WS=77;
    public static final int K_APPLY=28;
    public static final int K_TIMESTAMP=24;
    public static final int K_AND=19;
    public static final int K_LEVEL=13;
    public static final int K_BATCH=27;
    public static final int UUID=46;
    public static final int K_DELETE=31;
    public static final int FLOAT=39;
    public static final int K_SELECT=6;
    public static final int K_LIMIT=15;
    public static final int K_ALTER=43;
    public static final int K_SET=30;
    public static final int K_WHERE=14;
    public static final int MULTILINE_COMMENT=79;
    public static final int HEX=76;
    public static final int K_INTO=22;
    public static final int T__103=103;
    public static final int T__104=104;
    public static final int IDENT=5;
    public static final int DIGIT=74;
    public static final int K_FIRST=16;
    public static final int K_BEGIN=26;
    public static final int INTEGER=10;
    public static final int RANGEOP=18;
    public static final int K_CONSISTENCY=12;
    public static final int K_WITH=34;
    public static final int COMPIDENT=35;
    public static final int T__102=102;
    public static final int T__101=101;
    public static final int T__100=100;
    public static final int K_IN=20;
    public static final int K_FROM=8;
    public static final int K_COLUMNFAMILY=36;
    public static final int K_DROP=42;

    // delegates
    // delegators


        public CqlParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public CqlParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        

    public String[] getTokenNames() { return CqlParser.tokenNames; }
    public String getGrammarFileName() { return "/home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g"; }


        private List<String> recognitionErrors = new ArrayList<String>();
        
        public void displayRecognitionError(String[] tokenNames, RecognitionException e)
        {
            String hdr = getErrorHeader(e);
            String msg = getErrorMessage(e, tokenNames);
            recognitionErrors.add(hdr + " " + msg);
        }
        
        public List<String> getRecognitionErrors()
        {
            return recognitionErrors;
        }
        
        public void throwLastRecognitionError() throws InvalidRequestException
        {
            if (recognitionErrors.size() > 0)
                throw new InvalidRequestException(recognitionErrors.get((recognitionErrors.size()-1)));
        }



    // $ANTLR start "query"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:105:1: query returns [CQLStatement stmnt] : ( selectStatement | insertStatement endStmnt | updateStatement endStmnt | batchStatement | useStatement | truncateStatement | deleteStatement endStmnt | createKeyspaceStatement | createColumnFamilyStatement | createIndexStatement | dropIndexStatement | dropKeyspaceStatement | dropColumnFamilyStatement | alterTableStatement );
    public final CQLStatement query() throws RecognitionException {
        CQLStatement stmnt = null;

        SelectStatement selectStatement1 = null;

        UpdateStatement insertStatement2 = null;

        UpdateStatement updateStatement3 = null;

        BatchStatement batchStatement4 = null;

        String useStatement5 = null;

        String truncateStatement6 = null;

        DeleteStatement deleteStatement7 = null;

        CreateKeyspaceStatement createKeyspaceStatement8 = null;

        CreateColumnFamilyStatement createColumnFamilyStatement9 = null;

        CreateIndexStatement createIndexStatement10 = null;

        DropIndexStatement dropIndexStatement11 = null;

        String dropKeyspaceStatement12 = null;

        String dropColumnFamilyStatement13 = null;

        AlterTableStatement alterTableStatement14 = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:106:5: ( selectStatement | insertStatement endStmnt | updateStatement endStmnt | batchStatement | useStatement | truncateStatement | deleteStatement endStmnt | createKeyspaceStatement | createColumnFamilyStatement | createIndexStatement | dropIndexStatement | dropKeyspaceStatement | dropColumnFamilyStatement | alterTableStatement )
            int alt1=14;
            alt1 = dfa1.predict(input);
            switch (alt1) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:106:7: selectStatement
                    {
                    pushFollow(FOLLOW_selectStatement_in_query69);
                    selectStatement1=selectStatement();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.SELECT, selectStatement1); 

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:107:7: insertStatement endStmnt
                    {
                    pushFollow(FOLLOW_insertStatement_in_query81);
                    insertStatement2=insertStatement();

                    state._fsp--;

                    pushFollow(FOLLOW_endStmnt_in_query83);
                    endStmnt();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.INSERT, insertStatement2); 

                    }
                    break;
                case 3 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:108:7: updateStatement endStmnt
                    {
                    pushFollow(FOLLOW_updateStatement_in_query93);
                    updateStatement3=updateStatement();

                    state._fsp--;

                    pushFollow(FOLLOW_endStmnt_in_query95);
                    endStmnt();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.UPDATE, updateStatement3); 

                    }
                    break;
                case 4 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:109:7: batchStatement
                    {
                    pushFollow(FOLLOW_batchStatement_in_query105);
                    batchStatement4=batchStatement();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.BATCH, batchStatement4); 

                    }
                    break;
                case 5 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:110:7: useStatement
                    {
                    pushFollow(FOLLOW_useStatement_in_query115);
                    useStatement5=useStatement();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.USE, useStatement5); 

                    }
                    break;
                case 6 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:111:7: truncateStatement
                    {
                    pushFollow(FOLLOW_truncateStatement_in_query130);
                    truncateStatement6=truncateStatement();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.TRUNCATE, truncateStatement6); 

                    }
                    break;
                case 7 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:112:7: deleteStatement endStmnt
                    {
                    pushFollow(FOLLOW_deleteStatement_in_query140);
                    deleteStatement7=deleteStatement();

                    state._fsp--;

                    pushFollow(FOLLOW_endStmnt_in_query142);
                    endStmnt();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.DELETE, deleteStatement7); 

                    }
                    break;
                case 8 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:113:7: createKeyspaceStatement
                    {
                    pushFollow(FOLLOW_createKeyspaceStatement_in_query152);
                    createKeyspaceStatement8=createKeyspaceStatement();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.CREATE_KEYSPACE, createKeyspaceStatement8); 

                    }
                    break;
                case 9 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:114:7: createColumnFamilyStatement
                    {
                    pushFollow(FOLLOW_createColumnFamilyStatement_in_query162);
                    createColumnFamilyStatement9=createColumnFamilyStatement();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.CREATE_COLUMNFAMILY, createColumnFamilyStatement9); 

                    }
                    break;
                case 10 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:115:7: createIndexStatement
                    {
                    pushFollow(FOLLOW_createIndexStatement_in_query172);
                    createIndexStatement10=createIndexStatement();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.CREATE_INDEX, createIndexStatement10); 

                    }
                    break;
                case 11 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:116:7: dropIndexStatement
                    {
                    pushFollow(FOLLOW_dropIndexStatement_in_query182);
                    dropIndexStatement11=dropIndexStatement();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.DROP_INDEX, dropIndexStatement11); 

                    }
                    break;
                case 12 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:117:7: dropKeyspaceStatement
                    {
                    pushFollow(FOLLOW_dropKeyspaceStatement_in_query194);
                    dropKeyspaceStatement12=dropKeyspaceStatement();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.DROP_KEYSPACE, dropKeyspaceStatement12); 

                    }
                    break;
                case 13 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:118:7: dropColumnFamilyStatement
                    {
                    pushFollow(FOLLOW_dropColumnFamilyStatement_in_query204);
                    dropColumnFamilyStatement13=dropColumnFamilyStatement();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.DROP_COLUMNFAMILY, dropColumnFamilyStatement13); 

                    }
                    break;
                case 14 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:119:7: alterTableStatement
                    {
                    pushFollow(FOLLOW_alterTableStatement_in_query214);
                    alterTableStatement14=alterTableStatement();

                    state._fsp--;

                     stmnt = new CQLStatement(StatementType.ALTER_TABLE, alterTableStatement14); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return stmnt;
    }
    // $ANTLR end "query"


    // $ANTLR start "useStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:123:1: useStatement returns [String keyspace] : K_USE IDENT endStmnt ;
    public final String useStatement() throws RecognitionException {
        String keyspace = null;

        Token IDENT15=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:124:5: ( K_USE IDENT endStmnt )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:124:7: K_USE IDENT endStmnt
            {
            match(input,K_USE,FOLLOW_K_USE_in_useStatement238); 
            IDENT15=(Token)match(input,IDENT,FOLLOW_IDENT_in_useStatement240); 
             keyspace = (IDENT15!=null?IDENT15.getText():null); 
            pushFollow(FOLLOW_endStmnt_in_useStatement244);
            endStmnt();

            state._fsp--;


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return keyspace;
    }
    // $ANTLR end "useStatement"


    // $ANTLR start "selectStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:127:1: selectStatement returns [SelectStatement expr] : K_SELECT (s1= selectExpression | K_COUNT '(' s2= selectExpression ')' ) K_FROM columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( K_USING K_CONSISTENCY K_LEVEL )? ( K_WHERE whereClause )? ( K_LIMIT rows= INTEGER )? endStmnt ;
    public final SelectStatement selectStatement() throws RecognitionException {
        SelectStatement expr = null;

        Token columnFamily=null;
        Token rows=null;
        Token K_LEVEL16=null;
        SelectExpression s1 = null;

        SelectExpression s2 = null;

        WhereClause whereClause17 = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:140:5: ( K_SELECT (s1= selectExpression | K_COUNT '(' s2= selectExpression ')' ) K_FROM columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( K_USING K_CONSISTENCY K_LEVEL )? ( K_WHERE whereClause )? ( K_LIMIT rows= INTEGER )? endStmnt )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:140:7: K_SELECT (s1= selectExpression | K_COUNT '(' s2= selectExpression ')' ) K_FROM columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( K_USING K_CONSISTENCY K_LEVEL )? ( K_WHERE whereClause )? ( K_LIMIT rows= INTEGER )? endStmnt
            {
             
                      int numRecords = 10000;
                      SelectExpression expression = null;
                      boolean isCountOp = false;
                      ConsistencyLevel cLevel = ConsistencyLevel.ONE;
                  
            match(input,K_SELECT,FOLLOW_K_SELECT_in_selectStatement275); 
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:147:11: (s1= selectExpression | K_COUNT '(' s2= selectExpression ')' )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==IDENT||(LA2_0>=STRING_LITERAL && LA2_0<=INTEGER)||(LA2_0>=K_FIRST && LA2_0<=K_REVERSED)||(LA2_0>=K_KEY && LA2_0<=FLOAT)||LA2_0==UUID||LA2_0==83) ) {
                alt2=1;
            }
            else if ( (LA2_0==K_COUNT) ) {
                alt2=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }
            switch (alt2) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:147:13: s1= selectExpression
                    {
                    pushFollow(FOLLOW_selectExpression_in_selectStatement291);
                    s1=selectExpression();

                    state._fsp--;

                     expression = s1; 

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:148:13: K_COUNT '(' s2= selectExpression ')'
                    {
                    match(input,K_COUNT,FOLLOW_K_COUNT_in_selectStatement323); 
                    match(input,80,FOLLOW_80_in_selectStatement325); 
                    pushFollow(FOLLOW_selectExpression_in_selectStatement329);
                    s2=selectExpression();

                    state._fsp--;

                    match(input,81,FOLLOW_81_in_selectStatement331); 
                     expression = s2; isCountOp = true; 

                    }
                    break;

            }

            match(input,K_FROM,FOLLOW_K_FROM_in_selectStatement357); 
            columnFamily=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:151:11: ( K_USING K_CONSISTENCY K_LEVEL )?
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==K_USING) ) {
                alt3=1;
            }
            switch (alt3) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:151:13: K_USING K_CONSISTENCY K_LEVEL
                    {
                    match(input,K_USING,FOLLOW_K_USING_in_selectStatement387); 
                    match(input,K_CONSISTENCY,FOLLOW_K_CONSISTENCY_in_selectStatement389); 
                    K_LEVEL16=(Token)match(input,K_LEVEL,FOLLOW_K_LEVEL_in_selectStatement391); 
                     cLevel = ConsistencyLevel.valueOf((K_LEVEL16!=null?K_LEVEL16.getText():null)); 

                    }
                    break;

            }

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:152:11: ( K_WHERE whereClause )?
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==K_WHERE) ) {
                alt4=1;
            }
            switch (alt4) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:152:13: K_WHERE whereClause
                    {
                    match(input,K_WHERE,FOLLOW_K_WHERE_in_selectStatement410); 
                    pushFollow(FOLLOW_whereClause_in_selectStatement412);
                    whereClause17=whereClause();

                    state._fsp--;


                    }
                    break;

            }

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:153:11: ( K_LIMIT rows= INTEGER )?
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==K_LIMIT) ) {
                alt5=1;
            }
            switch (alt5) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:153:13: K_LIMIT rows= INTEGER
                    {
                    match(input,K_LIMIT,FOLLOW_K_LIMIT_in_selectStatement429); 
                    rows=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_selectStatement433); 
                     numRecords = Integer.parseInt((rows!=null?rows.getText():null)); 

                    }
                    break;

            }

            pushFollow(FOLLOW_endStmnt_in_selectStatement450);
            endStmnt();

            state._fsp--;


                      return new SelectStatement(expression,
                                                 isCountOp,
                                                 (columnFamily!=null?columnFamily.getText():null),
                                                 cLevel,
                                                 whereClause17,
                                                 numRecords);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "selectStatement"


    // $ANTLR start "selectExpression"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:167:1: selectExpression returns [SelectExpression expr] : ( K_FIRST cols= INTEGER )? ( K_REVERSED )? (first= term ( ',' next= term )* | start= term RANGEOP finish= term | '\\*' ) ;
    public final SelectExpression selectExpression() throws RecognitionException {
        SelectExpression expr = null;

        Token cols=null;
        Term first = null;

        Term next = null;

        Term start = null;

        Term finish = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:168:5: ( ( K_FIRST cols= INTEGER )? ( K_REVERSED )? (first= term ( ',' next= term )* | start= term RANGEOP finish= term | '\\*' ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:168:7: ( K_FIRST cols= INTEGER )? ( K_REVERSED )? (first= term ( ',' next= term )* | start= term RANGEOP finish= term | '\\*' )
            {

                      int count = 10000;
                      boolean reversed = false;
                  
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:172:7: ( K_FIRST cols= INTEGER )?
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0==K_FIRST) ) {
                alt6=1;
            }
            switch (alt6) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:172:9: K_FIRST cols= INTEGER
                    {
                    match(input,K_FIRST,FOLLOW_K_FIRST_in_selectExpression491); 
                    cols=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_selectExpression495); 
                     count = Integer.parseInt((cols!=null?cols.getText():null)); 

                    }
                    break;

            }

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:173:7: ( K_REVERSED )?
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==K_REVERSED) ) {
                alt7=1;
            }
            switch (alt7) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:173:9: K_REVERSED
                    {
                    match(input,K_REVERSED,FOLLOW_K_REVERSED_in_selectExpression510); 
                     reversed = true; 

                    }
                    break;

            }

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:174:7: (first= term ( ',' next= term )* | start= term RANGEOP finish= term | '\\*' )
            int alt9=3;
            alt9 = dfa9.predict(input);
            switch (alt9) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:174:9: first= term ( ',' next= term )*
                    {
                    pushFollow(FOLLOW_term_in_selectExpression527);
                    first=term();

                    state._fsp--;

                     expr = new SelectExpression(first, count, reversed); 
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:175:13: ( ',' next= term )*
                    loop8:
                    do {
                        int alt8=2;
                        int LA8_0 = input.LA(1);

                        if ( (LA8_0==82) ) {
                            alt8=1;
                        }


                        switch (alt8) {
                    	case 1 :
                    	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:175:14: ',' next= term
                    	    {
                    	    match(input,82,FOLLOW_82_in_selectExpression544); 
                    	    pushFollow(FOLLOW_term_in_selectExpression548);
                    	    next=term();

                    	    state._fsp--;

                    	     expr.and(next); 

                    	    }
                    	    break;

                    	default :
                    	    break loop8;
                        }
                    } while (true);


                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:176:9: start= term RANGEOP finish= term
                    {
                    pushFollow(FOLLOW_term_in_selectExpression564);
                    start=term();

                    state._fsp--;

                    match(input,RANGEOP,FOLLOW_RANGEOP_in_selectExpression566); 
                    pushFollow(FOLLOW_term_in_selectExpression570);
                    finish=term();

                    state._fsp--;

                     expr = new SelectExpression(start, finish, count, reversed, false); 

                    }
                    break;
                case 3 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:177:9: '\\*'
                    {
                    match(input,83,FOLLOW_83_in_selectExpression582); 
                     expr = new SelectExpression(new Term(), new Term(), count, reversed, true); 

                    }
                    break;

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "selectExpression"


    // $ANTLR start "whereClause"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:182:1: whereClause returns [WhereClause clause] : (first= relation ( K_AND next= relation )* | key_alias= term K_IN '(' f1= term ( ',' fN= term )* ')' );
    public final WhereClause whereClause() throws RecognitionException {
        WhereClause clause = null;

        Relation first = null;

        Relation next = null;

        Term key_alias = null;

        Term f1 = null;

        Term fN = null;



                WhereClause inClause = new WhereClause();
            
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:186:5: (first= relation ( K_AND next= relation )* | key_alias= term K_IN '(' f1= term ( ',' fN= term )* ')' )
            int alt12=2;
            switch ( input.LA(1) ) {
            case K_KEY:
                {
                int LA12_1 = input.LA(2);

                if ( (LA12_1==K_IN) ) {
                    alt12=2;
                }
                else if ( (LA12_1==85||(LA12_1>=101 && LA12_1<=104)) ) {
                    alt12=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 12, 1, input);

                    throw nvae;
                }
                }
                break;
            case STRING_LITERAL:
                {
                int LA12_2 = input.LA(2);

                if ( (LA12_2==K_IN) ) {
                    alt12=2;
                }
                else if ( (LA12_2==85||(LA12_2>=101 && LA12_2<=104)) ) {
                    alt12=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 12, 2, input);

                    throw nvae;
                }
                }
                break;
            case INTEGER:
                {
                int LA12_3 = input.LA(2);

                if ( (LA12_3==K_IN) ) {
                    alt12=2;
                }
                else if ( (LA12_3==85||(LA12_3>=101 && LA12_3<=104)) ) {
                    alt12=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 12, 3, input);

                    throw nvae;
                }
                }
                break;
            case UUID:
                {
                int LA12_4 = input.LA(2);

                if ( (LA12_4==K_IN) ) {
                    alt12=2;
                }
                else if ( (LA12_4==85||(LA12_4>=101 && LA12_4<=104)) ) {
                    alt12=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 12, 4, input);

                    throw nvae;
                }
                }
                break;
            case IDENT:
                {
                int LA12_5 = input.LA(2);

                if ( (LA12_5==K_IN) ) {
                    alt12=2;
                }
                else if ( (LA12_5==85||(LA12_5>=101 && LA12_5<=104)) ) {
                    alt12=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 12, 5, input);

                    throw nvae;
                }
                }
                break;
            case FLOAT:
                {
                int LA12_6 = input.LA(2);

                if ( (LA12_6==85||(LA12_6>=101 && LA12_6<=104)) ) {
                    alt12=1;
                }
                else if ( (LA12_6==K_IN) ) {
                    alt12=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 12, 6, input);

                    throw nvae;
                }
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 12, 0, input);

                throw nvae;
            }

            switch (alt12) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:186:7: first= relation ( K_AND next= relation )*
                    {
                    pushFollow(FOLLOW_relation_in_whereClause625);
                    first=relation();

                    state._fsp--;

                     clause = new WhereClause(first); 
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:187:11: ( K_AND next= relation )*
                    loop10:
                    do {
                        int alt10=2;
                        int LA10_0 = input.LA(1);

                        if ( (LA10_0==K_AND) ) {
                            alt10=1;
                        }


                        switch (alt10) {
                    	case 1 :
                    	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:187:12: K_AND next= relation
                    	    {
                    	    match(input,K_AND,FOLLOW_K_AND_in_whereClause641); 
                    	    pushFollow(FOLLOW_relation_in_whereClause645);
                    	    next=relation();

                    	    state._fsp--;

                    	     clause.and(next); 

                    	    }
                    	    break;

                    	default :
                    	    break loop10;
                        }
                    } while (true);


                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:188:9: key_alias= term K_IN '(' f1= term ( ',' fN= term )* ')'
                    {
                    pushFollow(FOLLOW_term_in_whereClause661);
                    key_alias=term();

                    state._fsp--;

                     inClause.setKeyAlias(key_alias.getText()); 
                    match(input,K_IN,FOLLOW_K_IN_in_whereClause676); 
                    match(input,80,FOLLOW_80_in_whereClause678); 
                    pushFollow(FOLLOW_term_in_whereClause682);
                    f1=term();

                    state._fsp--;

                     inClause.andKeyEquals(f1); 
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:190:19: ( ',' fN= term )*
                    loop11:
                    do {
                        int alt11=2;
                        int LA11_0 = input.LA(1);

                        if ( (LA11_0==82) ) {
                            alt11=1;
                        }


                        switch (alt11) {
                    	case 1 :
                    	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:190:20: ',' fN= term
                    	    {
                    	    match(input,82,FOLLOW_82_in_whereClause705); 
                    	    pushFollow(FOLLOW_term_in_whereClause709);
                    	    fN=term();

                    	    state._fsp--;

                    	     inClause.andKeyEquals(fN); 

                    	    }
                    	    break;

                    	default :
                    	    break loop11;
                        }
                    } while (true);

                    match(input,81,FOLLOW_81_in_whereClause716); 
                     inClause.setMultiKey(true); clause = inClause; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return clause;
    }
    // $ANTLR end "whereClause"


    // $ANTLR start "insertStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:194:1: insertStatement returns [UpdateStatement expr] : K_INSERT K_INTO columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) '(' key_alias= term ( ',' column_name= term )+ ')' K_VALUES '(' key= term ( ',' column_value= term )+ ')' ( usingClause[attrs] )? ;
    public final UpdateStatement insertStatement() throws RecognitionException {
        UpdateStatement expr = null;

        Token columnFamily=null;
        Term key_alias = null;

        Term column_name = null;

        Term key = null;

        Term column_value = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:208:5: ( K_INSERT K_INTO columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) '(' key_alias= term ( ',' column_name= term )+ ')' K_VALUES '(' key= term ( ',' column_value= term )+ ')' ( usingClause[attrs] )? )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:208:7: K_INSERT K_INTO columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) '(' key_alias= term ( ',' column_name= term )+ ')' K_VALUES '(' key= term ( ',' column_value= term )+ ')' ( usingClause[attrs] )?
            {

                      Attributes attrs = new Attributes();
                      Map<Term, Term> columns = new HashMap<Term, Term>();

                      List<Term> columnNames  = new ArrayList<Term>();
                      List<Term> columnValues = new ArrayList<Term>();
                  
            match(input,K_INSERT,FOLLOW_K_INSERT_in_insertStatement757); 
            match(input,K_INTO,FOLLOW_K_INTO_in_insertStatement759); 
            columnFamily=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            match(input,80,FOLLOW_80_in_insertStatement787); 
            pushFollow(FOLLOW_term_in_insertStatement791);
            key_alias=term();

            state._fsp--;

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:216:30: ( ',' column_name= term )+
            int cnt13=0;
            loop13:
            do {
                int alt13=2;
                int LA13_0 = input.LA(1);

                if ( (LA13_0==82) ) {
                    alt13=1;
                }


                switch (alt13) {
            	case 1 :
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:216:32: ',' column_name= term
            	    {
            	    match(input,82,FOLLOW_82_in_insertStatement795); 
            	    pushFollow(FOLLOW_term_in_insertStatement799);
            	    column_name=term();

            	    state._fsp--;

            	     columnNames.add(column_name); 

            	    }
            	    break;

            	default :
            	    if ( cnt13 >= 1 ) break loop13;
                        EarlyExitException eee =
                            new EarlyExitException(13, input);
                        throw eee;
                }
                cnt13++;
            } while (true);

            match(input,81,FOLLOW_81_in_insertStatement807); 
            match(input,K_VALUES,FOLLOW_K_VALUES_in_insertStatement817); 
            match(input,80,FOLLOW_80_in_insertStatement829); 
            pushFollow(FOLLOW_term_in_insertStatement833);
            key=term();

            state._fsp--;

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:218:24: ( ',' column_value= term )+
            int cnt14=0;
            loop14:
            do {
                int alt14=2;
                int LA14_0 = input.LA(1);

                if ( (LA14_0==82) ) {
                    alt14=1;
                }


                switch (alt14) {
            	case 1 :
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:218:26: ',' column_value= term
            	    {
            	    match(input,82,FOLLOW_82_in_insertStatement837); 
            	    pushFollow(FOLLOW_term_in_insertStatement841);
            	    column_value=term();

            	    state._fsp--;

            	     columnValues.add(column_value); 

            	    }
            	    break;

            	default :
            	    if ( cnt14 >= 1 ) break loop14;
                        EarlyExitException eee =
                            new EarlyExitException(14, input);
                        throw eee;
                }
                cnt14++;
            } while (true);

            match(input,81,FOLLOW_81_in_insertStatement847); 
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:219:9: ( usingClause[attrs] )?
            int alt15=2;
            int LA15_0 = input.LA(1);

            if ( (LA15_0==K_USING) ) {
                alt15=1;
            }
            switch (alt15) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:219:11: usingClause[attrs]
                    {
                    pushFollow(FOLLOW_usingClause_in_insertStatement859);
                    usingClause(attrs);

                    state._fsp--;


                    }
                    break;

            }


                      return new UpdateStatement((columnFamily!=null?columnFamily.getText():null), key_alias.getText(), columnNames, columnValues, Collections.singletonList(key), attrs);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "insertStatement"


    // $ANTLR start "usingClause"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:225:1: usingClause[Attributes attrs] : K_USING usingClauseObjective[attrs] ( ( K_AND )? usingClauseObjective[attrs] )* ;
    public final void usingClause(Attributes attrs) throws RecognitionException {
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:226:5: ( K_USING usingClauseObjective[attrs] ( ( K_AND )? usingClauseObjective[attrs] )* )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:226:7: K_USING usingClauseObjective[attrs] ( ( K_AND )? usingClauseObjective[attrs] )*
            {
            match(input,K_USING,FOLLOW_K_USING_in_usingClause889); 
            pushFollow(FOLLOW_usingClauseObjective_in_usingClause891);
            usingClauseObjective(attrs);

            state._fsp--;

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:226:43: ( ( K_AND )? usingClauseObjective[attrs] )*
            loop17:
            do {
                int alt17=2;
                int LA17_0 = input.LA(1);

                if ( (LA17_0==K_CONSISTENCY||LA17_0==K_AND||(LA17_0>=K_TIMESTAMP && LA17_0<=K_TTL)) ) {
                    alt17=1;
                }


                switch (alt17) {
            	case 1 :
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:226:45: ( K_AND )? usingClauseObjective[attrs]
            	    {
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:226:45: ( K_AND )?
            	    int alt16=2;
            	    int LA16_0 = input.LA(1);

            	    if ( (LA16_0==K_AND) ) {
            	        alt16=1;
            	    }
            	    switch (alt16) {
            	        case 1 :
            	            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:226:45: K_AND
            	            {
            	            match(input,K_AND,FOLLOW_K_AND_in_usingClause896); 

            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_usingClauseObjective_in_usingClause899);
            	    usingClauseObjective(attrs);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop17;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "usingClause"


    // $ANTLR start "usingClauseDelete"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:229:1: usingClauseDelete[Attributes attrs] : K_USING usingClauseDeleteObjective[attrs] ( ( K_AND )? usingClauseDeleteObjective[attrs] )* ;
    public final void usingClauseDelete(Attributes attrs) throws RecognitionException {
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:230:5: ( K_USING usingClauseDeleteObjective[attrs] ( ( K_AND )? usingClauseDeleteObjective[attrs] )* )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:230:7: K_USING usingClauseDeleteObjective[attrs] ( ( K_AND )? usingClauseDeleteObjective[attrs] )*
            {
            match(input,K_USING,FOLLOW_K_USING_in_usingClauseDelete921); 
            pushFollow(FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete923);
            usingClauseDeleteObjective(attrs);

            state._fsp--;

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:230:49: ( ( K_AND )? usingClauseDeleteObjective[attrs] )*
            loop19:
            do {
                int alt19=2;
                int LA19_0 = input.LA(1);

                if ( (LA19_0==K_CONSISTENCY||LA19_0==K_AND||LA19_0==K_TIMESTAMP) ) {
                    alt19=1;
                }


                switch (alt19) {
            	case 1 :
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:230:51: ( K_AND )? usingClauseDeleteObjective[attrs]
            	    {
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:230:51: ( K_AND )?
            	    int alt18=2;
            	    int LA18_0 = input.LA(1);

            	    if ( (LA18_0==K_AND) ) {
            	        alt18=1;
            	    }
            	    switch (alt18) {
            	        case 1 :
            	            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:230:51: K_AND
            	            {
            	            match(input,K_AND,FOLLOW_K_AND_in_usingClauseDelete928); 

            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete931);
            	    usingClauseDeleteObjective(attrs);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop19;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "usingClauseDelete"


    // $ANTLR start "usingClauseDeleteObjective"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:233:1: usingClauseDeleteObjective[Attributes attrs] : ( K_CONSISTENCY K_LEVEL | K_TIMESTAMP ts= INTEGER );
    public final void usingClauseDeleteObjective(Attributes attrs) throws RecognitionException {
        Token ts=null;
        Token K_LEVEL18=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:234:5: ( K_CONSISTENCY K_LEVEL | K_TIMESTAMP ts= INTEGER )
            int alt20=2;
            int LA20_0 = input.LA(1);

            if ( (LA20_0==K_CONSISTENCY) ) {
                alt20=1;
            }
            else if ( (LA20_0==K_TIMESTAMP) ) {
                alt20=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 20, 0, input);

                throw nvae;
            }
            switch (alt20) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:234:7: K_CONSISTENCY K_LEVEL
                    {
                    match(input,K_CONSISTENCY,FOLLOW_K_CONSISTENCY_in_usingClauseDeleteObjective953); 
                    K_LEVEL18=(Token)match(input,K_LEVEL,FOLLOW_K_LEVEL_in_usingClauseDeleteObjective955); 
                     attrs.setConsistencyLevel(ConsistencyLevel.valueOf((K_LEVEL18!=null?K_LEVEL18.getText():null))); 

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:235:7: K_TIMESTAMP ts= INTEGER
                    {
                    match(input,K_TIMESTAMP,FOLLOW_K_TIMESTAMP_in_usingClauseDeleteObjective966); 
                    ts=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_usingClauseDeleteObjective970); 
                     attrs.setTimestamp(Long.valueOf((ts!=null?ts.getText():null))); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "usingClauseDeleteObjective"


    // $ANTLR start "usingClauseObjective"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:238:1: usingClauseObjective[Attributes attrs] : ( usingClauseDeleteObjective[attrs] | K_TTL t= INTEGER );
    public final void usingClauseObjective(Attributes attrs) throws RecognitionException {
        Token t=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:239:5: ( usingClauseDeleteObjective[attrs] | K_TTL t= INTEGER )
            int alt21=2;
            int LA21_0 = input.LA(1);

            if ( (LA21_0==K_CONSISTENCY||LA21_0==K_TIMESTAMP) ) {
                alt21=1;
            }
            else if ( (LA21_0==K_TTL) ) {
                alt21=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 21, 0, input);

                throw nvae;
            }
            switch (alt21) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:239:7: usingClauseDeleteObjective[attrs]
                    {
                    pushFollow(FOLLOW_usingClauseDeleteObjective_in_usingClauseObjective990);
                    usingClauseDeleteObjective(attrs);

                    state._fsp--;


                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:240:7: K_TTL t= INTEGER
                    {
                    match(input,K_TTL,FOLLOW_K_TTL_in_usingClauseObjective999); 
                    t=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_usingClauseObjective1003); 
                     attrs.setTimeToLive(Integer.parseInt((t!=null?t.getText():null))); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "usingClauseObjective"


    // $ANTLR start "batchStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:243:1: batchStatement returns [BatchStatement expr] : K_BEGIN K_BATCH ( usingClause[attrs] )? s1= batchStatementObjective ( ';' )? (sN= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH endStmnt ;
    public final BatchStatement batchStatement() throws RecognitionException {
        BatchStatement expr = null;

        AbstractModification s1 = null;

        AbstractModification sN = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:268:5: ( K_BEGIN K_BATCH ( usingClause[attrs] )? s1= batchStatementObjective ( ';' )? (sN= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH endStmnt )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:268:7: K_BEGIN K_BATCH ( usingClause[attrs] )? s1= batchStatementObjective ( ';' )? (sN= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH endStmnt
            {

                      Attributes attrs = new Attributes();
                      attrs.setConsistencyLevel(ConsistencyLevel.ONE);

                      List<AbstractModification> statements = new ArrayList<AbstractModification>();
                  
            match(input,K_BEGIN,FOLLOW_K_BEGIN_in_batchStatement1043); 
            match(input,K_BATCH,FOLLOW_K_BATCH_in_batchStatement1045); 
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:274:23: ( usingClause[attrs] )?
            int alt22=2;
            int LA22_0 = input.LA(1);

            if ( (LA22_0==K_USING) ) {
                alt22=1;
            }
            switch (alt22) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:274:25: usingClause[attrs]
                    {
                    pushFollow(FOLLOW_usingClause_in_batchStatement1049);
                    usingClause(attrs);

                    state._fsp--;


                    }
                    break;

            }

            pushFollow(FOLLOW_batchStatementObjective_in_batchStatement1067);
            s1=batchStatementObjective();

            state._fsp--;

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:275:38: ( ';' )?
            int alt23=2;
            int LA23_0 = input.LA(1);

            if ( (LA23_0==84) ) {
                alt23=1;
            }
            switch (alt23) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:275:38: ';'
                    {
                    match(input,84,FOLLOW_84_in_batchStatement1069); 

                    }
                    break;

            }

             statements.add(s1); 
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:275:67: (sN= batchStatementObjective ( ';' )? )*
            loop25:
            do {
                int alt25=2;
                int LA25_0 = input.LA(1);

                if ( (LA25_0==K_INSERT||LA25_0==K_UPDATE||LA25_0==K_DELETE) ) {
                    alt25=1;
                }


                switch (alt25) {
            	case 1 :
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:275:69: sN= batchStatementObjective ( ';' )?
            	    {
            	    pushFollow(FOLLOW_batchStatementObjective_in_batchStatement1078);
            	    sN=batchStatementObjective();

            	    state._fsp--;

            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:275:96: ( ';' )?
            	    int alt24=2;
            	    int LA24_0 = input.LA(1);

            	    if ( (LA24_0==84) ) {
            	        alt24=1;
            	    }
            	    switch (alt24) {
            	        case 1 :
            	            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:275:96: ';'
            	            {
            	            match(input,84,FOLLOW_84_in_batchStatement1080); 

            	            }
            	            break;

            	    }

            	     statements.add(sN); 

            	    }
            	    break;

            	default :
            	    break loop25;
                }
            } while (true);

            match(input,K_APPLY,FOLLOW_K_APPLY_in_batchStatement1094); 
            match(input,K_BATCH,FOLLOW_K_BATCH_in_batchStatement1096); 
            pushFollow(FOLLOW_endStmnt_in_batchStatement1098);
            endStmnt();

            state._fsp--;


                      return new BatchStatement(statements, attrs);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "batchStatement"


    // $ANTLR start "batchStatementObjective"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:282:1: batchStatementObjective returns [AbstractModification statement] : (i= insertStatement | u= updateStatement | d= deleteStatement );
    public final AbstractModification batchStatementObjective() throws RecognitionException {
        AbstractModification statement = null;

        UpdateStatement i = null;

        UpdateStatement u = null;

        DeleteStatement d = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:283:5: (i= insertStatement | u= updateStatement | d= deleteStatement )
            int alt26=3;
            switch ( input.LA(1) ) {
            case K_INSERT:
                {
                alt26=1;
                }
                break;
            case K_UPDATE:
                {
                alt26=2;
                }
                break;
            case K_DELETE:
                {
                alt26=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 26, 0, input);

                throw nvae;
            }

            switch (alt26) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:283:7: i= insertStatement
                    {
                    pushFollow(FOLLOW_insertStatement_in_batchStatementObjective1129);
                    i=insertStatement();

                    state._fsp--;

                     statement = i; 

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:284:7: u= updateStatement
                    {
                    pushFollow(FOLLOW_updateStatement_in_batchStatementObjective1142);
                    u=updateStatement();

                    state._fsp--;

                     statement = u; 

                    }
                    break;
                case 3 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:285:7: d= deleteStatement
                    {
                    pushFollow(FOLLOW_deleteStatement_in_batchStatementObjective1155);
                    d=deleteStatement();

                    state._fsp--;

                     statement = d; 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return statement;
    }
    // $ANTLR end "batchStatementObjective"


    // $ANTLR start "updateStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:288:1: updateStatement returns [UpdateStatement expr] : K_UPDATE columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( usingClause[attrs] )? K_SET termPairWithOperation[columns] ( ',' termPairWithOperation[columns] )* K_WHERE (key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) ) ;
    public final UpdateStatement updateStatement() throws RecognitionException {
        UpdateStatement expr = null;

        Token columnFamily=null;
        Term key_alias = null;

        Term key = null;

        List<Term> keys = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:302:5: ( K_UPDATE columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( usingClause[attrs] )? K_SET termPairWithOperation[columns] ( ',' termPairWithOperation[columns] )* K_WHERE (key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:302:7: K_UPDATE columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( usingClause[attrs] )? K_SET termPairWithOperation[columns] ( ',' termPairWithOperation[columns] )* K_WHERE (key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) )
            {

                      Attributes attrs = new Attributes();
                      Map<Term, Operation> columns = new HashMap<Term, Operation>();
                      List<Term> keyList = null;
                  
            match(input,K_UPDATE,FOLLOW_K_UPDATE_in_updateStatement1189); 
            columnFamily=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:308:11: ( usingClause[attrs] )?
            int alt27=2;
            int LA27_0 = input.LA(1);

            if ( (LA27_0==K_USING) ) {
                alt27=1;
            }
            switch (alt27) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:308:13: usingClause[attrs]
                    {
                    pushFollow(FOLLOW_usingClause_in_updateStatement1219);
                    usingClause(attrs);

                    state._fsp--;


                    }
                    break;

            }

            match(input,K_SET,FOLLOW_K_SET_in_updateStatement1235); 
            pushFollow(FOLLOW_termPairWithOperation_in_updateStatement1237);
            termPairWithOperation(columns);

            state._fsp--;

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:309:48: ( ',' termPairWithOperation[columns] )*
            loop28:
            do {
                int alt28=2;
                int LA28_0 = input.LA(1);

                if ( (LA28_0==82) ) {
                    alt28=1;
                }


                switch (alt28) {
            	case 1 :
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:309:49: ',' termPairWithOperation[columns]
            	    {
            	    match(input,82,FOLLOW_82_in_updateStatement1241); 
            	    pushFollow(FOLLOW_termPairWithOperation_in_updateStatement1243);
            	    termPairWithOperation(columns);

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop28;
                }
            } while (true);

            match(input,K_WHERE,FOLLOW_K_WHERE_in_updateStatement1258); 
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:310:19: (key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:310:21: key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' )
            {
            pushFollow(FOLLOW_term_in_updateStatement1264);
            key_alias=term();

            state._fsp--;

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:310:36: ( '=' key= term | K_IN '(' keys= termList ')' )
            int alt29=2;
            int LA29_0 = input.LA(1);

            if ( (LA29_0==85) ) {
                alt29=1;
            }
            else if ( (LA29_0==K_IN) ) {
                alt29=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 29, 0, input);

                throw nvae;
            }
            switch (alt29) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:310:37: '=' key= term
                    {
                    match(input,85,FOLLOW_85_in_updateStatement1267); 
                    pushFollow(FOLLOW_term_in_updateStatement1271);
                    key=term();

                    state._fsp--;

                     keyList = Collections.singletonList(key); 

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:312:37: K_IN '(' keys= termList ')'
                    {
                    match(input,K_IN,FOLLOW_K_IN_in_updateStatement1349); 
                    match(input,80,FOLLOW_80_in_updateStatement1351); 
                    pushFollow(FOLLOW_termList_in_updateStatement1355);
                    keys=termList();

                    state._fsp--;

                     keyList = keys; 
                    match(input,81,FOLLOW_81_in_updateStatement1359); 

                    }
                    break;

            }


            }


                      return new UpdateStatement((columnFamily!=null?columnFamily.getText():null), key_alias.getText(), columns, keyList, attrs);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "updateStatement"


    // $ANTLR start "deleteStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:318:1: deleteStatement returns [DeleteStatement expr] : K_DELETE (cols= termList )? K_FROM columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( usingClauseDelete[attrs] )? K_WHERE (key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) )? ;
    public final DeleteStatement deleteStatement() throws RecognitionException {
        DeleteStatement expr = null;

        Token columnFamily=null;
        List<Term> cols = null;

        Term key_alias = null;

        Term key = null;

        List<Term> keys = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:329:5: ( K_DELETE (cols= termList )? K_FROM columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( usingClauseDelete[attrs] )? K_WHERE (key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) )? )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:329:7: K_DELETE (cols= termList )? K_FROM columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( usingClauseDelete[attrs] )? K_WHERE (key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) )?
            {

                      Attributes attrs = new Attributes();
                      List<Term> keyList = null;
                      List<Term> columnsList = Collections.emptyList();
                  
            match(input,K_DELETE,FOLLOW_K_DELETE_in_deleteStatement1401); 
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:335:11: (cols= termList )?
            int alt30=2;
            int LA30_0 = input.LA(1);

            if ( (LA30_0==IDENT||(LA30_0>=STRING_LITERAL && LA30_0<=INTEGER)||(LA30_0>=K_KEY && LA30_0<=FLOAT)||LA30_0==UUID) ) {
                alt30=1;
            }
            switch (alt30) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:335:13: cols= termList
                    {
                    pushFollow(FOLLOW_termList_in_deleteStatement1417);
                    cols=termList();

                    state._fsp--;

                     columnsList = cols; 

                    }
                    break;

            }

            match(input,K_FROM,FOLLOW_K_FROM_in_deleteStatement1433); 
            columnFamily=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:337:11: ( usingClauseDelete[attrs] )?
            int alt31=2;
            int LA31_0 = input.LA(1);

            if ( (LA31_0==K_USING) ) {
                alt31=1;
            }
            switch (alt31) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:337:13: usingClauseDelete[attrs]
                    {
                    pushFollow(FOLLOW_usingClauseDelete_in_deleteStatement1463);
                    usingClauseDelete(attrs);

                    state._fsp--;


                    }
                    break;

            }

            match(input,K_WHERE,FOLLOW_K_WHERE_in_deleteStatement1479); 
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:338:19: (key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) )?
            int alt33=2;
            int LA33_0 = input.LA(1);

            if ( (LA33_0==IDENT||(LA33_0>=STRING_LITERAL && LA33_0<=INTEGER)||(LA33_0>=K_KEY && LA33_0<=FLOAT)||LA33_0==UUID) ) {
                alt33=1;
            }
            switch (alt33) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:338:21: key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' )
                    {
                    pushFollow(FOLLOW_term_in_deleteStatement1485);
                    key_alias=term();

                    state._fsp--;

                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:338:36: ( '=' key= term | K_IN '(' keys= termList ')' )
                    int alt32=2;
                    int LA32_0 = input.LA(1);

                    if ( (LA32_0==85) ) {
                        alt32=1;
                    }
                    else if ( (LA32_0==K_IN) ) {
                        alt32=2;
                    }
                    else {
                        NoViableAltException nvae =
                            new NoViableAltException("", 32, 0, input);

                        throw nvae;
                    }
                    switch (alt32) {
                        case 1 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:338:37: '=' key= term
                            {
                            match(input,85,FOLLOW_85_in_deleteStatement1488); 
                            pushFollow(FOLLOW_term_in_deleteStatement1492);
                            key=term();

                            state._fsp--;

                             keyList = Collections.singletonList(key); 

                            }
                            break;
                        case 2 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:339:38: K_IN '(' keys= termList ')'
                            {
                            match(input,K_IN,FOLLOW_K_IN_in_deleteStatement1543); 
                            match(input,80,FOLLOW_80_in_deleteStatement1545); 
                            pushFollow(FOLLOW_termList_in_deleteStatement1549);
                            keys=termList();

                            state._fsp--;

                             keyList = keys; 
                            match(input,81,FOLLOW_81_in_deleteStatement1553); 

                            }
                            break;

                    }


                    }
                    break;

            }


                      return new DeleteStatement(columnsList, (columnFamily!=null?columnFamily.getText():null), key_alias.getText(), keyList, attrs);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "deleteStatement"


    // $ANTLR start "createKeyspaceStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:348:1: createKeyspaceStatement returns [CreateKeyspaceStatement expr] : K_CREATE K_KEYSPACE keyspace= ( IDENT | STRING_LITERAL | INTEGER ) K_WITH a1= ( COMPIDENT | IDENT ) '=' v1= ( STRING_LITERAL | INTEGER | IDENT ) ( K_AND aN= ( COMPIDENT | IDENT ) '=' vN= ( STRING_LITERAL | INTEGER | IDENT ) )* endStmnt ;
    public final CreateKeyspaceStatement createKeyspaceStatement() throws RecognitionException {
        CreateKeyspaceStatement expr = null;

        Token keyspace=null;
        Token a1=null;
        Token v1=null;
        Token aN=null;
        Token vN=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:350:5: ( K_CREATE K_KEYSPACE keyspace= ( IDENT | STRING_LITERAL | INTEGER ) K_WITH a1= ( COMPIDENT | IDENT ) '=' v1= ( STRING_LITERAL | INTEGER | IDENT ) ( K_AND aN= ( COMPIDENT | IDENT ) '=' vN= ( STRING_LITERAL | INTEGER | IDENT ) )* endStmnt )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:350:7: K_CREATE K_KEYSPACE keyspace= ( IDENT | STRING_LITERAL | INTEGER ) K_WITH a1= ( COMPIDENT | IDENT ) '=' v1= ( STRING_LITERAL | INTEGER | IDENT ) ( K_AND aN= ( COMPIDENT | IDENT ) '=' vN= ( STRING_LITERAL | INTEGER | IDENT ) )* endStmnt
            {

                      Map<String, String> attrs = new HashMap<String, String>();
                  
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createKeyspaceStatement1616); 
            match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_createKeyspaceStatement1618); 
            keyspace=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            match(input,K_WITH,FOLLOW_K_WITH_in_createKeyspaceStatement1646); 
            a1=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||input.LA(1)==COMPIDENT ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            match(input,85,FOLLOW_85_in_createKeyspaceStatement1661); 
            v1=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

             attrs.put((a1!=null?a1.getText():null), (v1!=null?v1.getText():null)); 
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:355:11: ( K_AND aN= ( COMPIDENT | IDENT ) '=' vN= ( STRING_LITERAL | INTEGER | IDENT ) )*
            loop34:
            do {
                int alt34=2;
                int LA34_0 = input.LA(1);

                if ( (LA34_0==K_AND) ) {
                    alt34=1;
                }


                switch (alt34) {
            	case 1 :
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:355:13: K_AND aN= ( COMPIDENT | IDENT ) '=' vN= ( STRING_LITERAL | INTEGER | IDENT )
            	    {
            	    match(input,K_AND,FOLLOW_K_AND_in_createKeyspaceStatement1693); 
            	    aN=(Token)input.LT(1);
            	    if ( input.LA(1)==IDENT||input.LA(1)==COMPIDENT ) {
            	        input.consume();
            	        state.errorRecovery=false;
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        throw mse;
            	    }

            	    match(input,85,FOLLOW_85_in_createKeyspaceStatement1707); 
            	    vN=(Token)input.LT(1);
            	    if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
            	        input.consume();
            	        state.errorRecovery=false;
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        throw mse;
            	    }

            	     attrs.put((aN!=null?aN.getText():null), (vN!=null?vN.getText():null)); 

            	    }
            	    break;

            	default :
            	    break loop34;
                }
            } while (true);

            pushFollow(FOLLOW_endStmnt_in_createKeyspaceStatement1740);
            endStmnt();

            state._fsp--;


                      return new CreateKeyspaceStatement((keyspace!=null?keyspace.getText():null), attrs);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "createKeyspaceStatement"


    // $ANTLR start "createColumnFamilyStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:362:1: createColumnFamilyStatement returns [CreateColumnFamilyStatement expr] : K_CREATE K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) ( '(' createCfamColumns[expr] ( ',' createCfamColumns[expr] )* ')' )? ( K_WITH prop1= IDENT '=' arg1= createCfamKeywordArgument ( K_AND propN= IDENT '=' argN= createCfamKeywordArgument )* )? endStmnt ;
    public final CreateColumnFamilyStatement createColumnFamilyStatement() throws RecognitionException {
        CreateColumnFamilyStatement expr = null;

        Token name=null;
        Token prop1=null;
        Token propN=null;
        String arg1 = null;

        String argN = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:370:5: ( K_CREATE K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) ( '(' createCfamColumns[expr] ( ',' createCfamColumns[expr] )* ')' )? ( K_WITH prop1= IDENT '=' arg1= createCfamKeywordArgument ( K_AND propN= IDENT '=' argN= createCfamKeywordArgument )* )? endStmnt )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:370:7: K_CREATE K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) ( '(' createCfamColumns[expr] ( ',' createCfamColumns[expr] )* ')' )? ( K_WITH prop1= IDENT '=' arg1= createCfamKeywordArgument ( K_AND propN= IDENT '=' argN= createCfamKeywordArgument )* )? endStmnt
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createColumnFamilyStatement1775); 
            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_createColumnFamilyStatement1777); 
            name=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

             expr = new CreateColumnFamilyStatement((name!=null?name.getText():null)); 
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:371:7: ( '(' createCfamColumns[expr] ( ',' createCfamColumns[expr] )* ')' )?
            int alt36=2;
            int LA36_0 = input.LA(1);

            if ( (LA36_0==80) ) {
                alt36=1;
            }
            switch (alt36) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:371:9: '(' createCfamColumns[expr] ( ',' createCfamColumns[expr] )* ')'
                    {
                    match(input,80,FOLLOW_80_in_createColumnFamilyStatement1805); 
                    pushFollow(FOLLOW_createCfamColumns_in_createColumnFamilyStatement1807);
                    createCfamColumns(expr);

                    state._fsp--;

                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:371:37: ( ',' createCfamColumns[expr] )*
                    loop35:
                    do {
                        int alt35=2;
                        int LA35_0 = input.LA(1);

                        if ( (LA35_0==82) ) {
                            alt35=1;
                        }


                        switch (alt35) {
                    	case 1 :
                    	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:371:39: ',' createCfamColumns[expr]
                    	    {
                    	    match(input,82,FOLLOW_82_in_createColumnFamilyStatement1812); 
                    	    pushFollow(FOLLOW_createCfamColumns_in_createColumnFamilyStatement1814);
                    	    createCfamColumns(expr);

                    	    state._fsp--;


                    	    }
                    	    break;

                    	default :
                    	    break loop35;
                        }
                    } while (true);

                    match(input,81,FOLLOW_81_in_createColumnFamilyStatement1820); 

                    }
                    break;

            }

            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:372:7: ( K_WITH prop1= IDENT '=' arg1= createCfamKeywordArgument ( K_AND propN= IDENT '=' argN= createCfamKeywordArgument )* )?
            int alt38=2;
            int LA38_0 = input.LA(1);

            if ( (LA38_0==K_WITH) ) {
                alt38=1;
            }
            switch (alt38) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:372:9: K_WITH prop1= IDENT '=' arg1= createCfamKeywordArgument ( K_AND propN= IDENT '=' argN= createCfamKeywordArgument )*
                    {
                    match(input,K_WITH,FOLLOW_K_WITH_in_createColumnFamilyStatement1833); 
                    prop1=(Token)match(input,IDENT,FOLLOW_IDENT_in_createColumnFamilyStatement1837); 
                    match(input,85,FOLLOW_85_in_createColumnFamilyStatement1839); 
                    pushFollow(FOLLOW_createCfamKeywordArgument_in_createColumnFamilyStatement1843);
                    arg1=createCfamKeywordArgument();

                    state._fsp--;

                     expr.addProperty((prop1!=null?prop1.getText():null), arg1); 
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:373:11: ( K_AND propN= IDENT '=' argN= createCfamKeywordArgument )*
                    loop37:
                    do {
                        int alt37=2;
                        int LA37_0 = input.LA(1);

                        if ( (LA37_0==K_AND) ) {
                            alt37=1;
                        }


                        switch (alt37) {
                    	case 1 :
                    	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:373:13: K_AND propN= IDENT '=' argN= createCfamKeywordArgument
                    	    {
                    	    match(input,K_AND,FOLLOW_K_AND_in_createColumnFamilyStatement1859); 
                    	    propN=(Token)match(input,IDENT,FOLLOW_IDENT_in_createColumnFamilyStatement1863); 
                    	    match(input,85,FOLLOW_85_in_createColumnFamilyStatement1865); 
                    	    pushFollow(FOLLOW_createCfamKeywordArgument_in_createColumnFamilyStatement1869);
                    	    argN=createCfamKeywordArgument();

                    	    state._fsp--;

                    	     expr.addProperty((propN!=null?propN.getText():null), argN); 

                    	    }
                    	    break;

                    	default :
                    	    break loop37;
                        }
                    } while (true);


                    }
                    break;

            }

            pushFollow(FOLLOW_endStmnt_in_createColumnFamilyStatement1891);
            endStmnt();

            state._fsp--;


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "createColumnFamilyStatement"


    // $ANTLR start "createCfamColumns"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:378:1: createCfamColumns[CreateColumnFamilyStatement expr] : (n= term v= createCfamColumnValidator | k= term v= createCfamColumnValidator K_PRIMARY K_KEY );
    public final void createCfamColumns(CreateColumnFamilyStatement expr) throws RecognitionException {
        Term n = null;

        String v = null;

        Term k = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:379:5: (n= term v= createCfamColumnValidator | k= term v= createCfamColumnValidator K_PRIMARY K_KEY )
            int alt39=2;
            alt39 = dfa39.predict(input);
            switch (alt39) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:379:7: n= term v= createCfamColumnValidator
                    {
                    pushFollow(FOLLOW_term_in_createCfamColumns1911);
                    n=term();

                    state._fsp--;

                    pushFollow(FOLLOW_createCfamColumnValidator_in_createCfamColumns1915);
                    v=createCfamColumnValidator();

                    state._fsp--;

                     expr.addColumn(n, v); 

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:380:7: k= term v= createCfamColumnValidator K_PRIMARY K_KEY
                    {
                    pushFollow(FOLLOW_term_in_createCfamColumns1927);
                    k=term();

                    state._fsp--;

                    pushFollow(FOLLOW_createCfamColumnValidator_in_createCfamColumns1931);
                    v=createCfamColumnValidator();

                    state._fsp--;

                    match(input,K_PRIMARY,FOLLOW_K_PRIMARY_in_createCfamColumns1933); 
                    match(input,K_KEY,FOLLOW_K_KEY_in_createCfamColumns1935); 
                     expr.setKeyAlias(k.getText()); expr.setKeyType(v); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "createCfamColumns"


    // $ANTLR start "createCfamColumnValidator"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:383:1: createCfamColumnValidator returns [String validator] : ( comparatorType | STRING_LITERAL );
    public final String createCfamColumnValidator() throws RecognitionException {
        String validator = null;

        Token STRING_LITERAL20=null;
        CqlParser.comparatorType_return comparatorType19 = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:384:5: ( comparatorType | STRING_LITERAL )
            int alt40=2;
            int LA40_0 = input.LA(1);

            if ( ((LA40_0>=86 && LA40_0<=98)) ) {
                alt40=1;
            }
            else if ( (LA40_0==STRING_LITERAL) ) {
                alt40=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 40, 0, input);

                throw nvae;
            }
            switch (alt40) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:384:7: comparatorType
                    {
                    pushFollow(FOLLOW_comparatorType_in_createCfamColumnValidator1958);
                    comparatorType19=comparatorType();

                    state._fsp--;

                     validator = (comparatorType19!=null?input.toString(comparatorType19.start,comparatorType19.stop):null); 

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:385:7: STRING_LITERAL
                    {
                    STRING_LITERAL20=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_createCfamColumnValidator1968); 
                     validator = (STRING_LITERAL20!=null?STRING_LITERAL20.getText():null); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return validator;
    }
    // $ANTLR end "createCfamColumnValidator"


    // $ANTLR start "createCfamKeywordArgument"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:388:1: createCfamKeywordArgument returns [String arg] : ( comparatorType | value= ( STRING_LITERAL | IDENT | INTEGER | FLOAT ) );
    public final String createCfamKeywordArgument() throws RecognitionException {
        String arg = null;

        Token value=null;
        CqlParser.comparatorType_return comparatorType21 = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:389:5: ( comparatorType | value= ( STRING_LITERAL | IDENT | INTEGER | FLOAT ) )
            int alt41=2;
            int LA41_0 = input.LA(1);

            if ( ((LA41_0>=86 && LA41_0<=98)) ) {
                alt41=1;
            }
            else if ( (LA41_0==IDENT||(LA41_0>=STRING_LITERAL && LA41_0<=INTEGER)||LA41_0==FLOAT) ) {
                alt41=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 41, 0, input);

                throw nvae;
            }
            switch (alt41) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:389:7: comparatorType
                    {
                    pushFollow(FOLLOW_comparatorType_in_createCfamKeywordArgument1991);
                    comparatorType21=comparatorType();

                    state._fsp--;

                     arg = (comparatorType21!=null?input.toString(comparatorType21.start,comparatorType21.stop):null); 

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:390:7: value= ( STRING_LITERAL | IDENT | INTEGER | FLOAT )
                    {
                    value=(Token)input.LT(1);
                    if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER)||input.LA(1)==FLOAT ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                     arg = (value!=null?value.getText():null); 

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return arg;
    }
    // $ANTLR end "createCfamKeywordArgument"


    // $ANTLR start "createIndexStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:393:1: createIndexStatement returns [CreateIndexStatement expr] : K_CREATE K_INDEX (idxName= IDENT )? K_ON cf= ( IDENT | STRING_LITERAL | INTEGER ) '(' columnName= term ')' endStmnt ;
    public final CreateIndexStatement createIndexStatement() throws RecognitionException {
        CreateIndexStatement expr = null;

        Token idxName=null;
        Token cf=null;
        Term columnName = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:395:5: ( K_CREATE K_INDEX (idxName= IDENT )? K_ON cf= ( IDENT | STRING_LITERAL | INTEGER ) '(' columnName= term ')' endStmnt )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:395:7: K_CREATE K_INDEX (idxName= IDENT )? K_ON cf= ( IDENT | STRING_LITERAL | INTEGER ) '(' columnName= term ')' endStmnt
            {
            match(input,K_CREATE,FOLLOW_K_CREATE_in_createIndexStatement2044); 
            match(input,K_INDEX,FOLLOW_K_INDEX_in_createIndexStatement2046); 
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:395:24: (idxName= IDENT )?
            int alt42=2;
            int LA42_0 = input.LA(1);

            if ( (LA42_0==IDENT) ) {
                alt42=1;
            }
            switch (alt42) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:395:25: idxName= IDENT
                    {
                    idxName=(Token)match(input,IDENT,FOLLOW_IDENT_in_createIndexStatement2051); 

                    }
                    break;

            }

            match(input,K_ON,FOLLOW_K_ON_in_createIndexStatement2055); 
            cf=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            match(input,80,FOLLOW_80_in_createIndexStatement2073); 
            pushFollow(FOLLOW_term_in_createIndexStatement2077);
            columnName=term();

            state._fsp--;

            match(input,81,FOLLOW_81_in_createIndexStatement2079); 
            pushFollow(FOLLOW_endStmnt_in_createIndexStatement2081);
            endStmnt();

            state._fsp--;

             expr = new CreateIndexStatement((idxName!=null?idxName.getText():null), (cf!=null?cf.getText():null), columnName); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "createIndexStatement"


    // $ANTLR start "dropIndexStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:398:1: dropIndexStatement returns [DropIndexStatement expr] : K_DROP K_INDEX index= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt ;
    public final DropIndexStatement dropIndexStatement() throws RecognitionException {
        DropIndexStatement expr = null;

        Token index=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:403:5: ( K_DROP K_INDEX index= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:404:7: K_DROP K_INDEX index= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropIndexStatement2117); 
            match(input,K_INDEX,FOLLOW_K_INDEX_in_dropIndexStatement2119); 
            index=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            pushFollow(FOLLOW_endStmnt_in_dropIndexStatement2137);
            endStmnt();

            state._fsp--;

             expr = new DropIndexStatement((index!=null?index.getText():null)); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "dropIndexStatement"


    // $ANTLR start "dropKeyspaceStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:408:1: dropKeyspaceStatement returns [String ksp] : K_DROP K_KEYSPACE name= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt ;
    public final String dropKeyspaceStatement() throws RecognitionException {
        String ksp = null;

        Token name=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:410:5: ( K_DROP K_KEYSPACE name= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:410:7: K_DROP K_KEYSPACE name= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropKeyspaceStatement2168); 
            match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_dropKeyspaceStatement2170); 
            name=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            pushFollow(FOLLOW_endStmnt_in_dropKeyspaceStatement2188);
            endStmnt();

            state._fsp--;

             ksp = (name!=null?name.getText():null); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ksp;
    }
    // $ANTLR end "dropKeyspaceStatement"


    // $ANTLR start "alterTableStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:414:1: alterTableStatement returns [AlterTableStatement expr] : K_ALTER K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) ( K_ALTER (col= ( IDENT | STRING_LITERAL | INTEGER ) ) K_TYPE alterValidator= comparatorType | K_ADD (col= ( IDENT | STRING_LITERAL | INTEGER ) ) addValidator= comparatorType | K_DROP (col= ( IDENT | STRING_LITERAL | INTEGER ) ) ) endStmnt ;
    public final AlterTableStatement alterTableStatement() throws RecognitionException {
        AlterTableStatement expr = null;

        Token name=null;
        Token col=null;
        CqlParser.comparatorType_return alterValidator = null;

        CqlParser.comparatorType_return addValidator = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:415:5: ( K_ALTER K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) ( K_ALTER (col= ( IDENT | STRING_LITERAL | INTEGER ) ) K_TYPE alterValidator= comparatorType | K_ADD (col= ( IDENT | STRING_LITERAL | INTEGER ) ) addValidator= comparatorType | K_DROP (col= ( IDENT | STRING_LITERAL | INTEGER ) ) ) endStmnt )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:416:5: K_ALTER K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) ( K_ALTER (col= ( IDENT | STRING_LITERAL | INTEGER ) ) K_TYPE alterValidator= comparatorType | K_ADD (col= ( IDENT | STRING_LITERAL | INTEGER ) ) addValidator= comparatorType | K_DROP (col= ( IDENT | STRING_LITERAL | INTEGER ) ) ) endStmnt
            {

                    OperationType type = null;
                    String columnFamily = null, columnName = null, validator = null;
                
            match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTableStatement2222); 
            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_alterTableStatement2224); 
            name=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

             columnFamily = (name!=null?name.getText():null); 
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:421:11: ( K_ALTER (col= ( IDENT | STRING_LITERAL | INTEGER ) ) K_TYPE alterValidator= comparatorType | K_ADD (col= ( IDENT | STRING_LITERAL | INTEGER ) ) addValidator= comparatorType | K_DROP (col= ( IDENT | STRING_LITERAL | INTEGER ) ) )
            int alt43=3;
            switch ( input.LA(1) ) {
            case K_ALTER:
                {
                alt43=1;
                }
                break;
            case K_ADD:
                {
                alt43=2;
                }
                break;
            case K_DROP:
                {
                alt43=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 43, 0, input);

                throw nvae;
            }

            switch (alt43) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:421:13: K_ALTER (col= ( IDENT | STRING_LITERAL | INTEGER ) ) K_TYPE alterValidator= comparatorType
                    {
                    match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTableStatement2256); 
                     type = OperationType.ALTER; 
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:422:16: (col= ( IDENT | STRING_LITERAL | INTEGER ) )
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:422:17: col= ( IDENT | STRING_LITERAL | INTEGER )
                    {
                    col=(Token)input.LT(1);
                    if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                     columnName = (col!=null?col.getText():null); 

                    }

                    match(input,K_TYPE,FOLLOW_K_TYPE_in_alterTableStatement2310); 
                    pushFollow(FOLLOW_comparatorType_in_alterTableStatement2314);
                    alterValidator=comparatorType();

                    state._fsp--;

                     validator = (alterValidator!=null?input.toString(alterValidator.start,alterValidator.stop):null); 

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:424:13: K_ADD (col= ( IDENT | STRING_LITERAL | INTEGER ) ) addValidator= comparatorType
                    {
                    match(input,K_ADD,FOLLOW_K_ADD_in_alterTableStatement2330); 
                     type = OperationType.ADD; 
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:425:16: (col= ( IDENT | STRING_LITERAL | INTEGER ) )
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:425:17: col= ( IDENT | STRING_LITERAL | INTEGER )
                    {
                    col=(Token)input.LT(1);
                    if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                     columnName = (col!=null?col.getText():null); 

                    }

                    pushFollow(FOLLOW_comparatorType_in_alterTableStatement2386);
                    addValidator=comparatorType();

                    state._fsp--;

                     validator = (addValidator!=null?input.toString(addValidator.start,addValidator.stop):null); 

                    }
                    break;
                case 3 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:427:13: K_DROP (col= ( IDENT | STRING_LITERAL | INTEGER ) )
                    {
                    match(input,K_DROP,FOLLOW_K_DROP_in_alterTableStatement2402); 
                     type = OperationType.DROP; 
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:428:16: (col= ( IDENT | STRING_LITERAL | INTEGER ) )
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:428:17: col= ( IDENT | STRING_LITERAL | INTEGER )
                    {
                    col=(Token)input.LT(1);
                    if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                        input.consume();
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                     columnName = (col!=null?col.getText():null); 

                    }


                    }
                    break;

            }

            pushFollow(FOLLOW_endStmnt_in_alterTableStatement2446);
            endStmnt();

            state._fsp--;


                      expr = new AlterTableStatement(columnFamily, type, columnName, validator);
                  

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return expr;
    }
    // $ANTLR end "alterTableStatement"


    // $ANTLR start "dropColumnFamilyStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:434:1: dropColumnFamilyStatement returns [String cfam] : K_DROP K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt ;
    public final String dropColumnFamilyStatement() throws RecognitionException {
        String cfam = null;

        Token name=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:436:5: ( K_DROP K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:436:7: K_DROP K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt
            {
            match(input,K_DROP,FOLLOW_K_DROP_in_dropColumnFamilyStatement2476); 
            match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_dropColumnFamilyStatement2478); 
            name=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            pushFollow(FOLLOW_endStmnt_in_dropColumnFamilyStatement2496);
            endStmnt();

            state._fsp--;

             cfam = (name!=null?name.getText():null); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return cfam;
    }
    // $ANTLR end "dropColumnFamilyStatement"

    public static class comparatorType_return extends ParserRuleReturnScope {
    };

    // $ANTLR start "comparatorType"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:439:1: comparatorType : ( 'bytea' | 'ascii' | 'text' | 'varchar' | 'int' | 'varint' | 'bigint' | 'uuid' | 'counter' | 'boolean' | 'date' | 'float' | 'double' );
    public final CqlParser.comparatorType_return comparatorType() throws RecognitionException {
        CqlParser.comparatorType_return retval = new CqlParser.comparatorType_return();
        retval.start = input.LT(1);

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:440:5: ( 'bytea' | 'ascii' | 'text' | 'varchar' | 'int' | 'varint' | 'bigint' | 'uuid' | 'counter' | 'boolean' | 'date' | 'float' | 'double' )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:
            {
            if ( (input.LA(1)>=86 && input.LA(1)<=98) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "comparatorType"


    // $ANTLR start "term"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:443:1: term returns [Term item] : (t= K_KEY | t= STRING_LITERAL | t= INTEGER | t= UUID | t= IDENT | t= FLOAT ) ;
    public final Term term() throws RecognitionException {
        Term item = null;

        Token t=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:444:5: ( (t= K_KEY | t= STRING_LITERAL | t= INTEGER | t= UUID | t= IDENT | t= FLOAT ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:444:7: (t= K_KEY | t= STRING_LITERAL | t= INTEGER | t= UUID | t= IDENT | t= FLOAT )
            {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:444:7: (t= K_KEY | t= STRING_LITERAL | t= INTEGER | t= UUID | t= IDENT | t= FLOAT )
            int alt44=6;
            switch ( input.LA(1) ) {
            case K_KEY:
                {
                alt44=1;
                }
                break;
            case STRING_LITERAL:
                {
                alt44=2;
                }
                break;
            case INTEGER:
                {
                alt44=3;
                }
                break;
            case UUID:
                {
                alt44=4;
                }
                break;
            case IDENT:
                {
                alt44=5;
                }
                break;
            case FLOAT:
                {
                alt44=6;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 44, 0, input);

                throw nvae;
            }

            switch (alt44) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:444:9: t= K_KEY
                    {
                    t=(Token)match(input,K_KEY,FOLLOW_K_KEY_in_term2588); 

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:444:19: t= STRING_LITERAL
                    {
                    t=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_term2594); 

                    }
                    break;
                case 3 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:444:38: t= INTEGER
                    {
                    t=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_term2600); 

                    }
                    break;
                case 4 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:444:50: t= UUID
                    {
                    t=(Token)match(input,UUID,FOLLOW_UUID_in_term2606); 

                    }
                    break;
                case 5 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:444:59: t= IDENT
                    {
                    t=(Token)match(input,IDENT,FOLLOW_IDENT_in_term2612); 

                    }
                    break;
                case 6 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:444:69: t= FLOAT
                    {
                    t=(Token)match(input,FLOAT,FOLLOW_FLOAT_in_term2618); 

                    }
                    break;

            }

             item = new Term((t!=null?t.getText():null), (t!=null?t.getType():0)); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return item;
    }
    // $ANTLR end "term"


    // $ANTLR start "termList"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:447:1: termList returns [List<Term> items] : t1= term ( ',' tN= term )* ;
    public final List<Term> termList() throws RecognitionException {
        List<Term> items = null;

        Term t1 = null;

        Term tN = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:448:5: (t1= term ( ',' tN= term )* )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:448:7: t1= term ( ',' tN= term )*
            {
             items = new ArrayList<Term>(); 
            pushFollow(FOLLOW_term_in_termList2652);
            t1=term();

            state._fsp--;

             items.add(t1); 
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:449:35: ( ',' tN= term )*
            loop45:
            do {
                int alt45=2;
                int LA45_0 = input.LA(1);

                if ( (LA45_0==82) ) {
                    alt45=1;
                }


                switch (alt45) {
            	case 1 :
            	    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:449:36: ',' tN= term
            	    {
            	    match(input,82,FOLLOW_82_in_termList2657); 
            	    pushFollow(FOLLOW_term_in_termList2661);
            	    tN=term();

            	    state._fsp--;

            	     items.add(tN); 

            	    }
            	    break;

            	default :
            	    break loop45;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return items;
    }
    // $ANTLR end "termList"


    // $ANTLR start "termPair"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:453:1: termPair[Map<Term, Term> columns] : key= term '=' value= term ;
    public final void termPair(Map<Term, Term> columns) throws RecognitionException {
        Term key = null;

        Term value = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:454:5: (key= term '=' value= term )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:454:9: key= term '=' value= term
            {
            pushFollow(FOLLOW_term_in_termPair2688);
            key=term();

            state._fsp--;

            match(input,85,FOLLOW_85_in_termPair2690); 
            pushFollow(FOLLOW_term_in_termPair2694);
            value=term();

            state._fsp--;

             columns.put(key, value); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "termPair"


    // $ANTLR start "termPairWithOperation"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:457:1: termPairWithOperation[Map<Term, Operation> columns] : key= term '=' (value= term | c= term ( '+' v= term | '-' v= term ) ) ;
    public final void termPairWithOperation(Map<Term, Operation> columns) throws RecognitionException {
        Term key = null;

        Term value = null;

        Term c = null;

        Term v = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:458:5: (key= term '=' (value= term | c= term ( '+' v= term | '-' v= term ) ) )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:458:7: key= term '=' (value= term | c= term ( '+' v= term | '-' v= term ) )
            {
            pushFollow(FOLLOW_term_in_termPairWithOperation2716);
            key=term();

            state._fsp--;

            match(input,85,FOLLOW_85_in_termPairWithOperation2718); 
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:458:20: (value= term | c= term ( '+' v= term | '-' v= term ) )
            int alt47=2;
            switch ( input.LA(1) ) {
            case K_KEY:
                {
                int LA47_1 = input.LA(2);

                if ( (LA47_1==K_WHERE||LA47_1==82) ) {
                    alt47=1;
                }
                else if ( ((LA47_1>=99 && LA47_1<=100)) ) {
                    alt47=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 47, 1, input);

                    throw nvae;
                }
                }
                break;
            case STRING_LITERAL:
                {
                int LA47_2 = input.LA(2);

                if ( (LA47_2==K_WHERE||LA47_2==82) ) {
                    alt47=1;
                }
                else if ( ((LA47_2>=99 && LA47_2<=100)) ) {
                    alt47=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 47, 2, input);

                    throw nvae;
                }
                }
                break;
            case INTEGER:
                {
                int LA47_3 = input.LA(2);

                if ( (LA47_3==K_WHERE||LA47_3==82) ) {
                    alt47=1;
                }
                else if ( ((LA47_3>=99 && LA47_3<=100)) ) {
                    alt47=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 47, 3, input);

                    throw nvae;
                }
                }
                break;
            case UUID:
                {
                int LA47_4 = input.LA(2);

                if ( (LA47_4==K_WHERE||LA47_4==82) ) {
                    alt47=1;
                }
                else if ( ((LA47_4>=99 && LA47_4<=100)) ) {
                    alt47=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 47, 4, input);

                    throw nvae;
                }
                }
                break;
            case IDENT:
                {
                int LA47_5 = input.LA(2);

                if ( (LA47_5==K_WHERE||LA47_5==82) ) {
                    alt47=1;
                }
                else if ( ((LA47_5>=99 && LA47_5<=100)) ) {
                    alt47=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 47, 5, input);

                    throw nvae;
                }
                }
                break;
            case FLOAT:
                {
                int LA47_6 = input.LA(2);

                if ( ((LA47_6>=99 && LA47_6<=100)) ) {
                    alt47=2;
                }
                else if ( (LA47_6==K_WHERE||LA47_6==82) ) {
                    alt47=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 47, 6, input);

                    throw nvae;
                }
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 47, 0, input);

                throw nvae;
            }

            switch (alt47) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:458:21: value= term
                    {
                    pushFollow(FOLLOW_term_in_termPairWithOperation2723);
                    value=term();

                    state._fsp--;

                     columns.put(key, new Operation(value)); 

                    }
                    break;
                case 2 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:459:9: c= term ( '+' v= term | '-' v= term )
                    {
                    pushFollow(FOLLOW_term_in_termPairWithOperation2737);
                    c=term();

                    state._fsp--;

                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:459:16: ( '+' v= term | '-' v= term )
                    int alt46=2;
                    int LA46_0 = input.LA(1);

                    if ( (LA46_0==99) ) {
                        alt46=1;
                    }
                    else if ( (LA46_0==100) ) {
                        alt46=2;
                    }
                    else {
                        NoViableAltException nvae =
                            new NoViableAltException("", 46, 0, input);

                        throw nvae;
                    }
                    switch (alt46) {
                        case 1 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:459:18: '+' v= term
                            {
                            match(input,99,FOLLOW_99_in_termPairWithOperation2741); 
                            pushFollow(FOLLOW_term_in_termPairWithOperation2745);
                            v=term();

                            state._fsp--;

                             columns.put(key, new Operation(c, org.apache.cassandra.cql.Operation.OperationType.PLUS, v)); 

                            }
                            break;
                        case 2 :
                            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:460:31: '-' v= term
                            {
                            match(input,100,FOLLOW_100_in_termPairWithOperation2779); 
                            pushFollow(FOLLOW_term_in_termPairWithOperation2783);
                            v=term();

                            state._fsp--;

                             columns.put(key, new Operation(c, org.apache.cassandra.cql.Operation.OperationType.MINUS, v)); 

                            }
                            break;

                    }


                    }
                    break;

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "termPairWithOperation"


    // $ANTLR start "relation"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:464:1: relation returns [Relation rel] : name= term type= ( '=' | '<' | '<=' | '>=' | '>' ) t= term ;
    public final Relation relation() throws RecognitionException {
        Relation rel = null;

        Token type=null;
        Term name = null;

        Term t = null;


        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:465:5: (name= term type= ( '=' | '<' | '<=' | '>=' | '>' ) t= term )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:465:7: name= term type= ( '=' | '<' | '<=' | '>=' | '>' ) t= term
            {
            pushFollow(FOLLOW_term_in_relation2812);
            name=term();

            state._fsp--;

            type=(Token)input.LT(1);
            if ( input.LA(1)==85||(input.LA(1)>=101 && input.LA(1)<=104) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

            pushFollow(FOLLOW_term_in_relation2838);
            t=term();

            state._fsp--;

             return new Relation(name, (type!=null?type.getText():null), t); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return rel;
    }
    // $ANTLR end "relation"


    // $ANTLR start "truncateStatement"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:470:1: truncateStatement returns [String cfam] : K_TRUNCATE columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt ;
    public final String truncateStatement() throws RecognitionException {
        String cfam = null;

        Token columnFamily=null;

        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:471:5: ( K_TRUNCATE columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:471:7: K_TRUNCATE columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt
            {
            match(input,K_TRUNCATE,FOLLOW_K_TRUNCATE_in_truncateStatement2868); 
            columnFamily=(Token)input.LT(1);
            if ( input.LA(1)==IDENT||(input.LA(1)>=STRING_LITERAL && input.LA(1)<=INTEGER) ) {
                input.consume();
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }

             cfam = (columnFamily!=null?columnFamily.getText():null); 
            pushFollow(FOLLOW_endStmnt_in_truncateStatement2888);
            endStmnt();

            state._fsp--;


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return cfam;
    }
    // $ANTLR end "truncateStatement"


    // $ANTLR start "endStmnt"
    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:474:1: endStmnt : ( ';' )? EOF ;
    public final void endStmnt() throws RecognitionException {
        try {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:475:5: ( ( ';' )? EOF )
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:475:7: ( ';' )? EOF
            {
            // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:475:7: ( ';' )?
            int alt48=2;
            int LA48_0 = input.LA(1);

            if ( (LA48_0==84) ) {
                alt48=1;
            }
            switch (alt48) {
                case 1 :
                    // /home/drift/svn/cassandra/trunk/src/java/org/apache/cassandra/cql/Cql.g:475:7: ';'
                    {
                    match(input,84,FOLLOW_84_in_endStmnt2905); 

                    }
                    break;

            }

            match(input,EOF,FOLLOW_EOF_in_endStmnt2909); 

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "endStmnt"

    // Delegated rules


    protected DFA1 dfa1 = new DFA1(this);
    protected DFA9 dfa9 = new DFA9(this);
    protected DFA39 dfa39 = new DFA39(this);
    static final String DFA1_eotS =
        "\21\uffff";
    static final String DFA1_eofS =
        "\21\uffff";
    static final String DFA1_minS =
        "\1\4\7\uffff\2\41\7\uffff";
    static final String DFA1_maxS =
        "\1\57\7\uffff\2\50\7\uffff";
    static final String DFA1_acceptS =
        "\1\uffff\1\1\1\2\1\3\1\4\1\5\1\6\1\7\2\uffff\1\16\1\10\1\11\1\12"+
        "\1\13\1\14\1\15";
    static final String DFA1_specialS =
        "\21\uffff}>";
    static final String[] DFA1_transitionS = {
            "\1\5\1\uffff\1\1\16\uffff\1\2\4\uffff\1\4\2\uffff\1\3\1\uffff"+
            "\1\7\1\10\11\uffff\1\11\1\12\3\uffff\1\6",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\13\2\uffff\1\14\3\uffff\1\15",
            "\1\17\2\uffff\1\20\3\uffff\1\16",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA1_eot = DFA.unpackEncodedString(DFA1_eotS);
    static final short[] DFA1_eof = DFA.unpackEncodedString(DFA1_eofS);
    static final char[] DFA1_min = DFA.unpackEncodedStringToUnsignedChars(DFA1_minS);
    static final char[] DFA1_max = DFA.unpackEncodedStringToUnsignedChars(DFA1_maxS);
    static final short[] DFA1_accept = DFA.unpackEncodedString(DFA1_acceptS);
    static final short[] DFA1_special = DFA.unpackEncodedString(DFA1_specialS);
    static final short[][] DFA1_transition;

    static {
        int numStates = DFA1_transitionS.length;
        DFA1_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA1_transition[i] = DFA.unpackEncodedString(DFA1_transitionS[i]);
        }
    }

    class DFA1 extends DFA {

        public DFA1(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 1;
            this.eot = DFA1_eot;
            this.eof = DFA1_eof;
            this.min = DFA1_min;
            this.max = DFA1_max;
            this.accept = DFA1_accept;
            this.special = DFA1_special;
            this.transition = DFA1_transition;
        }
        public String getDescription() {
            return "105:1: query returns [CQLStatement stmnt] : ( selectStatement | insertStatement endStmnt | updateStatement endStmnt | batchStatement | useStatement | truncateStatement | deleteStatement endStmnt | createKeyspaceStatement | createColumnFamilyStatement | createIndexStatement | dropIndexStatement | dropKeyspaceStatement | dropColumnFamilyStatement | alterTableStatement );";
        }
    }
    static final String DFA9_eotS =
        "\12\uffff";
    static final String DFA9_eofS =
        "\12\uffff";
    static final String DFA9_minS =
        "\1\5\6\10\3\uffff";
    static final String DFA9_maxS =
        "\1\123\6\122\3\uffff";
    static final String DFA9_acceptS =
        "\7\uffff\1\3\1\1\1\2";
    static final String DFA9_specialS =
        "\12\uffff}>";
    static final String[] DFA9_transitionS = {
            "\1\5\3\uffff\1\2\1\3\33\uffff\1\1\1\6\6\uffff\1\4\44\uffff\1"+
            "\7",
            "\1\10\11\uffff\1\11\76\uffff\2\10",
            "\1\10\11\uffff\1\11\76\uffff\2\10",
            "\1\10\11\uffff\1\11\76\uffff\2\10",
            "\1\10\11\uffff\1\11\76\uffff\2\10",
            "\1\10\11\uffff\1\11\76\uffff\2\10",
            "\1\10\11\uffff\1\11\76\uffff\2\10",
            "",
            "",
            ""
    };

    static final short[] DFA9_eot = DFA.unpackEncodedString(DFA9_eotS);
    static final short[] DFA9_eof = DFA.unpackEncodedString(DFA9_eofS);
    static final char[] DFA9_min = DFA.unpackEncodedStringToUnsignedChars(DFA9_minS);
    static final char[] DFA9_max = DFA.unpackEncodedStringToUnsignedChars(DFA9_maxS);
    static final short[] DFA9_accept = DFA.unpackEncodedString(DFA9_acceptS);
    static final short[] DFA9_special = DFA.unpackEncodedString(DFA9_specialS);
    static final short[][] DFA9_transition;

    static {
        int numStates = DFA9_transitionS.length;
        DFA9_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA9_transition[i] = DFA.unpackEncodedString(DFA9_transitionS[i]);
        }
    }

    class DFA9 extends DFA {

        public DFA9(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 9;
            this.eot = DFA9_eot;
            this.eof = DFA9_eof;
            this.min = DFA9_min;
            this.max = DFA9_max;
            this.accept = DFA9_accept;
            this.special = DFA9_special;
            this.transition = DFA9_transition;
        }
        public String getDescription() {
            return "174:7: (first= term ( ',' next= term )* | start= term RANGEOP finish= term | '\\*' )";
        }
    }
    static final String DFA39_eotS =
        "\13\uffff";
    static final String DFA39_eofS =
        "\13\uffff";
    static final String DFA39_minS =
        "\1\5\6\11\2\45\2\uffff";
    static final String DFA39_maxS =
        "\1\56\6\142\2\122\2\uffff";
    static final String DFA39_acceptS =
        "\11\uffff\1\1\1\2";
    static final String DFA39_specialS =
        "\13\uffff}>";
    static final String[] DFA39_transitionS = {
            "\1\5\3\uffff\1\2\1\3\33\uffff\1\1\1\6\6\uffff\1\4",
            "\1\10\114\uffff\15\7",
            "\1\10\114\uffff\15\7",
            "\1\10\114\uffff\15\7",
            "\1\10\114\uffff\15\7",
            "\1\10\114\uffff\15\7",
            "\1\10\114\uffff\15\7",
            "\1\12\53\uffff\2\11",
            "\1\12\53\uffff\2\11",
            "",
            ""
    };

    static final short[] DFA39_eot = DFA.unpackEncodedString(DFA39_eotS);
    static final short[] DFA39_eof = DFA.unpackEncodedString(DFA39_eofS);
    static final char[] DFA39_min = DFA.unpackEncodedStringToUnsignedChars(DFA39_minS);
    static final char[] DFA39_max = DFA.unpackEncodedStringToUnsignedChars(DFA39_maxS);
    static final short[] DFA39_accept = DFA.unpackEncodedString(DFA39_acceptS);
    static final short[] DFA39_special = DFA.unpackEncodedString(DFA39_specialS);
    static final short[][] DFA39_transition;

    static {
        int numStates = DFA39_transitionS.length;
        DFA39_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA39_transition[i] = DFA.unpackEncodedString(DFA39_transitionS[i]);
        }
    }

    class DFA39 extends DFA {

        public DFA39(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 39;
            this.eot = DFA39_eot;
            this.eof = DFA39_eof;
            this.min = DFA39_min;
            this.max = DFA39_max;
            this.accept = DFA39_accept;
            this.special = DFA39_special;
            this.transition = DFA39_transition;
        }
        public String getDescription() {
            return "378:1: createCfamColumns[CreateColumnFamilyStatement expr] : (n= term v= createCfamColumnValidator | k= term v= createCfamColumnValidator K_PRIMARY K_KEY );";
        }
    }
 

    public static final BitSet FOLLOW_selectStatement_in_query69 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_insertStatement_in_query81 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_query83 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_updateStatement_in_query93 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_query95 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_batchStatement_in_query105 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_useStatement_in_query115 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_truncateStatement_in_query130 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteStatement_in_query140 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_query142 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createKeyspaceStatement_in_query152 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createColumnFamilyStatement_in_query162 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_createIndexStatement_in_query172 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropIndexStatement_in_query182 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropKeyspaceStatement_in_query194 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropColumnFamilyStatement_in_query204 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_alterTableStatement_in_query214 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_USE_in_useStatement238 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_IDENT_in_useStatement240 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_useStatement244 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_SELECT_in_selectStatement275 = new BitSet(new long[]{0x000040C0000306A0L,0x0000000000080000L});
    public static final BitSet FOLLOW_selectExpression_in_selectStatement291 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_K_COUNT_in_selectStatement323 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
    public static final BitSet FOLLOW_80_in_selectStatement325 = new BitSet(new long[]{0x000040C000030620L,0x0000000000080000L});
    public static final BitSet FOLLOW_selectExpression_in_selectStatement329 = new BitSet(new long[]{0x0000000000000000L,0x0000000000020000L});
    public static final BitSet FOLLOW_81_in_selectStatement331 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_K_FROM_in_selectStatement357 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_selectStatement361 = new BitSet(new long[]{0x000000000000C800L,0x0000000000100000L});
    public static final BitSet FOLLOW_K_USING_in_selectStatement387 = new BitSet(new long[]{0x0000000000001000L});
    public static final BitSet FOLLOW_K_CONSISTENCY_in_selectStatement389 = new BitSet(new long[]{0x0000000000002000L});
    public static final BitSet FOLLOW_K_LEVEL_in_selectStatement391 = new BitSet(new long[]{0x000000000000C000L,0x0000000000100000L});
    public static final BitSet FOLLOW_K_WHERE_in_selectStatement410 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_whereClause_in_selectStatement412 = new BitSet(new long[]{0x0000000000008000L,0x0000000000100000L});
    public static final BitSet FOLLOW_K_LIMIT_in_selectStatement429 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_INTEGER_in_selectStatement433 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_selectStatement450 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_FIRST_in_selectExpression491 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_INTEGER_in_selectExpression495 = new BitSet(new long[]{0x000040C000020620L,0x0000000000080000L});
    public static final BitSet FOLLOW_K_REVERSED_in_selectExpression510 = new BitSet(new long[]{0x000040C000000620L,0x0000000000080000L});
    public static final BitSet FOLLOW_term_in_selectExpression527 = new BitSet(new long[]{0x0000000000000002L,0x0000000000040000L});
    public static final BitSet FOLLOW_82_in_selectExpression544 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_selectExpression548 = new BitSet(new long[]{0x0000000000000002L,0x0000000000040000L});
    public static final BitSet FOLLOW_term_in_selectExpression564 = new BitSet(new long[]{0x0000000000040000L});
    public static final BitSet FOLLOW_RANGEOP_in_selectExpression566 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_selectExpression570 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_83_in_selectExpression582 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_relation_in_whereClause625 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_K_AND_in_whereClause641 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_relation_in_whereClause645 = new BitSet(new long[]{0x0000000000080002L});
    public static final BitSet FOLLOW_term_in_whereClause661 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_K_IN_in_whereClause676 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
    public static final BitSet FOLLOW_80_in_whereClause678 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_whereClause682 = new BitSet(new long[]{0x0000000000000000L,0x0000000000060000L});
    public static final BitSet FOLLOW_82_in_whereClause705 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_whereClause709 = new BitSet(new long[]{0x0000000000000000L,0x0000000000060000L});
    public static final BitSet FOLLOW_81_in_whereClause716 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_INSERT_in_insertStatement757 = new BitSet(new long[]{0x0000000000400000L});
    public static final BitSet FOLLOW_K_INTO_in_insertStatement759 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_insertStatement763 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
    public static final BitSet FOLLOW_80_in_insertStatement787 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_insertStatement791 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_82_in_insertStatement795 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_insertStatement799 = new BitSet(new long[]{0x0000000000000000L,0x0000000000060000L});
    public static final BitSet FOLLOW_81_in_insertStatement807 = new BitSet(new long[]{0x0000000000800000L});
    public static final BitSet FOLLOW_K_VALUES_in_insertStatement817 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
    public static final BitSet FOLLOW_80_in_insertStatement829 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_insertStatement833 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_82_in_insertStatement837 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_insertStatement841 = new BitSet(new long[]{0x0000000000000000L,0x0000000000060000L});
    public static final BitSet FOLLOW_81_in_insertStatement847 = new BitSet(new long[]{0x0000000000000802L});
    public static final BitSet FOLLOW_usingClause_in_insertStatement859 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_USING_in_usingClause889 = new BitSet(new long[]{0x0000000003001000L});
    public static final BitSet FOLLOW_usingClauseObjective_in_usingClause891 = new BitSet(new long[]{0x0000000003081002L});
    public static final BitSet FOLLOW_K_AND_in_usingClause896 = new BitSet(new long[]{0x0000000003001000L});
    public static final BitSet FOLLOW_usingClauseObjective_in_usingClause899 = new BitSet(new long[]{0x0000000003081002L});
    public static final BitSet FOLLOW_K_USING_in_usingClauseDelete921 = new BitSet(new long[]{0x0000000001001000L});
    public static final BitSet FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete923 = new BitSet(new long[]{0x0000000001081002L});
    public static final BitSet FOLLOW_K_AND_in_usingClauseDelete928 = new BitSet(new long[]{0x0000000001001000L});
    public static final BitSet FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete931 = new BitSet(new long[]{0x0000000001081002L});
    public static final BitSet FOLLOW_K_CONSISTENCY_in_usingClauseDeleteObjective953 = new BitSet(new long[]{0x0000000000002000L});
    public static final BitSet FOLLOW_K_LEVEL_in_usingClauseDeleteObjective955 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TIMESTAMP_in_usingClauseDeleteObjective966 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_INTEGER_in_usingClauseDeleteObjective970 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_usingClauseDeleteObjective_in_usingClauseObjective990 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TTL_in_usingClauseObjective999 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_INTEGER_in_usingClauseObjective1003 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_BEGIN_in_batchStatement1043 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_K_BATCH_in_batchStatement1045 = new BitSet(new long[]{0x00000000A0200800L});
    public static final BitSet FOLLOW_usingClause_in_batchStatement1049 = new BitSet(new long[]{0x00000000A0200800L});
    public static final BitSet FOLLOW_batchStatementObjective_in_batchStatement1067 = new BitSet(new long[]{0x00000000B0200800L,0x0000000000100000L});
    public static final BitSet FOLLOW_84_in_batchStatement1069 = new BitSet(new long[]{0x00000000B0200800L});
    public static final BitSet FOLLOW_batchStatementObjective_in_batchStatement1078 = new BitSet(new long[]{0x00000000B0200800L,0x0000000000100000L});
    public static final BitSet FOLLOW_84_in_batchStatement1080 = new BitSet(new long[]{0x00000000B0200800L});
    public static final BitSet FOLLOW_K_APPLY_in_batchStatement1094 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_K_BATCH_in_batchStatement1096 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_batchStatement1098 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_insertStatement_in_batchStatementObjective1129 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_updateStatement_in_batchStatementObjective1142 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_deleteStatement_in_batchStatementObjective1155 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_UPDATE_in_updateStatement1189 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_updateStatement1193 = new BitSet(new long[]{0x0000000040000800L});
    public static final BitSet FOLLOW_usingClause_in_updateStatement1219 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_K_SET_in_updateStatement1235 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_termPairWithOperation_in_updateStatement1237 = new BitSet(new long[]{0x0000000000004000L,0x0000000000040000L});
    public static final BitSet FOLLOW_82_in_updateStatement1241 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_termPairWithOperation_in_updateStatement1243 = new BitSet(new long[]{0x0000000000004000L,0x0000000000040000L});
    public static final BitSet FOLLOW_K_WHERE_in_updateStatement1258 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_updateStatement1264 = new BitSet(new long[]{0x0000000000100000L,0x0000000000200000L});
    public static final BitSet FOLLOW_85_in_updateStatement1267 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_updateStatement1271 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_IN_in_updateStatement1349 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
    public static final BitSet FOLLOW_80_in_updateStatement1351 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_termList_in_updateStatement1355 = new BitSet(new long[]{0x0000000000000000L,0x0000000000020000L});
    public static final BitSet FOLLOW_81_in_updateStatement1359 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DELETE_in_deleteStatement1401 = new BitSet(new long[]{0x000040C000000720L});
    public static final BitSet FOLLOW_termList_in_deleteStatement1417 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_K_FROM_in_deleteStatement1433 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_deleteStatement1437 = new BitSet(new long[]{0x0000000000004800L});
    public static final BitSet FOLLOW_usingClauseDelete_in_deleteStatement1463 = new BitSet(new long[]{0x0000000000004000L});
    public static final BitSet FOLLOW_K_WHERE_in_deleteStatement1479 = new BitSet(new long[]{0x000040C000000622L});
    public static final BitSet FOLLOW_term_in_deleteStatement1485 = new BitSet(new long[]{0x0000000000100000L,0x0000000000200000L});
    public static final BitSet FOLLOW_85_in_deleteStatement1488 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_deleteStatement1492 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_IN_in_deleteStatement1543 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
    public static final BitSet FOLLOW_80_in_deleteStatement1545 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_termList_in_deleteStatement1549 = new BitSet(new long[]{0x0000000000000000L,0x0000000000020000L});
    public static final BitSet FOLLOW_81_in_deleteStatement1553 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createKeyspaceStatement1616 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_K_KEYSPACE_in_createKeyspaceStatement1618 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_createKeyspaceStatement1622 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_K_WITH_in_createKeyspaceStatement1646 = new BitSet(new long[]{0x0000000800000020L});
    public static final BitSet FOLLOW_set_in_createKeyspaceStatement1651 = new BitSet(new long[]{0x0000000000000000L,0x0000000000200000L});
    public static final BitSet FOLLOW_85_in_createKeyspaceStatement1661 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_createKeyspaceStatement1665 = new BitSet(new long[]{0x0000000000080000L,0x0000000000100000L});
    public static final BitSet FOLLOW_K_AND_in_createKeyspaceStatement1693 = new BitSet(new long[]{0x0000000800000020L});
    public static final BitSet FOLLOW_set_in_createKeyspaceStatement1697 = new BitSet(new long[]{0x0000000000000000L,0x0000000000200000L});
    public static final BitSet FOLLOW_85_in_createKeyspaceStatement1707 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_createKeyspaceStatement1711 = new BitSet(new long[]{0x0000000000080000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_createKeyspaceStatement1740 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createColumnFamilyStatement1775 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_createColumnFamilyStatement1777 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_createColumnFamilyStatement1781 = new BitSet(new long[]{0x0000000400000000L,0x0000000000110000L});
    public static final BitSet FOLLOW_80_in_createColumnFamilyStatement1805 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_createCfamColumns_in_createColumnFamilyStatement1807 = new BitSet(new long[]{0x0000000000000000L,0x0000000000060000L});
    public static final BitSet FOLLOW_82_in_createColumnFamilyStatement1812 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_createCfamColumns_in_createColumnFamilyStatement1814 = new BitSet(new long[]{0x0000000000000000L,0x0000000000060000L});
    public static final BitSet FOLLOW_81_in_createColumnFamilyStatement1820 = new BitSet(new long[]{0x0000000400000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_K_WITH_in_createColumnFamilyStatement1833 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_IDENT_in_createColumnFamilyStatement1837 = new BitSet(new long[]{0x0000000000000000L,0x0000000000200000L});
    public static final BitSet FOLLOW_85_in_createColumnFamilyStatement1839 = new BitSet(new long[]{0x0000008000000620L,0x00000007FFC00000L});
    public static final BitSet FOLLOW_createCfamKeywordArgument_in_createColumnFamilyStatement1843 = new BitSet(new long[]{0x0000000000080000L,0x0000000000100000L});
    public static final BitSet FOLLOW_K_AND_in_createColumnFamilyStatement1859 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_IDENT_in_createColumnFamilyStatement1863 = new BitSet(new long[]{0x0000000000000000L,0x0000000000200000L});
    public static final BitSet FOLLOW_85_in_createColumnFamilyStatement1865 = new BitSet(new long[]{0x0000008000000620L,0x00000007FFC00000L});
    public static final BitSet FOLLOW_createCfamKeywordArgument_in_createColumnFamilyStatement1869 = new BitSet(new long[]{0x0000000000080000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_createColumnFamilyStatement1891 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_term_in_createCfamColumns1911 = new BitSet(new long[]{0x0000000000000200L,0x00000007FFC00000L});
    public static final BitSet FOLLOW_createCfamColumnValidator_in_createCfamColumns1915 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_term_in_createCfamColumns1927 = new BitSet(new long[]{0x0000000000000200L,0x00000007FFC00000L});
    public static final BitSet FOLLOW_createCfamColumnValidator_in_createCfamColumns1931 = new BitSet(new long[]{0x0000002000000000L});
    public static final BitSet FOLLOW_K_PRIMARY_in_createCfamColumns1933 = new BitSet(new long[]{0x0000004000000000L});
    public static final BitSet FOLLOW_K_KEY_in_createCfamColumns1935 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_comparatorType_in_createCfamColumnValidator1958 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_LITERAL_in_createCfamColumnValidator1968 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_comparatorType_in_createCfamKeywordArgument1991 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_createCfamKeywordArgument2003 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_CREATE_in_createIndexStatement2044 = new BitSet(new long[]{0x0000010000000000L});
    public static final BitSet FOLLOW_K_INDEX_in_createIndexStatement2046 = new BitSet(new long[]{0x0000020000000020L});
    public static final BitSet FOLLOW_IDENT_in_createIndexStatement2051 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_K_ON_in_createIndexStatement2055 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_createIndexStatement2059 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
    public static final BitSet FOLLOW_80_in_createIndexStatement2073 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_createIndexStatement2077 = new BitSet(new long[]{0x0000000000000000L,0x0000000000020000L});
    public static final BitSet FOLLOW_81_in_createIndexStatement2079 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_createIndexStatement2081 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropIndexStatement2117 = new BitSet(new long[]{0x0000010000000000L});
    public static final BitSet FOLLOW_K_INDEX_in_dropIndexStatement2119 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_dropIndexStatement2123 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_dropIndexStatement2137 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropKeyspaceStatement2168 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_K_KEYSPACE_in_dropKeyspaceStatement2170 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_dropKeyspaceStatement2174 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_dropKeyspaceStatement2188 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_ALTER_in_alterTableStatement2222 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_alterTableStatement2224 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_alterTableStatement2228 = new BitSet(new long[]{0x00002C0000000000L});
    public static final BitSet FOLLOW_K_ALTER_in_alterTableStatement2256 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_alterTableStatement2278 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_K_TYPE_in_alterTableStatement2310 = new BitSet(new long[]{0x0000000000000000L,0x00000007FFC00000L});
    public static final BitSet FOLLOW_comparatorType_in_alterTableStatement2314 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_K_ADD_in_alterTableStatement2330 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_alterTableStatement2352 = new BitSet(new long[]{0x0000000000000000L,0x00000007FFC00000L});
    public static final BitSet FOLLOW_comparatorType_in_alterTableStatement2386 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_K_DROP_in_alterTableStatement2402 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_alterTableStatement2424 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_alterTableStatement2446 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_DROP_in_dropColumnFamilyStatement2476 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_K_COLUMNFAMILY_in_dropColumnFamilyStatement2478 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_dropColumnFamilyStatement2482 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_dropColumnFamilyStatement2496 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_comparatorType0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_KEY_in_term2588 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_LITERAL_in_term2594 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_term2600 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UUID_in_term2606 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IDENT_in_term2612 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FLOAT_in_term2618 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_term_in_termList2652 = new BitSet(new long[]{0x0000000000000002L,0x0000000000040000L});
    public static final BitSet FOLLOW_82_in_termList2657 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_termList2661 = new BitSet(new long[]{0x0000000000000002L,0x0000000000040000L});
    public static final BitSet FOLLOW_term_in_termPair2688 = new BitSet(new long[]{0x0000000000000000L,0x0000000000200000L});
    public static final BitSet FOLLOW_85_in_termPair2690 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_termPair2694 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_term_in_termPairWithOperation2716 = new BitSet(new long[]{0x0000000000000000L,0x0000000000200000L});
    public static final BitSet FOLLOW_85_in_termPairWithOperation2718 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_termPairWithOperation2723 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_term_in_termPairWithOperation2737 = new BitSet(new long[]{0x0000000000000000L,0x0000001800000000L});
    public static final BitSet FOLLOW_99_in_termPairWithOperation2741 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_termPairWithOperation2745 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_100_in_termPairWithOperation2779 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_termPairWithOperation2783 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_term_in_relation2812 = new BitSet(new long[]{0x0000000000000000L,0x000001E000200000L});
    public static final BitSet FOLLOW_set_in_relation2816 = new BitSet(new long[]{0x000040C000000620L});
    public static final BitSet FOLLOW_term_in_relation2838 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_K_TRUNCATE_in_truncateStatement2868 = new BitSet(new long[]{0x0000000000000620L});
    public static final BitSet FOLLOW_set_in_truncateStatement2872 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_endStmnt_in_truncateStatement2888 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_84_in_endStmnt2905 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_endStmnt2909 = new BitSet(new long[]{0x0000000000000002L});

}