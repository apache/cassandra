/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

grammar Cql;

options {
    language = Java;
}

@header {
    package org.apache.cassandra.cql3;

    import java.util.Map;
    import java.util.HashMap;
    import java.util.Collections;
    import java.util.List;
    import java.util.ArrayList;

    import org.apache.cassandra.cql3.statements.*;
    import org.apache.cassandra.utils.Pair;
    import org.apache.cassandra.thrift.ConsistencyLevel;
    import org.apache.cassandra.thrift.InvalidRequestException;
}

@members {
    private List<String> recognitionErrors = new ArrayList<String>();
    private int currentBindMarkerIdx = -1;

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

    // used by UPDATE of the counter columns to validate if '-' was supplied by user
    public void validateMinusSupplied(Object op, final Term value, IntStream stream) throws MissingTokenException
    {
        if (op == null && (value.isBindMarker() || Long.parseLong(value.getText()) > 0))
            throw new MissingTokenException(102, stream, value);
    }

}

@lexer::header {
    package org.apache.cassandra.cql3;

    import org.apache.cassandra.thrift.InvalidRequestException;
}

@lexer::members {
    List<Token> tokens = new ArrayList<Token>();

    public void emit(Token token)
    {
        state.token = token;
        tokens.add(token);
    }

    public Token nextToken()
    {
        super.nextToken();
        if (tokens.size() == 0)
            return Token.EOF_TOKEN;
        return tokens.remove(0);
    }

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
}

/** STATEMENTS **/

query returns [ParsedStatement stmnt]
    : st=cqlStatement (';')* EOF { $stmnt = st; }
    ;

cqlStatement returns [ParsedStatement stmt]
    @after{ if (stmt != null) stmt.setBoundTerms(currentBindMarkerIdx + 1); }
    : st1= selectStatement             { $stmt = st1; }
    | st2= insertStatement             { $stmt = st2; }
    | st3= updateStatement             { $stmt = st3; }
    | st4= batchStatement              { $stmt = st4; }
    | st5= deleteStatement             { $stmt = st5; }
    | st6= useStatement                { $stmt = st6; }
    | st7= truncateStatement           { $stmt = st7; }
    | st8= createKeyspaceStatement     { $stmt = st8; }
    | st9= createColumnFamilyStatement { $stmt = st9; }
    | st10=createIndexStatement        { $stmt = st10; }
    | st11=dropKeyspaceStatement       { $stmt = st11; }
    | st12=dropColumnFamilyStatement   { $stmt = st12; }
    | st13=dropIndexStatement          { $stmt = st13; }
    | st14=alterTableStatement         { $stmt = st14; }
    ;

/*
 * USE <KEYSPACE>;
 */
useStatement returns [UseStatement stmt]
    : K_USE ks=keyspaceName { $stmt = new UseStatement(ks); }
    ;

/**
 * SELECT <expression>
 * FROM <CF>
 * USING CONSISTENCY <LEVEL>
 * WHERE KEY = "key1" AND COL > 1 AND COL < 100
 * LIMIT <NUMBER>;
 */
selectStatement returns [SelectStatement.RawStatement expr]
    @init {
        boolean isCount = false;
        ConsistencyLevel cLevel = ConsistencyLevel.ONE;
        int limit = 10000;
        boolean reversed = false;
        ColumnIdentifier orderBy = null;
    }
    : K_SELECT ( sclause=selectClause | (K_COUNT '(' sclause=selectClause ')' { isCount = true; }) )
      K_FROM cf=columnFamilyName
      ( K_USING K_CONSISTENCY K_LEVEL { cLevel = ConsistencyLevel.valueOf($K_LEVEL.text.toUpperCase()); } )?
      ( K_WHERE wclause=whereClause )?
      ( K_ORDER K_BY c=cident { orderBy = c; } (K_ASC | K_DESC { reversed = true; })? )?
      ( K_LIMIT rows=INTEGER { limit = Integer.parseInt($rows.text); } )?
      {
          SelectStatement.Parameters params = new SelectStatement.Parameters(cLevel,
                                                                             limit,
                                                                             orderBy,
                                                                             reversed,
                                                                             isCount);
          $expr = new SelectStatement.RawStatement(cf, params, sclause, wclause);
      }
    ;

selectClause returns [List<ColumnIdentifier> expr]
    : ids=cidentList { $expr = ids; }
    | '\*'           { $expr = Collections.<ColumnIdentifier>emptyList();}
    ;

whereClause returns [List<Relation> clause]
    @init{ $clause = new ArrayList<Relation>(); }
    : first=relation { $clause.add(first); } (K_AND next=relation { $clause.add(next); })*
    ;

/**
 * INSERT INTO <CF> (<column>, <column>, <column>, ...)
 * VALUES (<value>, <value>, <value>, ...)
 * USING CONSISTENCY <level> AND TIMESTAMP <long>;
 *
 * Consistency level is set to ONE by default
 */
insertStatement returns [UpdateStatement expr]
    @init {
        Attributes attrs = new Attributes();
        List<ColumnIdentifier> columnNames  = new ArrayList<ColumnIdentifier>();
        List<Term> columnValues = new ArrayList<Term>();
    }
    : K_INSERT K_INTO cf=columnFamilyName
          '(' c1=cident { columnNames.add(c1); }  ( ',' cn=cident { columnNames.add(cn); } )+ ')'
        K_VALUES
          '(' v1=term { columnValues.add(v1); } ( ',' vn=term { columnValues.add(vn); } )+ ')'
        ( usingClause[attrs] )?
      {
          $expr = new UpdateStatement(cf, columnNames, columnValues, attrs);
      }
    ;

usingClause[Attributes attrs]
    : K_USING usingClauseObjective[attrs] ( K_AND? usingClauseObjective[attrs] )*
    ;

usingClauseDelete[Attributes attrs]
    : K_USING usingClauseDeleteObjective[attrs] ( K_AND? usingClauseDeleteObjective[attrs] )*
    ;

usingClauseDeleteObjective[Attributes attrs]
    : K_CONSISTENCY K_LEVEL  { attrs.cLevel = ConsistencyLevel.valueOf($K_LEVEL.text.toUpperCase()); }
    | K_TIMESTAMP ts=INTEGER { attrs.timestamp = Long.valueOf($ts.text); }
    ;

usingClauseObjective[Attributes attrs]
    : usingClauseDeleteObjective[attrs]
    | K_TTL t=INTEGER { attrs.timeToLive = Integer.valueOf($t.text); }
    ;

/**
 * UPDATE <CF>
 * USING CONSISTENCY <level> AND TIMESTAMP <long>
 * SET name1 = value1, name2 = value2
 * WHERE key = value;
 */
updateStatement returns [UpdateStatement expr]
    @init {
        Attributes attrs = new Attributes();
        Map<ColumnIdentifier, Operation> columns = new HashMap<ColumnIdentifier, Operation>();
    }
    : K_UPDATE cf=columnFamilyName
      ( usingClause[attrs] )?
      K_SET termPairWithOperation[columns] (',' termPairWithOperation[columns])*
      K_WHERE wclause=whereClause
      {
          return new UpdateStatement(cf, columns, wclause, attrs);
      }
    ;

/**
 * DELETE name1, name2
 * FROM <CF>
 * USING CONSISTENCY <level> AND TIMESTAMP <long>
 * WHERE KEY = keyname;
 */
deleteStatement returns [DeleteStatement expr]
    @init {
        Attributes attrs = new Attributes();
        List<ColumnIdentifier> columnsList = Collections.emptyList();
    }
    : K_DELETE ( ids=cidentList { columnsList = ids; } )?
      K_FROM cf=columnFamilyName
      ( usingClauseDelete[attrs] )?
      K_WHERE wclause=whereClause
      {
          return new DeleteStatement(cf, columnsList, wclause, attrs);
      }
    ;


/**
 * BEGIN BATCH [USING CONSISTENCY <LVL>]
 *   UPDATE <CF> SET name1 = value1 WHERE KEY = keyname1;
 *   UPDATE <CF> SET name2 = value2 WHERE KEY = keyname2;
 *   UPDATE <CF> SET name3 = value3 WHERE KEY = keyname3;
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH [USING CONSISTENCY <LVL>]
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH [USING CONSISTENCY <LVL>]
 *   DELETE name1, name2 FROM <CF> WHERE key = <key>
 *   DELETE name3, name4 FROM <CF> WHERE key = <key>
 *   ...
 * APPLY BATCH
 */
batchStatement returns [BatchStatement expr]
    @init {
        Attributes attrs = new Attributes();
        List<ModificationStatement> statements = new ArrayList<ModificationStatement>();
    }
    : K_BEGIN K_BATCH ( usingClause[attrs] )?
          s1=batchStatementObjective ';'? { statements.add(s1); } ( sN=batchStatementObjective ';'? { statements.add(sN); } )*
      K_APPLY K_BATCH
      {
          return new BatchStatement(statements, attrs);
      }
    ;

batchStatementObjective returns [ModificationStatement statement]
    : i=insertStatement  { $statement = i; }
    | u=updateStatement  { $statement = u; }
    | d=deleteStatement  { $statement = d; }
    ;

/**
 * CREATE KEYSPACE <KEYSPACE> WITH attr1 = value1 AND attr2 = value2;
 */
createKeyspaceStatement returns [CreateKeyspaceStatement expr]
    : K_CREATE K_KEYSPACE ks=keyspaceName
      K_WITH props=properties { $expr = new CreateKeyspaceStatement(ks, props); }
    ;

/**
 * CREATE COLUMNFAMILY <CF> (
 *     <name1> <type>,
 *     <name2> <type>,
 *     <name3> <type>
 * ) WITH <property> = <value> AND ...;
 */
createColumnFamilyStatement returns [CreateColumnFamilyStatement.RawStatement expr]
    : K_CREATE K_COLUMNFAMILY cf=columnFamilyName { $expr = new CreateColumnFamilyStatement.RawStatement(cf); }
      cfamDefinition[expr]
    ;

cfamDefinition[CreateColumnFamilyStatement.RawStatement expr]
    : '(' cfamColumns[expr] ( ',' cfamColumns[expr]? )* ')'
      ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )*)?
    ;

cfamColumns[CreateColumnFamilyStatement.RawStatement expr]
    : k=cident v=comparatorType { $expr.addDefinition(k, v); } (K_PRIMARY K_KEY { $expr.setKeyAlias(k); })?
    | K_PRIMARY K_KEY '(' k=cident { $expr.setKeyAlias(k); } (',' c=cident { $expr.addColumnAlias(c); } )* ')'
    ;

cfamProperty[CreateColumnFamilyStatement.RawStatement expr]
    : k=property '=' v=propertyValue { $expr.addProperty(k, v); }
    | K_COMPACT K_STORAGE { $expr.setCompactStorage(); }
    ;

/**
 * CREATE INDEX [indexName] ON columnFamily (columnName);
 */
createIndexStatement returns [CreateIndexStatement expr]
    : K_CREATE K_INDEX (idxName=IDENT)? K_ON cf=columnFamilyName '(' id=cident ')'
      { $expr = new CreateIndexStatement(cf, $idxName.text, id); }
    ;

/**
 * ALTER COLUMN FAMILY <CF> ALTER <column> TYPE <newtype>;
 * ALTER COLUMN FAMILY <CF> ADD <column> <newtype>;
 * ALTER COLUMN FAMILY <CF> DROP <column>;
 * ALTER COLUMN FAMILY <CF> WITH <property> = <value>;
 */
alterTableStatement returns [AlterTableStatement expr]
    @init {
        AlterTableStatement.Type type = null;
        String validator = null;
        ColumnIdentifier columnName = null;
        Map<String, String> propertyMap = null;
    }
    : K_ALTER K_COLUMNFAMILY cf=columnFamilyName
          ( K_ALTER id=cident K_TYPE v=comparatorType { type = AlterTableStatement.Type.ALTER; }
          | K_ADD   id=cident v=comparatorType        { type = AlterTableStatement.Type.ADD; }
          | K_DROP  id=cident                         { type = AlterTableStatement.Type.DROP; }
          | K_WITH  props=properties                  { type = AlterTableStatement.Type.OPTS; }
          )
    {
        $expr = new AlterTableStatement(cf, type, id, v, props);
    }
    ;

/**
 * DROP KEYSPACE <KSP>;
 */
dropKeyspaceStatement returns [DropKeyspaceStatement ksp]
    : K_DROP K_KEYSPACE ks=keyspaceName { $ksp = new DropKeyspaceStatement(ks); }
    ;

/**
 * DROP COLUMNFAMILY <CF>;
 */
dropColumnFamilyStatement returns [DropColumnFamilyStatement stmt]
    : K_DROP K_COLUMNFAMILY cf=columnFamilyName { $stmt = new DropColumnFamilyStatement(cf); }
    ;

/**
 * DROP INDEX <INDEX_NAME>
 */
dropIndexStatement returns [DropIndexStatement expr]
    :
      K_DROP K_INDEX index=IDENT
      { $expr = new DropIndexStatement($index.text); }
    ;

/**
  * TRUNCATE <CF>;
  */
truncateStatement returns [TruncateStatement stmt]
    : K_TRUNCATE cf=columnFamilyName { $stmt = new TruncateStatement(cf); }
    ;


/** DEFINITIONS **/

// Column Identifiers
cident returns [ColumnIdentifier id]
    : t=( IDENT | UUID | INTEGER ) { $id = new ColumnIdentifier($t.text, false); }
    | t=QUOTED_NAME                { $id = new ColumnIdentifier($t.text, true); }
    ;

// Keyspace & Column family names
keyspaceName returns [String id]
    @init { CFName name = new CFName(); }
    : cfOrKsName[name, true] { $id = name.getKeyspace(); }
    ;

columnFamilyName returns [CFName name]
    @init { $name = new CFName(); }
    : (cfOrKsName[name, true] '.')? cfOrKsName[name, false]
    ;

cfOrKsName[CFName name, boolean isKs]
    : t=IDENT        { if (isKs) $name.setKeyspace($t.text, false); else $name.setColumnFamily($t.text, false); }
    | t=QUOTED_NAME  { if (isKs) $name.setKeyspace($t.text, true); else $name.setColumnFamily($t.text, true); }
    ;

cidentList returns [List<ColumnIdentifier> items]
    @init{ $items = new ArrayList<ColumnIdentifier>(); }
    :  t1=cident { $items.add(t1); } (',' tN=cident { $items.add(tN); })*
    ;

// Values (includes prepared statement markers)
term returns [Term term]
    : t=(STRING_LITERAL | UUID | IDENT | INTEGER | FLOAT ) { $term = new Term($t.text, $t.type); }
    | t=QMARK                                              { $term = new Term($t.text, $t.type, ++currentBindMarkerIdx); }
    ;

intTerm returns [Term integer]
    : t=INTEGER { $integer = new Term($t.text, $t.type); }
    | t=QMARK   { $integer = new Term($t.text, $t.type, ++currentBindMarkerIdx); }
    ;

termPairWithOperation[Map<ColumnIdentifier, Operation> columns]
    : key=cident '='
        ( value=term { columns.put(key, new Operation(value)); }
        | c=cident ( '+'     v=intTerm { columns.put(key, new Operation(c, Operation.Type.PLUS, v)); }
                   | op='-'? v=intTerm
                     {
                       validateMinusSupplied(op, v, input);
                       if (op == null)
                           v = new Term(-(Long.valueOf(v.getText())), v.getType());
                       columns.put(key, new Operation(c, Operation.Type.MINUS, v));
                     }
                    )
        )
    ;

property returns [String str]
    : p=(COMPIDENT | IDENT) { $str = $p.text; }
    ;

propertyValue returns [String str]
    : v=(STRING_LITERAL | IDENT | INTEGER | FLOAT) { $str = $v.text; }
    ;

properties returns [Map<String, String> props]
    @init{ $props = new HashMap<String, String>(); }
    : k1=property '=' v1=propertyValue { $props.put(k1, v1); } (K_AND kn=property '=' vn=propertyValue { $props.put(kn, vn); } )*
    ;

relation returns [Relation rel]
    : name=cident type=('=' | '<' | '<=' | '>=' | '>') t=term { $rel = new Relation($name.id, $type.text, $t.term); }
    | name=cident K_IN { $rel = Relation.createInRelation($name.id); }
      '(' f1=term { $rel.addInValue(f1); } (',' fN=term { $rel.addInValue(fN); } )* ')'
    ;

comparatorType returns [String str]
    : c=(IDENT | STRING_LITERAL | K_TIMESTAMP) { $str = $c.text; }
    ;


// Case-insensitive keywords
K_SELECT:      S E L E C T;
K_FROM:        F R O M;
K_WHERE:       W H E R E;
K_AND:         A N D;
K_KEY:         K E Y;
K_INSERT:      I N S E R T;
K_UPDATE:      U P D A T E;
K_WITH:        W I T H;
K_LIMIT:       L I M I T;
K_USING:       U S I N G;
K_CONSISTENCY: C O N S I S T E N C Y;
K_LEVEL:       ( O N E
               | Q U O R U M
               | A L L
               | A N Y
               | L O C A L '_' Q U O R U M
               | E A C H '_' Q U O R U M
               )
               ;
K_USE:         U S E;
K_COUNT:       C O U N T;
K_SET:         S E T;
K_BEGIN:       B E G I N;
K_APPLY:       A P P L Y;
K_BATCH:       B A T C H;
K_TRUNCATE:    T R U N C A T E;
K_DELETE:      D E L E T E;
K_IN:          I N;
K_CREATE:      C R E A T E;
K_KEYSPACE:    ( K E Y S P A C E
                 | S C H E M A );
K_COLUMNFAMILY:( C O L U M N F A M I L Y
                 | T A B L E );
K_INDEX:       I N D E X;
K_ON:          O N;
K_DROP:        D R O P;
K_PRIMARY:     P R I M A R Y;
K_INTO:        I N T O;
K_VALUES:      V A L U E S;
K_TIMESTAMP:   T I M E S T A M P;
K_TTL:         T T L;
K_ALTER:       A L T E R;
K_ADD:         A D D;
K_TYPE:        T Y P E;
K_COMPACT:     C O M P A C T;
K_STORAGE:     S T O R A G E;
K_ORDER:       O R D E R;
K_BY:          B Y;
K_ASC:         A S C;
K_DESC:        D E S C;

// Case-insensitive alpha characters
fragment A: ('a'|'A');
fragment B: ('b'|'B');
fragment C: ('c'|'C');
fragment D: ('d'|'D');
fragment E: ('e'|'E');
fragment F: ('f'|'F');
fragment G: ('g'|'G');
fragment H: ('h'|'H');
fragment I: ('i'|'I');
fragment J: ('j'|'J');
fragment K: ('k'|'K');
fragment L: ('l'|'L');
fragment M: ('m'|'M');
fragment N: ('n'|'N');
fragment O: ('o'|'O');
fragment P: ('p'|'P');
fragment Q: ('q'|'Q');
fragment R: ('r'|'R');
fragment S: ('s'|'S');
fragment T: ('t'|'T');
fragment U: ('u'|'U');
fragment V: ('v'|'V');
fragment W: ('w'|'W');
fragment X: ('x'|'X');
fragment Y: ('y'|'Y');
fragment Z: ('z'|'Z');

STRING_LITERAL
    @init{ StringBuilder b = new StringBuilder(); }
    @after{ setText(b.toString()); }
    : '\'' (c=~('\'') { b.appendCodePoint(c);} | '\'' '\'' { b.appendCodePoint('\''); })* '\''
    ;

QUOTED_NAME
    @init{ StringBuilder b = new StringBuilder(); }
    @after{ setText(b.toString()); }
    : '\"' (c=~('\"') { b.appendCodePoint(c); } | '\"' '\"' { b.appendCodePoint('\"'); })* '\"'
    ;

fragment DIGIT
    : '0'..'9'
    ;

fragment LETTER
    : ('A'..'Z' | 'a'..'z')
    ;

fragment HEX
    : ('A'..'F' | 'a'..'f' | '0'..'9')
    ;

INTEGER
    : '-'? DIGIT+
    ;

QMARK
    : '?'
    ;

/*
 * Normally a lexer only emits one token at a time, but ours is tricked out
 * to support multiple (see @lexer::members near the top of the grammar).
 */
FLOAT
    : INTEGER '.' INTEGER
    ;

IDENT
    : LETTER (LETTER | DIGIT | '_')*
    ;

COMPIDENT
    : IDENT ( ':' (IDENT | INTEGER))+
    ;

UUID
    : HEX HEX HEX HEX HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX
    ;

WS
    : (' ' | '\t' | '\n' | '\r')+ { $channel = HIDDEN; }
    ;

COMMENT
    : ('--' | '//') .* ('\n'|'\r') { $channel = HIDDEN; }
    ;

MULTILINE_COMMENT
    : '/*' .* '*/' { $channel = HIDDEN; }
    ;
