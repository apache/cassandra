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

parser grammar Parser;

options {
    language = Java;
}

@members {
    private final List<ErrorListener> listeners = new ArrayList<ErrorListener>();
    protected final List<ColumnIdentifier> bindVariables = new ArrayList<ColumnIdentifier>();

    public static final Set<String> reservedTypeNames = new HashSet<String>()
    {{
        add("byte");
        add("complex");
        add("enum");
        add("date");
        add("interval");
        add("macaddr");
        add("bitstring");
    }};

    public AbstractMarker.Raw newBindVariables(ColumnIdentifier name)
    {
        AbstractMarker.Raw marker = new AbstractMarker.Raw(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public AbstractMarker.INRaw newINBindVariables(ColumnIdentifier name)
    {
        AbstractMarker.INRaw marker = new AbstractMarker.INRaw(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public Tuples.Raw newTupleBindVariables(ColumnIdentifier name)
    {
        Tuples.Raw marker = new Tuples.Raw(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public Tuples.INRaw newTupleINBindVariables(ColumnIdentifier name)
    {
        Tuples.INRaw marker = new Tuples.INRaw(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public Json.Marker newJsonBindVariables(ColumnIdentifier name)
    {
        Json.Marker marker = new Json.Marker(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public void addErrorListener(ErrorListener listener)
    {
        this.listeners.add(listener);
    }

    public void removeErrorListener(ErrorListener listener)
    {
        this.listeners.remove(listener);
    }

    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
    {
        for (int i = 0, m = listeners.size(); i < m; i++)
            listeners.get(i).syntaxError(this, tokenNames, e);
    }

    protected void addRecognitionError(String msg)
    {
        for (int i = 0, m = listeners.size(); i < m; i++)
            listeners.get(i).syntaxError(this, msg);
    }

    public Map<String, String> convertPropertyMap(Maps.Literal map)
    {
        if (map == null || map.entries == null || map.entries.isEmpty())
            return Collections.<String, String>emptyMap();

        Map<String, String> res = new HashMap<>(map.entries.size());

        for (Pair<Term.Raw, Term.Raw> entry : map.entries)
        {
            // Because the parser tries to be smart and recover on error (to
            // allow displaying more than one error I suppose), we have null
            // entries in there. Just skip those, a proper error will be thrown in the end.
            if (entry.left == null || entry.right == null)
                break;

            if (!(entry.left instanceof Constants.Literal))
            {
                String msg = "Invalid property name: " + entry.left;
                if (entry.left instanceof AbstractMarker.Raw)
                    msg += " (bind variables are not supported in DDL queries)";
                addRecognitionError(msg);
                break;
            }
            if (!(entry.right instanceof Constants.Literal))
            {
                String msg = "Invalid property value: " + entry.right + " for property: " + entry.left;
                if (entry.right instanceof AbstractMarker.Raw)
                    msg += " (bind variables are not supported in DDL queries)";
                addRecognitionError(msg);
                break;
            }

            if (res.put(((Constants.Literal)entry.left).getRawText(), ((Constants.Literal)entry.right).getRawText()) != null)
            {
                addRecognitionError(String.format("Multiple definition for property " + ((Constants.Literal)entry.left).getRawText()));
            }
        }

        return res;
    }

    public void addRawUpdate(List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations, ColumnIdentifier key, Operation.RawUpdate update)
    {
        for (Pair<ColumnIdentifier, Operation.RawUpdate> p : operations)
        {
            if (p.left.equals(key) && !p.right.isCompatibleWith(update))
                addRecognitionError("Multiple incompatible setting of column " + key);
        }
        operations.add(Pair.create(key, update));
    }

    public Set<Permission> filterPermissions(Set<Permission> permissions, IResource resource)
    {
        if (resource == null)
            return Collections.emptySet();
        Set<Permission> filtered = new HashSet<>(permissions);
        filtered.retainAll(resource.applicablePermissions());
        if (filtered.isEmpty())
            addRecognitionError("Resource type " + resource.getClass().getSimpleName() +
                                    " does not support any of the requested permissions");

        return filtered;
    }

    public String canonicalizeObjectName(String s, boolean enforcePattern)
    {
        // these two conditions are here because technically they are valid
        // ObjectNames, but we want to restrict their use without adding unnecessary
        // work to JMXResource construction as that also happens on hotter code paths
        if ("".equals(s))
            addRecognitionError("Empty JMX object name supplied");

        if ("*:*".equals(s))
            addRecognitionError("Please use ALL MBEANS instead of wildcard pattern");

        try
        {
            javax.management.ObjectName objectName = javax.management.ObjectName.getInstance(s);
            if (enforcePattern && !objectName.isPattern())
                addRecognitionError("Plural form used, but non-pattern JMX object name specified (" + s + ")");
            return objectName.getCanonicalName();
        }
        catch (javax.management.MalformedObjectNameException e)
        {
          addRecognitionError(s + " is not a valid JMX object name");
          return s;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Recovery methods are overridden to avoid wasting work on recovering from errors when the result will be
    // ignored anyway.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    protected Object recoverFromMismatchedToken(IntStream input, int ttype, BitSet follow) throws RecognitionException
    {
        throw new MismatchedTokenException(ttype, input);
    }

    @Override
    public void recover(IntStream input, RecognitionException re)
    {
        // Do nothing.
    }
}

/** STATEMENTS **/

cqlStatement returns [CQLStatement.Raw stmt]
    @after{ if (stmt != null) stmt.setBindVariables(bindVariables); }
    : st1= selectStatement                 { $stmt = st1; }
    | st2= insertStatement                 { $stmt = st2; }
    | st3= updateStatement                 { $stmt = st3; }
    | st4= batchStatement                  { $stmt = st4; }
    | st5= deleteStatement                 { $stmt = st5; }
    | st6= useStatement                    { $stmt = st6; }
    | st7= truncateStatement               { $stmt = st7; }
    | st8= createKeyspaceStatement         { $stmt = st8; }
    | st9= createTableStatement            { $stmt = st9; }
    | st10=createIndexStatement            { $stmt = st10; }
    | st11=dropKeyspaceStatement           { $stmt = st11; }
    | st12=dropTableStatement              { $stmt = st12; }
    | st13=dropIndexStatement              { $stmt = st13; }
    | st14=alterTableStatement             { $stmt = st14; }
    | st15=alterKeyspaceStatement          { $stmt = st15; }
    | st16=grantPermissionsStatement       { $stmt = st16; }
    | st17=revokePermissionsStatement      { $stmt = st17; }
    | st18=listPermissionsStatement        { $stmt = st18; }
    | st19=createUserStatement             { $stmt = st19; }
    | st20=alterUserStatement              { $stmt = st20; }
    | st21=dropUserStatement               { $stmt = st21; }
    | st22=listUsersStatement              { $stmt = st22; }
    | st23=createTriggerStatement          { $stmt = st23; }
    | st24=dropTriggerStatement            { $stmt = st24; }
    | st25=createTypeStatement             { $stmt = st25; }
    | st26=alterTypeStatement              { $stmt = st26; }
    | st27=dropTypeStatement               { $stmt = st27; }
    | st28=createFunctionStatement         { $stmt = st28; }
    | st29=dropFunctionStatement           { $stmt = st29; }
    | st30=createAggregateStatement        { $stmt = st30; }
    | st31=dropAggregateStatement          { $stmt = st31; }
    | st32=createRoleStatement             { $stmt = st32; }
    | st33=alterRoleStatement              { $stmt = st33; }
    | st34=dropRoleStatement               { $stmt = st34; }
    | st35=listRolesStatement              { $stmt = st35; }
    | st36=grantRoleStatement              { $stmt = st36; }
    | st37=revokeRoleStatement             { $stmt = st37; }
    | st38=createMaterializedViewStatement { $stmt = st38; }
    | st39=dropMaterializedViewStatement   { $stmt = st39; }
    | st40=alterMaterializedViewStatement  { $stmt = st40; }
    | st41=describeStatement               { $stmt = st41; }
    | st42=addIdentityStatement            { $stmt = st42; }
    | st43=dropIdentityStatement           { $stmt = st43; }
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
 * WHERE KEY = "key1" AND COL > 1 AND COL < 100
 * LIMIT <NUMBER>;
 */
selectStatement returns [SelectStatement.RawStatement expr]
    @init {
        Term.Raw limit = null;
        Term.Raw perPartitionLimit = null;
        List<Ordering.Raw> orderings = new ArrayList<>();
        List<Selectable.Raw> groups = new ArrayList<>();
        boolean allowFiltering = false;
        boolean isJson = false;
    }
    : K_SELECT
        // json is a valid column name. By consequence, we need to resolve the ambiguity for "json - json"
      ( (K_JSON selectClause)=> K_JSON { isJson = true; } )? sclause=selectClause
      K_FROM cf=columnFamilyName
      ( K_WHERE wclause=whereClause )?
      ( K_GROUP K_BY groupByClause[groups] ( ',' groupByClause[groups] )* )?
      ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )?
      ( K_PER K_PARTITION K_LIMIT rows=intValue { perPartitionLimit = rows; } )?
      ( K_LIMIT rows=intValue { limit = rows; } )?
      ( K_ALLOW K_FILTERING  { allowFiltering = true; } )?
      {
          SelectStatement.Parameters params = new SelectStatement.Parameters(orderings,
                                                                             groups,
                                                                             $sclause.isDistinct,
                                                                             allowFiltering,
                                                                             isJson);
          WhereClause where = wclause == null ? WhereClause.empty() : wclause.build();
          $expr = new SelectStatement.RawStatement(cf, params, $sclause.selectors, where, limit, perPartitionLimit);
      }
    ;

selectClause returns [boolean isDistinct, List<RawSelector> selectors]
    @init{ $isDistinct = false; }
    // distinct is a valid column name. By consequence, we need to resolve the ambiguity for "distinct - distinct"
    : (K_DISTINCT selectors)=> K_DISTINCT s=selectors { $isDistinct = true; $selectors = s; }
    | s=selectors { $selectors = s; }
    ;

selectors returns [List<RawSelector> expr]
    : t1=selector { $expr = new ArrayList<RawSelector>(); $expr.add(t1); } (',' tN=selector { $expr.add(tN); })*
    | '\*' { $expr = Collections.<RawSelector>emptyList();}
    ;

selector returns [RawSelector s]
    @init{ ColumnIdentifier alias = null; }
    : us=unaliasedSelector (K_AS c=noncol_ident { alias = c; })? { $s = new RawSelector(us, alias); }
    ;

unaliasedSelector returns [Selectable.Raw s]
    : a=selectionAddition {$s = a;}
    ;

selectionAddition returns [Selectable.Raw s]
    :   l=selectionMultiplication   {$s = l;}
        ( '+' r=selectionMultiplication {$s = Selectable.WithFunction.Raw.newOperation('+', $s, r);}
        | '-' r=selectionMultiplication {$s = Selectable.WithFunction.Raw.newOperation('-', $s, r);}
        )*
    ;

selectionMultiplication returns [Selectable.Raw s]
    :   l=selectionGroup   {$s = l;}
        ( '\*' r=selectionGroup {$s = Selectable.WithFunction.Raw.newOperation('*', $s, r);}
        | '/' r=selectionGroup {$s = Selectable.WithFunction.Raw.newOperation('/', $s, r);}
        | '%' r=selectionGroup {$s = Selectable.WithFunction.Raw.newOperation('\%', $s, r);}
        )*
    ;

selectionGroup returns [Selectable.Raw s]
    : (selectionGroupWithField)=>  f=selectionGroupWithField { $s=f; }
    | g=selectionGroupWithoutField { $s=g; }
    | '-' g=selectionGroup {$s = Selectable.WithFunction.Raw.newNegation(g);}
    ;

selectionGroupWithField returns [Selectable.Raw s]
    : g=selectionGroupWithoutField m=selectorModifier[g] {$s = m;}
    ;

selectorModifier[Selectable.Raw receiver] returns [Selectable.Raw s]
    : f=fieldSelectorModifier[receiver] m=selectorModifier[f] { $s = m; }
    | '[' ss=collectionSubSelection[receiver] ']' m=selectorModifier[ss] { $s = m; }
    | { $s = receiver; }
    ;

fieldSelectorModifier[Selectable.Raw receiver] returns [Selectable.Raw s]
    : '.' fi=fident { $s = new Selectable.WithFieldSelection.Raw(receiver, fi); }
    ;

collectionSubSelection [Selectable.Raw receiver] returns [Selectable.Raw s]
    @init { boolean isSlice=false; }
    : ( t1=term ( { isSlice=true; } RANGE (t2=term)? )?
      | RANGE { isSlice=true; } t2=term
      ) {
          $s = isSlice
             ? new Selectable.WithSliceSelection.Raw(receiver, t1, t2)
             : new Selectable.WithElementSelection.Raw(receiver, t1);
      }
     ;

selectionGroupWithoutField returns [Selectable.Raw s]
    @init { Selectable.Raw tmp = null; }
    @after { $s = tmp; }
    : sn=simpleUnaliasedSelector  { tmp=sn; }
    | (selectionTypeHint)=> h=selectionTypeHint { tmp=h; }
    | t=selectionTupleOrNestedSelector { tmp=t; }
    | l=selectionList { tmp=l; }
    | m=selectionMapOrSet { tmp=m; }
    // UDTs are equivalent to maps from the syntax point of view, so the final decision will be done in Selectable.WithMapOrUdt
    ;

selectionTypeHint returns [Selectable.Raw s]
    : '(' ct=comparatorType ')' a=selectionGroupWithoutField { $s = new Selectable.WithTypeHint.Raw(ct, a); }
    ;

selectionList returns [Selectable.Raw s]
    @init { List<Selectable.Raw> l = new ArrayList<>(); }
    @after { $s = new Selectable.WithArrayLiteral.Raw(l); }
    : '[' ( t1=unaliasedSelector { l.add(t1); } ( ',' tn=unaliasedSelector { l.add(tn); } )* )? ']'
    ;

selectionMapOrSet returns [Selectable.Raw s]
    : '{' t1=unaliasedSelector ( m=selectionMap[t1] { $s = m; } | st=selectionSet[t1] { $s = st; }) '}'
    | '{' '}' { $s = new Selectable.WithSet.Raw(Collections.emptyList());}
    ;

selectionMap [Selectable.Raw k1] returns [Selectable.Raw s]
    @init { List<Pair<Selectable.Raw, Selectable.Raw>> m = new ArrayList<>(); }
    @after { $s = new Selectable.WithMapOrUdt.Raw(m); }
      : ':' v1=unaliasedSelector   { m.add(Pair.create(k1, v1)); } ( ',' kn=unaliasedSelector ':' vn=unaliasedSelector { m.add(Pair.create(kn, vn)); } )*
      ;

selectionSet [Selectable.Raw t1] returns [Selectable.Raw s]
    @init { List<Selectable.Raw> l = new ArrayList<>(); l.add(t1); }
    @after { $s = new Selectable.WithSet.Raw(l); }
      : ( ',' tn=unaliasedSelector { l.add(tn); } )*
      ;

selectionTupleOrNestedSelector returns [Selectable.Raw s]
    @init { List<Selectable.Raw> l = new ArrayList<>(); }
    @after { $s = new Selectable.BetweenParenthesesOrWithTuple.Raw(l); }
    : '(' t1=unaliasedSelector { l.add(t1); } (',' tn=unaliasedSelector { l.add(tn); } )* ')'
    ;

/*
 * A single selection. The core of it is selecting a column, but we also allow any term and function, as well as
 * sub-element selection for UDT.
 */
simpleUnaliasedSelector returns [Selectable.Raw s]
    : c=sident                                   { $s = c; }
    | l=selectionLiteral                         { $s = new Selectable.WithTerm.Raw(l); }
    | f=selectionFunction                        { $s = f; }
    ;

selectionFunction returns [Selectable.Raw s]
    : K_COUNT        '(' '\*' ')'                                    { $s = Selectable.WithFunction.Raw.newCountRowsFunction(); }
    | K_MAXWRITETIME '(' c=sident m=selectorModifier[c] ')'          { $s = new Selectable.WritetimeOrTTL.Raw(c, m, Selectable.WritetimeOrTTL.Kind.MAX_WRITE_TIME); }
    | K_WRITETIME    '(' c=sident m=selectorModifier[c] ')'          { $s = new Selectable.WritetimeOrTTL.Raw(c, m, Selectable.WritetimeOrTTL.Kind.WRITE_TIME); }
    | K_TTL          '(' c=sident m=selectorModifier[c] ')'          { $s = new Selectable.WritetimeOrTTL.Raw(c, m, Selectable.WritetimeOrTTL.Kind.TTL); }
    | K_CAST         '(' sn=unaliasedSelector K_AS t=native_type ')' { $s = new Selectable.WithCast.Raw(sn, t);}
    | f=functionName args=selectionFunctionArgs                      { $s = new Selectable.WithFunction.Raw(f, args); }
    ;

selectionLiteral returns [Term.Raw value]
    : c=constant                     { $value = c; }
    | K_NULL                         { $value = Constants.NULL_LITERAL; }
    | ':' id=noncol_ident            { $value = newBindVariables(id); }
    | QMARK                          { $value = newBindVariables(null); }
    ;

selectionFunctionArgs returns [List<Selectable.Raw> a]
    @init{ $a = new ArrayList<>(); }
    : '(' (s1=unaliasedSelector { $a.add(s1); }
          ( ',' sn=unaliasedSelector { $a.add(sn); } )*)?
      ')'
    ;

sident returns [Selectable.RawIdentifier id]
    : t=IDENT              { $id = Selectable.RawIdentifier.forUnquoted($t.text); }
    | t=QUOTED_NAME        { $id = Selectable.RawIdentifier.forQuoted($t.text); }
    | k=unreserved_keyword { $id = Selectable.RawIdentifier.forUnquoted(k); }
    ;

whereClause returns [WhereClause.Builder clause]
    @init{ $clause = new WhereClause.Builder(); }
    : relationOrExpression[$clause] (K_AND relationOrExpression[$clause])*
    ;

relationOrExpression [WhereClause.Builder clause]
    : relation[$clause]
    | customIndexExpression[$clause]
    ;

customIndexExpression [WhereClause.Builder clause]
    @init{QualifiedName name = new QualifiedName();}
    : 'expr(' idxName[name] ',' t=term ')' { clause.add(new CustomIndexExpression(name, t));}
    ;

orderByClause[List<Ordering.Raw> orderings]
    @init{
        Ordering.Direction direction = Ordering.Direction.ASC;
    }
    : c=cident (K_ANN K_OF t=term)? (K_ASC | K_DESC { direction = Ordering.Direction.DESC; })?
    {
        Ordering.Raw.Expression expr = (t == null)
            ? new Ordering.Raw.SingleColumn(c)
            : new Ordering.Raw.Ann(c, t);
        orderings.add(new Ordering.Raw(expr, direction));
    }
    ;

groupByClause[List<Selectable.Raw> groups]
    : s=unaliasedSelector { groups.add(s); }
    ;

/**
 * INSERT INTO <CF> (<column>, <column>, <column>, ...)
 * VALUES (<value>, <value>, <value>, ...)
 * USING TIMESTAMP <long>;
 *
 */
insertStatement returns [ModificationStatement.Parsed expr]
    : K_INSERT K_INTO cf=columnFamilyName
        ( st1=normalInsertStatement[cf] { $expr = st1; }
        | K_JSON st2=jsonInsertStatement[cf] { $expr = st2; })
    ;

normalInsertStatement [QualifiedName qn] returns [UpdateStatement.ParsedInsert expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        List<ColumnIdentifier> columnNames  = new ArrayList<>();
        List<Term.Raw> values = new ArrayList<>();
        boolean ifNotExists = false;
    }
    : '(' c1=cident { columnNames.add(c1); }  ( ',' cn=cident { columnNames.add(cn); } )* ')'
      K_VALUES
      '(' v1=term { values.add(v1); } ( ',' vn=term { values.add(vn); } )* ')'
      ( K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
      ( usingClause[attrs] )?
      {
          $expr = new UpdateStatement.ParsedInsert(qn, attrs, columnNames, values, ifNotExists);
      }
    ;

jsonInsertStatement [QualifiedName qn] returns [UpdateStatement.ParsedInsertJson expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        boolean ifNotExists = false;
        boolean defaultUnset = false;
    }
    : val=jsonValue
      ( K_DEFAULT ( K_NULL | ( { defaultUnset = true; } K_UNSET) ) )?
      ( K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
      ( usingClause[attrs] )?
      {
          $expr = new UpdateStatement.ParsedInsertJson(qn, attrs, val, defaultUnset, ifNotExists);
      }
    ;

jsonValue returns [Json.Raw value]
    : s=STRING_LITERAL { $value = new Json.Literal($s.text); }
    | ':' id=noncol_ident     { $value = newJsonBindVariables(id); }
    | QMARK            { $value = newJsonBindVariables(null); }
    ;

usingClause[Attributes.Raw attrs]
    : K_USING usingClauseObjective[attrs] ( K_AND usingClauseObjective[attrs] )*
    ;

usingClauseObjective[Attributes.Raw attrs]
    : K_TIMESTAMP ts=intValue { attrs.timestamp = ts; }
    | K_TTL t=intValue { attrs.timeToLive = t; }
    ;

/**
 * UPDATE <CF>
 * USING TIMESTAMP <long>
 * SET name1 = value1, name2 = value2
 * WHERE key = value;
 * [IF (EXISTS | name = value, ...)];
 */
updateStatement returns [UpdateStatement.ParsedUpdate expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations = new ArrayList<>();
        boolean ifExists = false;
    }
    : K_UPDATE cf=columnFamilyName
      ( usingClause[attrs] )?
      K_SET columnOperation[operations] (',' columnOperation[operations])*
      K_WHERE wclause=whereClause
      ( K_IF ( K_EXISTS { ifExists = true; } | conditions=updateConditions ))?
      {
          $expr = new UpdateStatement.ParsedUpdate(cf,
                                                   attrs,
                                                   operations,
                                                   wclause.build(),
                                                   conditions == null ? Collections.<Pair<ColumnIdentifier, ColumnCondition.Raw>>emptyList() : conditions,
                                                   ifExists);
     }
    ;

updateConditions returns [List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions]
    @init { conditions = new ArrayList<Pair<ColumnIdentifier, ColumnCondition.Raw>>(); }
    : columnCondition[conditions] ( K_AND columnCondition[conditions] )*
    ;


/**
 * DELETE name1, name2
 * FROM <CF>
 * USING TIMESTAMP <long>
 * WHERE KEY = keyname
   [IF (EXISTS | name = value, ...)];
 */
deleteStatement returns [DeleteStatement.Parsed expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        List<Operation.RawDeletion> columnDeletions = Collections.emptyList();
        boolean ifExists = false;
    }
    : K_DELETE ( dels=deleteSelection { columnDeletions = dels; } )?
      K_FROM cf=columnFamilyName
      ( usingClauseDelete[attrs] )?
      K_WHERE wclause=whereClause
      ( K_IF ( K_EXISTS { ifExists = true; } | conditions=updateConditions ))?
      {
          $expr = new DeleteStatement.Parsed(cf,
                                             attrs,
                                             columnDeletions,
                                             wclause.build(),
                                             conditions == null ? Collections.<Pair<ColumnIdentifier, ColumnCondition.Raw>>emptyList() : conditions,
                                             ifExists);
      }
    ;

deleteSelection returns [List<Operation.RawDeletion> operations]
    : { $operations = new ArrayList<Operation.RawDeletion>(); }
          t1=deleteOp { $operations.add(t1); }
          (',' tN=deleteOp { $operations.add(tN); })*
    ;

deleteOp returns [Operation.RawDeletion op]
    : c=cident                { $op = new Operation.ColumnDeletion(c); }
    | c=cident '[' t=term ']' { $op = new Operation.ElementDeletion(c, t); }
    | c=cident '.' field=fident { $op = new Operation.FieldDeletion(c, field); }
    ;

usingClauseDelete[Attributes.Raw attrs]
    : K_USING K_TIMESTAMP ts=intValue { attrs.timestamp = ts; }
    ;

/**
 * BEGIN BATCH
 *   UPDATE <CF> SET name1 = value1 WHERE KEY = keyname1;
 *   UPDATE <CF> SET name2 = value2 WHERE KEY = keyname2;
 *   UPDATE <CF> SET name3 = value3 WHERE KEY = keyname3;
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH
 *   DELETE name1, name2 FROM <CF> WHERE key = <key>
 *   DELETE name3, name4 FROM <CF> WHERE key = <key>
 *   ...
 * APPLY BATCH
 */
batchStatement returns [BatchStatement.Parsed expr]
    @init {
        BatchStatement.Type type = BatchStatement.Type.LOGGED;
        List<ModificationStatement.Parsed> statements = new ArrayList<ModificationStatement.Parsed>();
        Attributes.Raw attrs = new Attributes.Raw();
    }
    : K_BEGIN
      ( K_UNLOGGED { type = BatchStatement.Type.UNLOGGED; } | K_COUNTER { type = BatchStatement.Type.COUNTER; } )?
      K_BATCH ( usingClause[attrs] )?
          ( s=batchStatementObjective ';'? { statements.add(s); } )*
      K_APPLY K_BATCH
      {
          $expr = new BatchStatement.Parsed(type, attrs, statements);
      }
    ;

batchStatementObjective returns [ModificationStatement.Parsed statement]
    : i=insertStatement  { $statement = i; }
    | u=updateStatement  { $statement = u; }
    | d=deleteStatement  { $statement = d; }
    ;

createAggregateStatement returns [CreateAggregateStatement.Raw stmt]
    @init {
        boolean orReplace = false;
        boolean ifNotExists = false;

        List<CQL3Type.Raw> argTypes = new ArrayList<>();
    }
    : K_CREATE (K_OR K_REPLACE { orReplace = true; })?
      K_AGGREGATE
      (K_IF K_NOT K_EXISTS { ifNotExists = true; })?
      fn=functionName
      '('
        (
          v=comparatorType { argTypes.add(v); }
          ( ',' v=comparatorType { argTypes.add(v); } )*
        )?
      ')'
      K_SFUNC sfunc = allowedFunctionName
      K_STYPE stype = comparatorType
      (
        K_FINALFUNC ffunc = allowedFunctionName
      )?
      (
        K_INITCOND ival = term
      )?
      { $stmt = new CreateAggregateStatement.Raw(fn, argTypes, stype, sfunc, ffunc, ival, orReplace, ifNotExists); }
    ;

dropAggregateStatement returns [DropAggregateStatement.Raw stmt]
    @init {
        boolean ifExists = false;
        List<CQL3Type.Raw> argTypes = new ArrayList<>();
        boolean argsSpecified = false;
    }
    : K_DROP K_AGGREGATE
      (K_IF K_EXISTS { ifExists = true; } )?
      fn=functionName
      (
        '('
          (
            v=comparatorType { argTypes.add(v); }
            ( ',' v=comparatorType { argTypes.add(v); } )*
          )?
        ')'
        { argsSpecified = true; }
      )?
      { $stmt = new DropAggregateStatement.Raw(fn, argTypes, argsSpecified, ifExists); }
    ;

createFunctionStatement returns [CreateFunctionStatement.Raw stmt]
    @init {
        boolean orReplace = false;
        boolean ifNotExists = false;

        List<ColumnIdentifier> argNames = new ArrayList<>();
        List<CQL3Type.Raw> argTypes = new ArrayList<>();
        boolean calledOnNullInput = false;
    }
    : K_CREATE (K_OR K_REPLACE { orReplace = true; })?
      K_FUNCTION
      (K_IF K_NOT K_EXISTS { ifNotExists = true; })?
      fn=functionName
      '('
        (
          k=noncol_ident v=comparatorType { argNames.add(k); argTypes.add(v); }
          ( ',' k=noncol_ident v=comparatorType { argNames.add(k); argTypes.add(v); } )*
        )?
      ')'
      ( (K_RETURNS K_NULL) | (K_CALLED { calledOnNullInput=true; })) K_ON K_NULL K_INPUT
      K_RETURNS returnType = comparatorType
      K_LANGUAGE language = IDENT
      K_AS body = STRING_LITERAL
      { $stmt = new CreateFunctionStatement.Raw(
          fn, argNames, argTypes, returnType, calledOnNullInput, $language.text.toLowerCase(), $body.text, orReplace, ifNotExists);
      }
    ;

dropFunctionStatement returns [DropFunctionStatement.Raw stmt]
    @init {
        boolean ifExists = false;
        List<CQL3Type.Raw> argTypes = new ArrayList<>();
        boolean argsSpecified = false;
    }
    : K_DROP K_FUNCTION
      (K_IF K_EXISTS { ifExists = true; } )?
      fn=functionName
      (
        '('
          (
            v=comparatorType { argTypes.add(v); }
            ( ',' v=comparatorType { argTypes.add(v); } )*
          )?
        ')'
        { argsSpecified = true; }
      )?
      { $stmt = new DropFunctionStatement.Raw(fn, argTypes, argsSpecified, ifExists); }
    ;

/**
 * CREATE KEYSPACE [IF NOT EXISTS] <KEYSPACE> WITH attr1 = value1 AND attr2 = value2;
 */
createKeyspaceStatement returns [CreateKeyspaceStatement.Raw stmt]
    @init {
        KeyspaceAttributes attrs = new KeyspaceAttributes();
        boolean ifNotExists = false;
    }
    : K_CREATE K_KEYSPACE (K_IF K_NOT K_EXISTS { ifNotExists = true; } )? ks=keyspaceName
      K_WITH properties[attrs] { $stmt = new CreateKeyspaceStatement.Raw(ks, attrs, ifNotExists); }
    ;

/**
 * CREATE TABLE [IF NOT EXISTS] <CF> (
 *     <name1> <type>,
 *     <name2> <type>,
 *     <name3> <type>
 * ) WITH <property> = <value> AND ...;
 */
createTableStatement returns [CreateTableStatement.Raw stmt]
    @init { boolean ifNotExists = false; }
    : K_CREATE K_COLUMNFAMILY (K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
      cf=columnFamilyName { $stmt = new CreateTableStatement.Raw(cf, ifNotExists); }
      tableDefinition[stmt]
    ;

tableDefinition[CreateTableStatement.Raw stmt]
    : '(' tableColumns[stmt] ( ',' tableColumns[stmt]? )* ')'
      ( K_WITH tableProperty[stmt] ( K_AND tableProperty[stmt] )*)?
    ;

tableColumns[CreateTableStatement.Raw stmt]
    @init { boolean isStatic = false; }
    : k=ident v=comparatorType (K_STATIC { isStatic = true; })? (mask=columnMask)? { $stmt.addColumn(k, v, isStatic, mask); }
        (K_PRIMARY K_KEY { $stmt.setPartitionKeyColumn(k); })?
    | K_PRIMARY K_KEY '(' tablePartitionKey[stmt] (',' c=ident { $stmt.markClusteringColumn(c); } )* ')'
    ;

columnMask returns [ColumnMask.Raw mask]
    @init { List<Term.Raw> arguments = new ArrayList<>(); }
    : K_MASKED K_WITH name=functionName columnMaskArguments[arguments] { $mask = new ColumnMask.Raw(name, arguments); }
    | K_MASKED K_WITH K_DEFAULT { $mask = new ColumnMask.Raw(FunctionName.nativeFunction("mask_default"), arguments); }
    ;

columnMaskArguments[List<Term.Raw> arguments]
    : '('  ')' | '(' c=term { arguments.add(c); } (',' cn=term { arguments.add(cn); })* ')'
    ;

tablePartitionKey[CreateTableStatement.Raw stmt]
    @init {List<ColumnIdentifier> l = new ArrayList<ColumnIdentifier>();}
    @after{ $stmt.setPartitionKeyColumns(l); }
    : k1=ident { l.add(k1);}
    | '(' k1=ident { l.add(k1); } ( ',' kn=ident { l.add(kn); } )* ')'
    ;

tableProperty[CreateTableStatement.Raw stmt]
    : property[stmt.attrs]
    | K_COMPACT K_STORAGE { $stmt.setCompactStorage(); }
    | K_CLUSTERING K_ORDER K_BY '(' tableClusteringOrder[stmt] (',' tableClusteringOrder[stmt])* ')'
    ;

tableClusteringOrder[CreateTableStatement.Raw stmt]
    @init{ boolean ascending = true; }
    : k=ident (K_ASC | K_DESC { ascending = false; } ) { $stmt.extendClusteringOrder(k, ascending); }
    ;

/**
 * CREATE TYPE foo (
 *    <name1> <type1>,
 *    <name2> <type2>,
 *    ....
 * )
 */
createTypeStatement returns [CreateTypeStatement.Raw stmt]
    @init { boolean ifNotExists = false; }
    : K_CREATE K_TYPE (K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
         tn=userTypeName { $stmt = new CreateTypeStatement.Raw(tn, ifNotExists); }
         '(' typeColumns[stmt] ( ',' typeColumns[stmt]? )* ')'
    ;

typeColumns[CreateTypeStatement.Raw stmt]
    : k=fident v=comparatorType { $stmt.addField(k, v); }
    ;

/**
 * CREATE INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>);
 * CREATE CUSTOM INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>) USING <indexClass>;
 */
createIndexStatement returns [CreateIndexStatement.Raw stmt]
    @init {
        IndexAttributes props = new IndexAttributes();
        boolean ifNotExists = false;
        QualifiedName name = new QualifiedName();
        List<IndexTarget.Raw> targets = new ArrayList<>();
    }
    : K_CREATE (K_CUSTOM { props.isCustom = true; })? K_INDEX (K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
        (idxName[name])? K_ON cf=columnFamilyName '(' (indexIdent[targets] (',' indexIdent[targets])*)? ')'
        (K_USING cls=STRING_LITERAL { props.customClass = $cls.text; })?
        (K_WITH properties[props])?
      { $stmt = new CreateIndexStatement.Raw(cf, name, targets, props, ifNotExists); }
    ;

indexIdent [List<IndexTarget.Raw> targets]
    : c=cident                   { $targets.add(IndexTarget.Raw.simpleIndexOn(c)); }
    | K_VALUES '(' c=cident ')'  { $targets.add(IndexTarget.Raw.valuesOf(c)); }
    | K_KEYS '(' c=cident ')'    { $targets.add(IndexTarget.Raw.keysOf(c)); }
    | K_ENTRIES '(' c=cident ')' { $targets.add(IndexTarget.Raw.keysAndValuesOf(c)); }
    | K_FULL '(' c=cident ')'    { $targets.add(IndexTarget.Raw.fullCollection(c)); }
    ;

/**
 * CREATE MATERIALIZED VIEW <viewName> AS
 *  SELECT <columns>
 *  FROM <CF>
 *  WHERE <pkColumns> IS NOT NULL
 *  PRIMARY KEY (<pkColumns>)
 *  WITH <property> = <value> AND ...;
 */
createMaterializedViewStatement returns [CreateViewStatement.Raw stmt]
    @init {
        boolean ifNotExists = false;
    }
    : K_CREATE K_MATERIALIZED K_VIEW (K_IF K_NOT K_EXISTS { ifNotExists = true; })? cf=columnFamilyName K_AS
        K_SELECT sclause=selectors K_FROM basecf=columnFamilyName
        (K_WHERE wclause=whereClause)?
        {
             WhereClause where = wclause == null ? WhereClause.empty() : wclause.build();
             $stmt = new CreateViewStatement.Raw(basecf, cf, sclause, where, ifNotExists);
        }
        viewPrimaryKey[stmt]
        ( K_WITH viewProperty[stmt] ( K_AND viewProperty[stmt] )*)?
    ;

viewPrimaryKey[CreateViewStatement.Raw stmt]
    : K_PRIMARY K_KEY '(' viewPartitionKey[stmt] (',' c=ident { $stmt.markClusteringColumn(c); } )* ')'
    ;

viewPartitionKey[CreateViewStatement.Raw stmt]
    @init {List<ColumnIdentifier> l = new ArrayList<ColumnIdentifier>();}
    @after{ $stmt.setPartitionKeyColumns(l); }
    : k1=ident { l.add(k1);}
    | '(' k1=ident { l.add(k1); } ( ',' kn=ident { l.add(kn); } )* ')'
    ;

viewProperty[CreateViewStatement.Raw stmt]
    : property[stmt.attrs]
    | K_COMPACT K_STORAGE { throw new SyntaxException("COMPACT STORAGE tables are not allowed starting with version 4.0"); }
    | K_CLUSTERING K_ORDER K_BY '(' viewClusteringOrder[stmt] (',' viewClusteringOrder[stmt])* ')'
    ;

viewClusteringOrder[CreateViewStatement.Raw stmt]
    @init{ boolean ascending = true; }
    : k=ident (K_ASC | K_DESC { ascending = false; } ) { $stmt.extendClusteringOrder(k, ascending); }
    ;

/**
 * CREATE TRIGGER triggerName ON columnFamily USING 'triggerClass';
 */
createTriggerStatement returns [CreateTriggerStatement.Raw stmt]
    @init {
        boolean ifNotExists = false;
    }
    : K_CREATE K_TRIGGER (K_IF K_NOT K_EXISTS { ifNotExists = true; } )? (name=ident)
        K_ON cf=columnFamilyName K_USING cls=STRING_LITERAL
      { $stmt = new CreateTriggerStatement.Raw(cf, name.toString(), $cls.text, ifNotExists); }
    ;

/**
 * DROP TRIGGER [IF EXISTS] triggerName ON columnFamily;
 */
dropTriggerStatement returns [DropTriggerStatement.Raw stmt]
     @init { boolean ifExists = false; }
    : K_DROP K_TRIGGER (K_IF K_EXISTS { ifExists = true; } )? (name=ident) K_ON cf=columnFamilyName
      { $stmt = new DropTriggerStatement.Raw(cf, name.toString(), ifExists); }
    ;

/**
 * ALTER KEYSPACE [IF EXISTS] <KS> WITH <property> = <value>;
 */
alterKeyspaceStatement returns [AlterKeyspaceStatement.Raw stmt]
    @init {
     KeyspaceAttributes attrs = new KeyspaceAttributes();
     boolean ifExists = false;
    }
    : K_ALTER K_KEYSPACE (K_IF K_EXISTS { ifExists = true; } )? ks=keyspaceName
        K_WITH properties[attrs] { $stmt = new AlterKeyspaceStatement.Raw(ks, attrs, ifExists); }
    ;

/**
 * ALTER TABLE <table> ALTER <column> TYPE <newtype>;
 * ALTER TABLE [IF EXISTS] <table> ALTER [IF EXISTS] <column> MASKED WITH <maskFunction>);
 * ALTER TABLE [IF EXISTS] <table> ALTER [IF EXISTS] <column> DROP MASKED;
 * ALTER TABLE [IF EXISTS] <table> ADD [IF NOT EXISTS] <column> <newtype> <maskFunction>; | ALTER TABLE [IF EXISTS] <table> ADD [IF NOT EXISTS] (<column> <newtype> <maskFunction>, <column1> <newtype1>  <maskFunction1>..... <column n> <newtype n>  <maskFunction n>)
 * ALTER TABLE [IF EXISTS] <table> DROP [IF EXISTS] <column>; | ALTER TABLE [IF EXISTS] <table> DROP [IF EXISTS] ( <column>,<column1>.....<column n>)
 * ALTER TABLE [IF EXISTS] <table> RENAME [IF EXISTS] <column> TO <column>;
 * ALTER TABLE [IF EXISTS] <table> WITH <property> = <value>;
 */
alterTableStatement returns [AlterTableStatement.Raw stmt]
    @init { boolean ifExists = false; }
    : K_ALTER K_COLUMNFAMILY (K_IF K_EXISTS { ifExists = true; } )?
      cf=columnFamilyName { $stmt = new AlterTableStatement.Raw(cf, ifExists); }
      (
        K_ALTER id=cident K_TYPE v=comparatorType { $stmt.alter(id, v); }

      | K_ALTER ( K_IF K_EXISTS { $stmt.ifColumnExists(true); } )? id=cident
              ( mask=columnMask { $stmt.mask(id, mask); }
              | K_DROP K_MASKED { $stmt.mask(id, null); } )

      | K_ADD ( K_IF K_NOT K_EXISTS { $stmt.ifColumnNotExists(true); } )?
              (        id=ident  v=comparatorType  b=isStaticColumn (m=columnMask)? { $stmt.add(id,  v,  b, m);  }
               | ('('  id1=ident v1=comparatorType b1=isStaticColumn (m1=columnMask)? { $stmt.add(id1, v1, b1, m1); }
                 ( ',' idn=ident vn=comparatorType bn=isStaticColumn (mn=columnMask)? { $stmt.add(idn, vn, bn, mn); mn=null; } )* ')') )

      | K_DROP ( K_IF K_EXISTS { $stmt.ifColumnExists(true); } )?
               (       id=ident { $stmt.drop(id);  }
               | ('('  id1=ident { $stmt.drop(id1); }
                 ( ',' idn=ident { $stmt.drop(idn); } )* ')') )
               ( K_USING K_TIMESTAMP t=INTEGER { $stmt.timestamp(Long.parseLong(Constants.Literal.integer($t.text).getText())); } )?

      | K_RENAME ( K_IF K_EXISTS { $stmt.ifColumnExists(true); } )?
               (        id1=ident K_TO toId1=ident { $stmt.rename(id1, toId1); }
                ( K_AND idn=ident K_TO toIdn=ident { $stmt.rename(idn, toIdn); } )* )

      | K_DROP K_COMPACT K_STORAGE { $stmt.dropCompactStorage(); }

      | K_WITH properties[$stmt.attrs] { $stmt.attrs(); }
      )
    ;

isStaticColumn returns [boolean isStaticColumn]
    @init { boolean isStatic = false; }
    : (K_STATIC { isStatic=true; })? { $isStaticColumn = isStatic; }
    ;

alterMaterializedViewStatement returns [AlterViewStatement.Raw stmt]
    @init {
        TableAttributes attrs = new TableAttributes();
        boolean ifExists = false;
    }
    : K_ALTER K_MATERIALIZED K_VIEW (K_IF K_EXISTS { ifExists = true; } )? name=columnFamilyName
          K_WITH properties[attrs]
    {
        $stmt = new AlterViewStatement.Raw(name, attrs, ifExists);
    }
    ;


/**
 * ALTER TYPE [IF EXISTS] <name> ALTER <field> TYPE <newtype>;
 * ALTER TYPE [IF EXISTS] <name> ADD [IF NOT EXISTS]<field> <newtype>;
 * ALTER TYPE [IF EXISTS] <name> RENAME [IF EXISTS] <field> TO <newtype> AND ...;
 */
alterTypeStatement returns [AlterTypeStatement.Raw stmt]
    @init {
        boolean ifExists = false;
    }
    : K_ALTER K_TYPE (K_IF K_EXISTS { ifExists = true; } )? name=userTypeName { $stmt = new AlterTypeStatement.Raw(name, ifExists); }
      (
        K_ALTER   f=fident K_TYPE v=comparatorType { $stmt.alter(f, v); }

      | K_ADD (K_IF K_NOT K_EXISTS { $stmt.ifFieldNotExists(true); } )?     f=fident v=comparatorType        { $stmt.add(f, v); }

      | K_RENAME (K_IF K_EXISTS { $stmt.ifFieldExists(true); } )? f1=fident K_TO toF1=fident        { $stmt.rename(f1, toF1); }
         ( K_AND fn=fident K_TO toFn=fident        { $stmt.rename(fn, toFn); } )*
      )
    ;

/**
 * DROP KEYSPACE [IF EXISTS] <KSP>;
 */
dropKeyspaceStatement returns [DropKeyspaceStatement.Raw stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_KEYSPACE (K_IF K_EXISTS { ifExists = true; } )? ks=keyspaceName { $stmt = new DropKeyspaceStatement.Raw(ks, ifExists); }
    ;

/**
 * DROP TABLE [IF EXISTS] <table>;
 */
dropTableStatement returns [DropTableStatement.Raw stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_COLUMNFAMILY (K_IF K_EXISTS { ifExists = true; } )? name=columnFamilyName { $stmt = new DropTableStatement.Raw(name, ifExists); }
    ;

/**
 * DROP TYPE <name>;
 */
dropTypeStatement returns [DropTypeStatement.Raw stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_TYPE (K_IF K_EXISTS { ifExists = true; } )? name=userTypeName { $stmt = new DropTypeStatement.Raw(name, ifExists); }
    ;

/**
 * DROP INDEX [IF EXISTS] <INDEX_NAME>
 */
dropIndexStatement returns [DropIndexStatement.Raw stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_INDEX (K_IF K_EXISTS { ifExists = true; } )? index=indexName
      { $stmt = new DropIndexStatement.Raw(index, ifExists); }
    ;

/**
 * DROP MATERIALIZED VIEW [IF EXISTS] <view_name>
 */
dropMaterializedViewStatement returns [DropViewStatement.Raw stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_MATERIALIZED K_VIEW (K_IF K_EXISTS { ifExists = true; } )? cf=columnFamilyName
      { $stmt = new DropViewStatement.Raw(cf, ifExists); }
    ;

/**
  * TRUNCATE <CF>;
  */
truncateStatement returns [TruncateStatement stmt]
    : K_TRUNCATE (K_COLUMNFAMILY)? cf=columnFamilyName { $stmt = new TruncateStatement(cf); }
    ;

/**
 * GRANT <permission>[, <permission>]* | ALL ON <resource> TO <rolename>
 */
grantPermissionsStatement returns [GrantPermissionsStatement stmt]
    : K_GRANT
          permissionOrAll
      K_ON
          resource
      K_TO
          grantee=userOrRoleName
      { $stmt = new GrantPermissionsStatement(filterPermissions($permissionOrAll.perms, $resource.res), $resource.res, grantee); }
    ;

/**
 * REVOKE <permission>[, <permission>]* | ALL ON <resource> FROM <rolename>
 */
revokePermissionsStatement returns [RevokePermissionsStatement stmt]
    : K_REVOKE
          permissionOrAll
      K_ON
          resource
      K_FROM
          revokee=userOrRoleName
      { $stmt = new RevokePermissionsStatement(filterPermissions($permissionOrAll.perms, $resource.res), $resource.res, revokee); }
    ;

/**
 * GRANT ROLE <rolename> TO <grantee>
 */
grantRoleStatement returns [GrantRoleStatement stmt]
    : K_GRANT
          role=userOrRoleName
      K_TO
          grantee=userOrRoleName
      { $stmt = new GrantRoleStatement(role, grantee); }
    ;

/**
 * REVOKE ROLE <rolename> FROM <revokee>
 */
revokeRoleStatement returns [RevokeRoleStatement stmt]
    : K_REVOKE
          role=userOrRoleName
      K_FROM
          revokee=userOrRoleName
      { $stmt = new RevokeRoleStatement(role, revokee); }
    ;

listPermissionsStatement returns [ListPermissionsStatement stmt]
    @init {
        IResource resource = null;
        boolean recursive = true;
        RoleName grantee = new RoleName();
    }
    : K_LIST
          permissionOrAll
      ( K_ON resource { resource = $resource.res; } )?
      ( K_OF roleName[grantee] )?
      ( K_NORECURSIVE { recursive = false; } )?
      { $stmt = new ListPermissionsStatement($permissionOrAll.perms, resource, grantee, recursive); }
    ;

permission returns [Permission perm]
    : p=(K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE | K_DESCRIBE | K_EXECUTE | K_UNMASK | K_SELECT_MASKED)
    { $perm = Permission.valueOf($p.text.toUpperCase()); }
    ;

permissionOrAll returns [Set<Permission> perms]
    : K_ALL ( K_PERMISSIONS )?       { $perms = Permission.ALL; }
    | p=permission ( K_PERMISSION )? { $perms = EnumSet.of($p.perm); } ( ',' p=permission ( K_PERMISSION )? { $perms.add($p.perm); } )*
    ;

resource returns [IResource res]
    : d=dataResource { $res = $d.res; }
    | r=roleResource { $res = $r.res; }
    | f=functionResource { $res = $f.res; }
    | j=jmxResource { $res = $j.res; }
    ;

dataResource returns [DataResource res]
    : K_ALL K_KEYSPACES { $res = DataResource.root(); }
    | K_KEYSPACE ks = keyspaceName { $res = DataResource.keyspace($ks.id); }
    | ( K_COLUMNFAMILY )? cf = columnFamilyName { $res = DataResource.table($cf.name.getKeyspace(), $cf.name.getName()); }
    | K_ALL K_TABLES K_IN K_KEYSPACE ks = keyspaceName { $res = DataResource.allTables($ks.id); }
    ;

jmxResource returns [JMXResource res]
    : K_ALL K_MBEANS { $res = JMXResource.root(); }
    // when a bean name (or pattern) is supplied, validate that it's a legal ObjectName
    // also, just to be picky, if the "MBEANS" form is used, only allow a pattern style names
    | K_MBEAN mbean { $res = JMXResource.mbean(canonicalizeObjectName($mbean.text, false)); }
    | K_MBEANS mbean { $res = JMXResource.mbean(canonicalizeObjectName($mbean.text, true)); }
    ;

roleResource returns [RoleResource res]
    : K_ALL K_ROLES { $res = RoleResource.root(); }
    | K_ROLE role = userOrRoleName { $res = RoleResource.role($role.name.getName()); }
    ;

functionResource returns [FunctionResource res]
    @init {
        List<CQL3Type.Raw> argsTypes = new ArrayList<>();
    }
    : K_ALL K_FUNCTIONS { $res = FunctionResource.root(); }
    | K_ALL K_FUNCTIONS K_IN K_KEYSPACE ks = keyspaceName { $res = FunctionResource.keyspace($ks.id); }
    // Arg types are mandatory for DCL statements on Functions
    | K_FUNCTION fn=functionName
      (
        '('
          (
            v=comparatorType { argsTypes.add(v); }
            ( ',' v=comparatorType { argsTypes.add(v); } )*
          )?
        ')'
      )
      { $res = FunctionResource.functionFromCql($fn.s.keyspace, $fn.s.name, argsTypes); }
    ;

/**
 * CREATE USER [IF NOT EXISTS] <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
createUserStatement returns [CreateRoleStatement stmt]
    @init {
        RoleOptions opts = new RoleOptions();
        opts.setOption(IRoleManager.Option.LOGIN, true);
        boolean superuser = false;
        boolean ifNotExists = false;
        RoleName name = new RoleName();
    }
    : K_CREATE K_USER (K_IF K_NOT K_EXISTS { ifNotExists = true; })? u=username { name.setName($u.text, true); }
      ( K_WITH userPassword[opts] )?
      ( K_SUPERUSER { superuser = true; } | K_NOSUPERUSER { superuser = false; } )?
      { opts.setOption(IRoleManager.Option.SUPERUSER, superuser);
        if (opts.getPassword().isPresent() && opts.getHashedPassword().isPresent())
        {
           throw new SyntaxException("Options 'password' and 'hashed password' are mutually exclusive");
        }
        $stmt = new CreateRoleStatement(name, opts, DCPermissions.all(), CIDRPermissions.all(), ifNotExists); }
    ;

/**
 * ALTER USER [IF EXISTS] <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
alterUserStatement returns [AlterRoleStatement stmt]
    @init {
        RoleOptions opts = new RoleOptions();
        RoleName name = new RoleName();
        boolean ifExists = false;
    }
    : K_ALTER K_USER (K_IF K_EXISTS { ifExists = true; })? u=username { name.setName($u.text, true); }
      ( K_WITH userPassword[opts] )?
      ( K_SUPERUSER { opts.setOption(IRoleManager.Option.SUPERUSER, true); }
        | K_NOSUPERUSER { opts.setOption(IRoleManager.Option.SUPERUSER, false); } ) ?
      {
         if (opts.getPassword().isPresent() && opts.getHashedPassword().isPresent())
         {
            throw new SyntaxException("Options 'password' and 'hashed password' are mutually exclusive");
         }
         $stmt = new AlterRoleStatement(name, opts, null, null, ifExists);
      }
    ;

/**
 * DROP USER [IF EXISTS] <username>
 */
dropUserStatement returns [DropRoleStatement stmt]
    @init {
        boolean ifExists = false;
        RoleName name = new RoleName();
    }
    : K_DROP K_USER (K_IF K_EXISTS { ifExists = true; })? u=username { name.setName($u.text, true); $stmt = new DropRoleStatement(name, ifExists); }
    ;
/**
 * ADD IDENTITY [IF NOT EXISTS] <identity> TO ROLE <role>
 */
addIdentityStatement returns [AddIdentityStatement stmt]
    @init {
        String identity = null;
        String role = null;
        boolean ifNotExists = false;
    }
    : K_ADD K_IDENTITY (K_IF K_NOT K_EXISTS { ifNotExists = true; })? u=identity { identity= $u.text; } K_TO K_ROLE r=identity { role=$r.text; $stmt = new AddIdentityStatement(identity, role, ifNotExists); }
    ;

/**
 * DROP IDENTITY [IF EXISTS] <identity>
 */
 dropIdentityStatement returns [DropIdentityStatement stmt]
      @init {
          boolean ifExists = false;
          String identity = null;
      }
      : K_DROP K_IDENTITY (K_IF K_EXISTS { ifExists = true; })? u=identity { identity= $u.text; $stmt = new DropIdentityStatement(identity, ifExists);}
      ;

/**
 * LIST USERS
 */
listUsersStatement returns [ListRolesStatement stmt]
    : K_LIST K_USERS { $stmt = new ListUsersStatement(); }
    ;

/**
 * CREATE ROLE [IF NOT EXISTS] <rolename> [ [WITH] option [ [AND] option ]* ]
 *
 * where option can be:
 *  PASSWORD = '<password>'
 *  SUPERUSER = (true|false)
 *  LOGIN = (true|false)
 *  OPTIONS = { 'k1':'v1', 'k2':'v2'}
 *  ACCESS TO ALL DATACENTERS
 *  ACCESS TO DATACENTERS { dcPermission (, dcPermission)* }
 *  ACCESS FROM ALL CIDRS
 *  ACCESS FROM CIDRS { cidrPermission (, cidrPermission)* }
 */
createRoleStatement returns [CreateRoleStatement stmt]
    @init {
        RoleOptions opts = new RoleOptions();
        DCPermissions.Builder dcperms = DCPermissions.builder();
        CIDRPermissions.Builder cidrperms = CIDRPermissions.builder();
        boolean ifNotExists = false;
    }
    : K_CREATE K_ROLE (K_IF K_NOT K_EXISTS { ifNotExists = true; })? name=userOrRoleName
      ( K_WITH roleOptions[opts, dcperms, cidrperms] )?
      {
        // set defaults if they weren't explictly supplied
        if (!opts.getLogin().isPresent())
        {
            opts.setOption(IRoleManager.Option.LOGIN, false);
        }
        if (!opts.getSuperuser().isPresent())
        {
            opts.setOption(IRoleManager.Option.SUPERUSER, false);
        }
        if (opts.getPassword().isPresent() && opts.getHashedPassword().isPresent())
        {
            throw new SyntaxException("Options 'password' and 'hashed password' are mutually exclusive");
        }
        $stmt = new CreateRoleStatement(name, opts, dcperms.build(), cidrperms.build(), ifNotExists);
      }
    ;

/**
 * ALTER ROLE [IF EXISTS] <rolename> [ [WITH] option [ [AND] option ]* ]
 *
 * where option can be:
 *  PASSWORD = '<password>'
 *  SUPERUSER = (true|false)
 *  LOGIN = (true|false)
 *  OPTIONS = { 'k1':'v1', 'k2':'v2'}
 *  ACCESS TO ALL DATACENTERS
 *  ACCESS TO DATACENTERS { dcPermission (, dcPermission)* }
 *  ACCESS FROM ALL CIDRS
 *  ACCESS FROM CIDRS { cidrPermission (, cidrPermission)* }
 */
alterRoleStatement returns [AlterRoleStatement stmt]
    @init {
        RoleOptions opts = new RoleOptions();
        DCPermissions.Builder dcperms = DCPermissions.builder();
        CIDRPermissions.Builder cidrperms = CIDRPermissions.builder();
        boolean ifExists = false;
    }
    : K_ALTER K_ROLE (K_IF K_EXISTS { ifExists = true; })? name=userOrRoleName
      ( K_WITH roleOptions[opts, dcperms, cidrperms] )?
      {
         if (opts.getPassword().isPresent() && opts.getHashedPassword().isPresent())
         {
            throw new SyntaxException("Options 'password' and 'hashed password' are mutually exclusive");
         }
         $stmt = new AlterRoleStatement(name, opts, dcperms.isModified() ? dcperms.build() : null, cidrperms.isModified() ? cidrperms.build() : null, ifExists);
      }
    ;

/**
 * DROP ROLE [IF EXISTS] <rolename>
 */
dropRoleStatement returns [DropRoleStatement stmt]
    @init {
        boolean ifExists = false;
    }
    : K_DROP K_ROLE (K_IF K_EXISTS { ifExists = true; })? name=userOrRoleName
      { $stmt = new DropRoleStatement(name, ifExists); }
    ;

/**
 * LIST ROLES [OF <rolename>] [NORECURSIVE]
 */
listRolesStatement returns [ListRolesStatement stmt]
    @init {
        boolean recursive = true;
        RoleName grantee = new RoleName();
    }
    : K_LIST K_ROLES
      ( K_OF roleName[grantee])?
      ( K_NORECURSIVE { recursive = false; } )?
      { $stmt = new ListRolesStatement(grantee, recursive); }
    ;

roleOptions[RoleOptions opts, DCPermissions.Builder dcperms, CIDRPermissions.Builder cidrperms]
    : roleOption[opts, dcperms, cidrperms] (K_AND roleOption[opts, dcperms, cidrperms])*
    ;

roleOption[RoleOptions opts, DCPermissions.Builder dcperms, CIDRPermissions.Builder cidrperms]
    :  K_PASSWORD '=' v=STRING_LITERAL { opts.setOption(IRoleManager.Option.PASSWORD, $v.text); }
    |  K_HASHED K_PASSWORD '=' v=STRING_LITERAL { opts.setOption(IRoleManager.Option.HASHED_PASSWORD, $v.text); }
    |  K_OPTIONS '=' m=fullMapLiteral { opts.setOption(IRoleManager.Option.OPTIONS, convertPropertyMap(m)); }
    |  K_SUPERUSER '=' b=BOOLEAN { opts.setOption(IRoleManager.Option.SUPERUSER, Boolean.valueOf($b.text)); }
    |  K_LOGIN '=' b=BOOLEAN { opts.setOption(IRoleManager.Option.LOGIN, Boolean.valueOf($b.text)); }
    |  K_ACCESS K_TO K_ALL K_DATACENTERS { dcperms.all(); }
    |  K_ACCESS K_TO K_DATACENTERS '{' dcPermission[dcperms] (',' dcPermission[dcperms])* '}'
    |  K_ACCESS K_FROM K_ALL K_CIDRS { cidrperms.all(); }
    |  K_ACCESS K_FROM K_CIDRS '{' cidrPermission[cidrperms] (',' cidrPermission[cidrperms])* '}'
    ;

dcPermission[DCPermissions.Builder builder]
    : dc=STRING_LITERAL { builder.add($dc.text); }
    ;

cidrPermission[CIDRPermissions.Builder builder]
    : cidr=STRING_LITERAL { builder.add($cidr.text); }
    ;

// for backwards compatibility in CREATE/ALTER USER, this has no '='
userPassword[RoleOptions opts]
    :  K_PASSWORD v=STRING_LITERAL { opts.setOption(IRoleManager.Option.PASSWORD, $v.text); }
    |  K_HASHED K_PASSWORD v=STRING_LITERAL { opts.setOption(IRoleManager.Option.HASHED_PASSWORD, $v.text); }
    ;

/**
 * DESCRIBE statement(s)
 *
 * Must be in sync with the javadoc for org.apache.cassandra.cql3.statements.DescribeStatement and the
 * cqlsh syntax definition in for cqlsh_describe_cmd_syntax_rules pylib/cqlshlib/cqlshhandling.py.
 */
describeStatement returns [DescribeStatement stmt]
    @init {
        boolean fullSchema = false;
        boolean pending = false;
        boolean config = false;
        boolean only = false;
        QualifiedName gen = new QualifiedName();
    }
    : ( K_DESCRIBE | K_DESC )
    ( (K_CLUSTER)=> K_CLUSTER                     { $stmt = DescribeStatement.cluster(); }
    | (K_FULL { fullSchema=true; })? K_SCHEMA     { $stmt = DescribeStatement.schema(fullSchema); }
    | (K_KEYSPACES)=> K_KEYSPACES                 { $stmt = DescribeStatement.keyspaces(); }
    | (K_ONLY { only=true; })? K_KEYSPACE ( ks=keyspaceName )?
                                                  { $stmt = DescribeStatement.keyspace(ks, only); }
    | (K_TABLES) => K_TABLES                      { $stmt = DescribeStatement.tables(); }
    | K_COLUMNFAMILY cf=columnFamilyName          { $stmt = DescribeStatement.table(cf.getKeyspace(), cf.getName()); }
    | K_INDEX idx=columnFamilyName                { $stmt = DescribeStatement.index(idx.getKeyspace(), idx.getName()); }
    | K_MATERIALIZED K_VIEW view=columnFamilyName { $stmt = DescribeStatement.view(view.getKeyspace(), view.getName()); }
    | (K_TYPES) => K_TYPES                        { $stmt = DescribeStatement.types(); }
    | K_TYPE tn=userTypeName                      { $stmt = DescribeStatement.type(tn.getKeyspace(), tn.getStringTypeName()); }
    | (K_FUNCTIONS) => K_FUNCTIONS                { $stmt = DescribeStatement.functions(); }
    | K_FUNCTION fn=functionName                  { $stmt = DescribeStatement.function(fn.keyspace, fn.name); }
    | (K_AGGREGATES) => K_AGGREGATES              { $stmt = DescribeStatement.aggregates(); }
    | K_AGGREGATE ag=functionName                 { $stmt = DescribeStatement.aggregate(ag.keyspace, ag.name); }
    | ( ( ksT=IDENT                       { gen.setKeyspace($ksT.text, false);}
          | ksT=QUOTED_NAME                 { gen.setKeyspace($ksT.text, true);}
          | ksK=unreserved_keyword          { gen.setKeyspace(ksK, false);} ) '.' )?
        ( tT=IDENT                          { gen.setName($tT.text, false);}
        | tT=QUOTED_NAME                    { gen.setName($tT.text, true);}
        | tK=unreserved_keyword             { gen.setName(tK, false);} )
                                                    { $stmt = DescribeStatement.generic(gen.getKeyspace(), gen.getName()); }
    )
    ( K_WITH K_INTERNALS { $stmt.withInternalDetails(); } )?
    ;

/** DEFINITIONS **/

// Like ident, but for case where we take a column name that can be the legacy super column empty name. Importantly,
// this should not be used in DDL statements, as we don't want to let users create such column.
cident returns [ColumnIdentifier id]
    : EMPTY_QUOTED_NAME    { $id = ColumnIdentifier.getInterned("", true); }
    | t=ident              { $id = t; }
    ;

ident returns [ColumnIdentifier id]
    : t=IDENT              { $id = ColumnIdentifier.getInterned($t.text, false); }
    | t=QUOTED_NAME        { $id = ColumnIdentifier.getInterned($t.text, true); }
    | k=unreserved_keyword { $id = ColumnIdentifier.getInterned(k, false); }
    ;

fident returns [FieldIdentifier id]
    : t=IDENT              { $id = FieldIdentifier.forUnquoted($t.text); }
    | t=QUOTED_NAME        { $id = FieldIdentifier.forQuoted($t.text); }
    | k=unreserved_keyword { $id = FieldIdentifier.forUnquoted(k); }
    ;

// Identifiers that do not refer to columns
noncol_ident returns [ColumnIdentifier id]
    : t=IDENT              { $id = new ColumnIdentifier($t.text, false); }
    | t=QUOTED_NAME        { $id = new ColumnIdentifier($t.text, true); }
    | k=unreserved_keyword { $id = new ColumnIdentifier(k, false); }
    ;

// Keyspace & Column family names
keyspaceName returns [String id]
    @init { QualifiedName name = new QualifiedName(); }
    : ksName[name] { $id = name.getKeyspace(); }
    ;

indexName returns [QualifiedName name]
    @init { $name = new QualifiedName(); }
    : (ksName[name] '.')? idxName[name]
    ;

columnFamilyName returns [QualifiedName name]
    @init { $name = new QualifiedName(); }
    : (ksName[name] '.')? cfName[name]
    ;

userTypeName returns [UTName name]
    : (ks=noncol_ident '.')? ut=non_type_ident { $name = new UTName(ks, ut); }
    ;

userOrRoleName returns [RoleName name]
    @init { RoleName role = new RoleName(); }
    : roleName[role] {$name = role;}
    ;

ksName[QualifiedName name]
    : t=IDENT              { $name.setKeyspace($t.text, false);}
    | t=QUOTED_NAME        { $name.setKeyspace($t.text, true);}
    | k=unreserved_keyword { $name.setKeyspace(k, false);}
    | QMARK {addRecognitionError("Bind variables cannot be used for keyspace names");}
    ;

cfName[QualifiedName name]
    : t=IDENT              { $name.setName($t.text, false); }
    | t=QUOTED_NAME        { $name.setName($t.text, true); }
    | k=unreserved_keyword { $name.setName(k, false); }
    | QMARK {addRecognitionError("Bind variables cannot be used for table names");}
    ;

idxName[QualifiedName name]
    : t=IDENT              { $name.setName($t.text, false); }
    | t=QUOTED_NAME        { $name.setName($t.text, true);}
    | k=unreserved_keyword { $name.setName(k, false); }
    | QMARK {addRecognitionError("Bind variables cannot be used for index names");}
    ;

roleName[RoleName name]
    : t=IDENT              { $name.setName($t.text, false); }
    | s=STRING_LITERAL     { $name.setName($s.text, true); }
    | t=QUOTED_NAME        { $name.setName($t.text, true); }
    | k=unreserved_keyword { $name.setName(k, false); }
    | QMARK {addRecognitionError("Bind variables cannot be used for role names");}
    ;

constant returns [Constants.Literal constant]
    : t=STRING_LITERAL { $constant = Constants.Literal.string($t.text); }
    | t=INTEGER        { $constant = Constants.Literal.integer($t.text); }
    | t=FLOAT          { $constant = Constants.Literal.floatingPoint($t.text); }
    | t=BOOLEAN        { $constant = Constants.Literal.bool($t.text); }
    | t=DURATION       { $constant = Constants.Literal.duration($t.text);}
    | t=UUID           { $constant = Constants.Literal.uuid($t.text); }
    | t=HEXNUMBER      { $constant = Constants.Literal.hex($t.text); }
    | ((K_POSITIVE_NAN | K_NEGATIVE_NAN) { $constant = Constants.Literal.floatingPoint("NaN"); }
        | K_POSITIVE_INFINITY  { $constant = Constants.Literal.floatingPoint("Infinity"); }
        | K_NEGATIVE_INFINITY { $constant = Constants.Literal.floatingPoint("-Infinity"); })
    ;

fullMapLiteral returns [Maps.Literal map]
    @init { List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>();}
    @after{ $map = new Maps.Literal(m); }
    : '{' ( k1=term ':' v1=term { m.add(Pair.create(k1, v1)); } ( ',' kn=term ':' vn=term { m.add(Pair.create(kn, vn)); } )* )?
      '}'
    ;

setOrMapLiteral[Term.Raw t] returns [Term.Raw value]
    : m=mapLiteral[t] { $value=m; }
    | s=setLiteral[t] { $value=s; }
    ;

setLiteral[Term.Raw t] returns [Term.Raw value]
    @init { List<Term.Raw> s = new ArrayList<Term.Raw>(); s.add(t); }
    @after { $value = new Sets.Literal(s); }
    : ( ',' tn=term { s.add(tn); } )*
    ;

mapLiteral[Term.Raw k] returns [Term.Raw value]
    @init { List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>(); }
    @after { $value = new Maps.Literal(m); }
    : ':' v=term {  m.add(Pair.create(k, v)); } ( ',' kn=term ':' vn=term { m.add(Pair.create(kn, vn)); } )*
    ;

collectionLiteral returns [Term.Raw value]
    : l=listLiteral { $value = l; }
    | '{' t=term v=setOrMapLiteral[t] { $value = v; } '}'
    // Note that we have an ambiguity between maps and set for "{}". So we force it to a set literal,
    // and deal with it later based on the type of the column (SetLiteral.java).
    | '{' '}' { $value = new Sets.Literal(Collections.<Term.Raw>emptyList()); }
    ;

listLiteral returns [Term.Raw value]
    @init {List<Term.Raw> l = new ArrayList<Term.Raw>();}
    @after {$value = new ArrayLiteral(l);}
    : '[' ( t1=term { l.add(t1); } ( ',' tn=term { l.add(tn); } )* )? ']' { $value = new ArrayLiteral(l); }
    ;

usertypeLiteral returns [UserTypes.Literal ut]
    @init{ Map<FieldIdentifier, Term.Raw> m = new HashMap<>(); }
    @after{ $ut = new UserTypes.Literal(m); }
    // We don't allow empty literals because that conflicts with sets/maps and is currently useless since we don't allow empty user types
    : '{' k1=fident ':' v1=term { m.put(k1, v1); } ( ',' kn=fident ':' vn=term { m.put(kn, vn); } )* '}'
    ;

tupleLiteral returns [Tuples.Literal tt]
    @init{ List<Term.Raw> l = new ArrayList<Term.Raw>(); }
    @after{ $tt = new Tuples.Literal(l); }
    : '(' t1=term { l.add(t1); } ( ',' tn=term { l.add(tn); } )* ')'
    ;

value returns [Term.Raw value]
    : c=constant           { $value = c; }
    | l=collectionLiteral  { $value = l; }
    | u=usertypeLiteral    { $value = u; }
    | t=tupleLiteral       { $value = t; }
    | K_NULL               { $value = Constants.NULL_LITERAL; }
    | ':' id=noncol_ident  { $value = newBindVariables(id); }
    | QMARK                { $value = newBindVariables(null); }
    ;

intValue returns [Term.Raw value]
    : t=INTEGER     { $value = Constants.Literal.integer($t.text); }
    | ':' id=noncol_ident  { $value = newBindVariables(id); }
    | QMARK         { $value = newBindVariables(null); }
    ;

functionName returns [FunctionName s]
     // antlr might try to recover and give a null for f. It will still error out in the end, but FunctionName
     // wouldn't be happy with that so we should bypass this for now or we'll have a weird user-facing error
    : (ks=keyspaceName '.')? f=allowedFunctionName   { $s = f == null ? null : new FunctionName(ks, f); }
    ;

allowedFunctionName returns [String s]
    : f=IDENT                       { $s = $f.text.toLowerCase(); }
    | f=QUOTED_NAME                 { $s = $f.text; }
    | u=unreserved_function_keyword { $s = u; }
    | K_TOKEN                       { $s = "token"; }
    | K_COUNT                       { $s = "count"; }
    ;

function returns [Term.Raw t]
    : f=functionName '(' ')'                   { $t = new FunctionCall.Raw(f, Collections.<Term.Raw>emptyList()); }
    | f=functionName '(' args=functionArgs ')' { $t = new FunctionCall.Raw(f, args); }
    ;

functionArgs returns [List<Term.Raw> args]
    @init{ $args = new ArrayList<Term.Raw>(); }
    : t1=term {args.add(t1); } ( ',' tn=term { args.add(tn); } )*
    ;

term returns [Term.Raw term]
    : t=termAddition                          { $term = t; }
    ;

termAddition returns [Term.Raw term]
    :   l=termMultiplication   {$term = l;}
        ( '+' r=termMultiplication {$term = FunctionCall.Raw.newOperation('+', $term, r);}
        | '-' r=termMultiplication {$term = FunctionCall.Raw.newOperation('-', $term, r);}
        )*
    ;

termMultiplication returns [Term.Raw term]
    :   l=termGroup   {$term = l;}
        ( '\*' r=termGroup {$term = FunctionCall.Raw.newOperation('*', $term, r);}
        | '/' r=termGroup {$term = FunctionCall.Raw.newOperation('/', $term, r);}
        | '%' r=termGroup {$term = FunctionCall.Raw.newOperation('\%', $term, r);}
        )*
    ;

termGroup returns [Term.Raw term]
    : t=simpleTerm              { $term = t; }
    | '-'  t=simpleTerm         { $term = FunctionCall.Raw.newNegation(t); }
    ;

simpleTerm returns [Term.Raw term]
    : v=value                                        { $term = v; }
    | f=function                                     { $term = f; }
    | '(' c=comparatorType ')' t=simpleTerm          { $term = new TypeCast(c, t); }
    | K_CAST '(' t=simpleTerm K_AS n=native_type ')' { $term = FunctionCall.Raw.newCast(t, n); }
    ;

columnOperation[List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations]
    : key=cident columnOperationDifferentiator[operations, key]
    ;

columnOperationDifferentiator[List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations, ColumnIdentifier key]
    : '=' normalColumnOperation[operations, key]
    | shorthandColumnOperation[operations, key]
    | '[' k=term ']' collectionColumnOperation[operations, key, k]
    | '.' field=fident udtColumnOperation[operations, key, field]
    ;

normalColumnOperation[List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations, ColumnIdentifier key]
    : t=term ('+' c=cident )?
      {
          if (c == null)
          {
              addRawUpdate(operations, key, new Operation.SetValue(t));
          }
          else
          {
              if (!key.equals(c))
                  addRecognitionError("Only expressions of the form X = <value> + X are supported.");
              addRawUpdate(operations, key, new Operation.Prepend(t));
          }
      }
    | c=cident sig=('+' | '-') t=term
      {
          if (!key.equals(c))
              addRecognitionError("Only expressions of the form X = X " + $sig.text + "<value> are supported.");
          addRawUpdate(operations, key, $sig.text.equals("+") ? new Operation.Addition(t) : new Operation.Substraction(t));
      }
    | c=cident i=INTEGER
      {
          // Note that this production *is* necessary because X = X - 3 will in fact be lexed as [ X, '=', X, INTEGER].
          if (!key.equals(c))
              // We don't yet allow a '+' in front of an integer, but we could in the future really, so let's be future-proof in our error message
              addRecognitionError("Only expressions of the form X = X " + ($i.text.charAt(0) == '-' ? '-' : '+') + " <value> are supported.");
          addRawUpdate(operations, key, new Operation.Addition(Constants.Literal.integer($i.text)));
      }
    ;

shorthandColumnOperation[List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations, ColumnIdentifier key]
    : sig=('+=' | '-=') t=term
      {
          addRawUpdate(operations, key, $sig.text.equals("+=") ? new Operation.Addition(t) : new Operation.Substraction(t));
      }
    ;

collectionColumnOperation[List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations, ColumnIdentifier key, Term.Raw k]
    : '=' t=term
      {
          addRawUpdate(operations, key, new Operation.SetElement(k, t));
      }
    ;

udtColumnOperation[List<Pair<ColumnIdentifier, Operation.RawUpdate>> operations, ColumnIdentifier key, FieldIdentifier field]
    : '=' t=term
      {
          addRawUpdate(operations, key, new Operation.SetField(field, t));
      }
    ;

columnCondition[List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions]
    // Note: we'll reject duplicates later
    : key=cident
        ( op=relationType t=term { conditions.add(Pair.create(key, ColumnCondition.Raw.simpleCondition(t, op))); }
        | op=containsOperator t=term { conditions.add(Pair.create(key, ColumnCondition.Raw.simpleCondition(t, op))); }
        | K_IN
            ( values=singleColumnInValues { conditions.add(Pair.create(key, ColumnCondition.Raw.simpleInCondition(values))); }
            | marker=inMarker { conditions.add(Pair.create(key, ColumnCondition.Raw.simpleInCondition(marker))); }
            )
        | '[' element=term ']'
            ( op=relationType t=term { conditions.add(Pair.create(key, ColumnCondition.Raw.collectionCondition(t, element, op))); }
            | K_IN
                ( values=singleColumnInValues { conditions.add(Pair.create(key, ColumnCondition.Raw.collectionInCondition(element, values))); }
                | marker=inMarker { conditions.add(Pair.create(key, ColumnCondition.Raw.collectionInCondition(element, marker))); }
                )
            )
        | '.' field=fident
            ( op=relationType t=term { conditions.add(Pair.create(key, ColumnCondition.Raw.udtFieldCondition(t, field, op))); }
            | K_IN
                ( values=singleColumnInValues { conditions.add(Pair.create(key, ColumnCondition.Raw.udtFieldInCondition(field, values))); }
                | marker=inMarker { conditions.add(Pair.create(key, ColumnCondition.Raw.udtFieldInCondition(field, marker))); }
                )
            )
        )
    ;

properties[PropertyDefinitions props]
    : property[props] (K_AND property[props])*
    ;

property[PropertyDefinitions props]
    : k=noncol_ident '=' simple=propertyValue { try { $props.addProperty(k.toString(), simple); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } }
    | k=noncol_ident '=' map=fullMapLiteral { try { $props.addProperty(k.toString(), convertPropertyMap(map)); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } }
    ;

propertyValue returns [String str]
    : c=constant           { $str = c.getRawText(); }
    | u=unreserved_keyword { $str = u; }
    ;

relationType returns [Operator op]
    : '='  { $op = Operator.EQ; }
    | '<'  { $op = Operator.LT; }
    | '<=' { $op = Operator.LTE; }
    | '>'  { $op = Operator.GT; }
    | '>=' { $op = Operator.GTE; }
    | '!=' { $op = Operator.NEQ; }
    ;

relation[WhereClause.Builder clauses]
    : name=cident type=relationType t=term { $clauses.add(new SingleColumnRelation(name, type, t)); }
    | name=cident K_LIKE t=term { $clauses.add(new SingleColumnRelation(name, Operator.LIKE, t)); }
    | name=cident K_IS K_NOT K_NULL { $clauses.add(new SingleColumnRelation(name, Operator.IS_NOT, Constants.NULL_LITERAL)); }
    | K_TOKEN l=tupleOfIdentifiers type=relationType t=term
        { $clauses.add(new TokenRelation(l, type, t)); }
    | name=cident K_IN marker=inMarker
        { $clauses.add(new SingleColumnRelation(name, Operator.IN, marker)); }
    | name=cident K_IN inValues=singleColumnInValues
        { $clauses.add(SingleColumnRelation.createInRelation($name.id, inValues)); }
    | name=cident rt=containsOperator t=term { $clauses.add(new SingleColumnRelation(name, rt, t)); }
    | name=cident '[' key=term ']' type=relationType t=term { $clauses.add(new SingleColumnRelation(name, key, type, t)); }
    | ids=tupleOfIdentifiers
      ( K_IN
          ( '(' ')'
              { $clauses.add(MultiColumnRelation.createInRelation(ids, new ArrayList<Tuples.Literal>())); }
          | tupleInMarker=inMarkerForTuple /* (a, b, c) IN ? */
              { $clauses.add(MultiColumnRelation.createSingleMarkerInRelation(ids, tupleInMarker)); }
          | literals=tupleOfTupleLiterals /* (a, b, c) IN ((1, 2, 3), (4, 5, 6), ...) */
              {
                  $clauses.add(MultiColumnRelation.createInRelation(ids, literals));
              }
          | markers=tupleOfMarkersForTuples /* (a, b, c) IN (?, ?, ...) */
              { $clauses.add(MultiColumnRelation.createInRelation(ids, markers)); }
          )
      | type=relationType literal=tupleLiteral /* (a, b, c) > (1, 2, 3) or (a, b, c) > (?, ?, ?) */
          {
              $clauses.add(MultiColumnRelation.createNonInRelation(ids, type, literal));
          }
      | type=relationType tupleMarker=markerForTuple /* (a, b, c) >= ? */
          { $clauses.add(MultiColumnRelation.createNonInRelation(ids, type, tupleMarker)); }
      )
    | '(' relation[$clauses] ')'
    ;

containsOperator returns [Operator o]
    : K_CONTAINS { o = Operator.CONTAINS; } (K_KEY { o = Operator.CONTAINS_KEY; })?
    ;

inMarker returns [AbstractMarker.INRaw marker]
    : QMARK { $marker = newINBindVariables(null); }
    | ':' name=noncol_ident { $marker = newINBindVariables(name); }
    ;

tupleOfIdentifiers returns [List<ColumnIdentifier> ids]
    @init { $ids = new ArrayList<ColumnIdentifier>(); }
    : '(' n1=cident { $ids.add(n1); } (',' ni=cident { $ids.add(ni); })* ')'
    ;

singleColumnInValues returns [List<Term.Raw> terms]
    @init { $terms = new ArrayList<Term.Raw>(); }
    : '(' ( t1 = term { $terms.add(t1); } (',' ti=term { $terms.add(ti); })* )? ')'
    ;

tupleOfTupleLiterals returns [List<Tuples.Literal> literals]
    @init { $literals = new ArrayList<>(); }
    : '(' t1=tupleLiteral { $literals.add(t1); } (',' ti=tupleLiteral { $literals.add(ti); })* ')'
    ;

markerForTuple returns [Tuples.Raw marker]
    : QMARK { $marker = newTupleBindVariables(null); }
    | ':' name=noncol_ident { $marker = newTupleBindVariables(name); }
    ;

tupleOfMarkersForTuples returns [List<Tuples.Raw> markers]
    @init { $markers = new ArrayList<Tuples.Raw>(); }
    : '(' m1=markerForTuple { $markers.add(m1); } (',' mi=markerForTuple { $markers.add(mi); })* ')'
    ;

inMarkerForTuple returns [Tuples.INRaw marker]
    : QMARK { $marker = newTupleINBindVariables(null); }
    | ':' name=noncol_ident { $marker = newTupleINBindVariables(name); }
    ;

comparatorType returns [CQL3Type.Raw t]
    : n=native_type     { $t = CQL3Type.Raw.from(n); }
    | c=collection_type { $t = c; }
    | tt=tuple_type     { $t = tt; }
    | vc=vector_type    { $t = vc; }
    | id=userTypeName   { $t = CQL3Type.Raw.userType(id); }
    | K_FROZEN '<' f=comparatorType '>'
      {
        try {
            $t = f.freeze();
        } catch (InvalidRequestException e) {
            addRecognitionError(e.getMessage());
        }
      }
    | s=STRING_LITERAL
      {
        try {
            $t = CQL3Type.Raw.from(new CQL3Type.Custom($s.text));
        } catch (SyntaxException e) {
            addRecognitionError("Cannot parse type " + $s.text + ": " + e.getMessage());
        } catch (ConfigurationException e) {
            addRecognitionError("Error setting type " + $s.text + ": " + e.getMessage());
        }
      }
    ;

native_type returns [CQL3Type t]
    : K_ASCII     { $t = CQL3Type.Native.ASCII; }
    | K_BIGINT    { $t = CQL3Type.Native.BIGINT; }
    | K_BLOB      { $t = CQL3Type.Native.BLOB; }
    | K_BOOLEAN   { $t = CQL3Type.Native.BOOLEAN; }
    | K_COUNTER   { $t = CQL3Type.Native.COUNTER; }
    | K_DECIMAL   { $t = CQL3Type.Native.DECIMAL; }
    | K_DOUBLE    { $t = CQL3Type.Native.DOUBLE; }
    | K_DURATION  { $t = CQL3Type.Native.DURATION; }
    | K_FLOAT     { $t = CQL3Type.Native.FLOAT; }
    | K_INET      { $t = CQL3Type.Native.INET;}
    | K_INT       { $t = CQL3Type.Native.INT; }
    | K_SMALLINT  { $t = CQL3Type.Native.SMALLINT; }
    | K_TEXT      { $t = CQL3Type.Native.TEXT; }
    | K_TIMESTAMP { $t = CQL3Type.Native.TIMESTAMP; }
    | K_TINYINT   { $t = CQL3Type.Native.TINYINT; }
    | K_UUID      { $t = CQL3Type.Native.UUID; }
    | K_VARCHAR   { $t = CQL3Type.Native.VARCHAR; }
    | K_VARINT    { $t = CQL3Type.Native.VARINT; }
    | K_TIMEUUID  { $t = CQL3Type.Native.TIMEUUID; }
    | K_DATE      { $t = CQL3Type.Native.DATE; }
    | K_TIME      { $t = CQL3Type.Native.TIME; }
    ;

collection_type returns [CQL3Type.Raw pt]
    : K_MAP  '<' t1=comparatorType ',' t2=comparatorType '>'
        {
            // if we can't parse either t1 or t2, antlr will "recover" and we may have t1 or t2 null.
            if (t1 != null && t2 != null)
                $pt = CQL3Type.Raw.map(t1, t2);
        }
    | K_LIST '<' t=comparatorType '>'
        { if (t != null) $pt = CQL3Type.Raw.list(t); }
    | K_SET  '<' t=comparatorType '>'
        { if (t != null) $pt = CQL3Type.Raw.set(t); }
    ;

tuple_type returns [CQL3Type.Raw t]
    @init {List<CQL3Type.Raw> types = new ArrayList<>();}
    @after {$t = CQL3Type.Raw.tuple(types);}
    : K_TUPLE '<' t1=comparatorType { types.add(t1); } (',' tn=comparatorType { types.add(tn); })* '>'
    ;

vector_type returns [CQL3Type.Raw vt]
    : K_VECTOR '<' t1=comparatorType ','  d=INTEGER '>'
        { $vt = CQL3Type.Raw.vector(t1, Integer.parseInt($d.text)); }
    ;

username
    : IDENT
    | STRING_LITERAL
    | QUOTED_NAME { addRecognitionError("Quoted strings are are not supported for user names and USER is deprecated, please use ROLE");}
    ;

identity
    : IDENT
    | STRING_LITERAL
    | QUOTED_NAME { addRecognitionError("Quoted strings are are not supported for identity");}
    ;

mbean
    : STRING_LITERAL
    ;

// Basically the same as cident, but we need to exlude existing CQL3 types
// (which for some reason are not reserved otherwise)
non_type_ident returns [ColumnIdentifier id]
    : t=IDENT                    { if (reservedTypeNames.contains($t.text)) addRecognitionError("Invalid (reserved) user type name " + $t.text); $id = new ColumnIdentifier($t.text, false); }
    | t=QUOTED_NAME              { $id = new ColumnIdentifier($t.text, true); }
    | k=basic_unreserved_keyword { $id = new ColumnIdentifier(k, false); }
    | kk=K_KEY                   { $id = new ColumnIdentifier($kk.text, false); }
    ;

unreserved_keyword returns [String str]
    : u=unreserved_function_keyword     { $str = u; }
    | k=(K_TTL | K_COUNT | K_WRITETIME | K_MAXWRITETIME | K_KEY | K_CAST | K_JSON | K_DISTINCT) { $str = $k.text; }
    ;

unreserved_function_keyword returns [String str]
    : u=basic_unreserved_keyword { $str = u; }
    | t=native_type              { $str = t.toString(); }
    ;

basic_unreserved_keyword returns [String str]
    : k=( K_KEYS
        | K_AS
        | K_CLUSTER
        | K_CLUSTERING
        | K_COMPACT
        | K_STORAGE
        | K_TABLES
        | K_TYPE
        | K_TYPES
        | K_VALUES
        | K_MAP
        | K_LIST
        | K_FILTERING
        | K_PERMISSION
        | K_PERMISSIONS
        | K_KEYSPACES
        | K_ALL
        | K_USER
        | K_USERS
        | K_ROLE
        | K_ROLES
        | K_IDENTITY
        | K_SUPERUSER
        | K_NOSUPERUSER
        | K_LOGIN
        | K_NOLOGIN
        | K_OPTIONS
        | K_PASSWORD
        | K_HASHED
        | K_EXISTS
        | K_CUSTOM
        | K_TRIGGER
        | K_CONTAINS
        | K_INTERNALS
        | K_ONLY
        | K_STATIC
        | K_FROZEN
        | K_TUPLE
        | K_FUNCTION
        | K_FUNCTIONS
        | K_AGGREGATE
        | K_AGGREGATES
        | K_SFUNC
        | K_STYPE
        | K_FINALFUNC
        | K_INITCOND
        | K_RETURNS
        | K_LANGUAGE
        | K_CALLED
        | K_INPUT
        | K_LIKE
        | K_PER
        | K_PARTITION
        | K_GROUP
        | K_DATACENTERS
        | K_CIDRS
        | K_ACCESS
        | K_DEFAULT
        | K_MBEAN
        | K_MBEANS
        | K_REPLACE
        | K_UNSET
        | K_MASKED
        | K_UNMASK
        | K_SELECT_MASKED
        | K_VECTOR
        | K_ANN
        ) { $str = $k.text; }
    ;
