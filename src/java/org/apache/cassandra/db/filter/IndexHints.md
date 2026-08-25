<!---
Copyright DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Index Hints

Index hints are user-provided directives about what indexes should be used by a `SELECT` query.
They consist of a set of indexes that should be used (included) and a set of indexes that should not be used (excluded). 
The CQL syntax is:
```
SELECT ... FROM ... WHERE ...
  WITH included_indexes = { ... } 
    AND excluded_indexes = { ... };
```
So, for example, given the following schema:
```
CREATE TABLE users (
  username text PRIMARY KEY,
  birth_year int,
  country text,
  phone text
);

CREATE INDEX birth_year_idx ON users (birth_year);
CREATE INDEX country_idx ON users (country);
CREATE INDEX phone_idx ON users (phone);
```
The following query will use the index on `birth_year` and will not use the indexes on `country` and `phone`:
```
SELECT * FROM users
  WHERE birth_year = 1981 AND country = 'FR' ALLOW FILTERING
  WITH included_indexes = {birth_year_idx}
    AND excluded_indexes = {country_idx, phone_idx};
```
Please note that the query requires `ALLOW FILTERING` because there is a restriction on the `country` column, 
and we are explicitly excluding the index on that column.
Note also that excluding the index on `phone` is a no-op because there isn’t any restriction on it.

It’s guaranteed that the queries will utilize all the included indexes, or fail if it’s not possible to do so. 
It will never happen that a query succeeds without using all the included indexes. 
Queries might fail because the query doesn't have a restriction for those indexes, 
because there is a restriction that could use the index but is not compatible with other restrictions, 
or because the underlying index implementation isn't able to use the index for some reason.

Excluded indexes will never make the query fail, unless they reference a non-existent index. 
That's because it’s always possible to exclude an index regardless of the query expressions 
and index implementation capabilities. 
However, excluding indexes might make it necessary to add `ALLOW FILTERING` to the query.

Indexes that are applicable to the query and that are not mentioned in these two sets of included and excluded indexes
might or might not be used, depending on the index query planner.

## Excluding all indexes with a wildcard

Rather than enumerating every index by name, `excluded_indexes` accepts the `*` wildcard to exclude all the
indexes currently applicable to the queried table:
```
SELECT * FROM users
  WHERE birth_year = 1981 AND country = 'FR' ALLOW FILTERING
  WITH excluded_indexes = {*};
```
This is equivalent to explicitly listing every index on the table (`birth_year_idx`, `country_idx` and
`phone_idx` in the example schema above), and is resolved to that concrete list of indexes by the coordinator
before the query is sent to the replicas.

The wildcard is only supported in `excluded_indexes`. Using it in `included_indexes` (e.g.
`included_indexes = {*}`) will be rejected, since there is no defined semantics for "include all indexes".

The wildcard can be combined with explicit index names in the same `excluded_indexes` set (e.g.
`excluded_indexes = {*, some_idx}`), which is redundant but not an error. However, if `included_indexes` names
an index that also exists on the table, combining it with `excluded_indexes = {*}` will fail with the same
"indexes cannot be both included and excluded" error as if that index had been explicitly excluded, since the
wildcard is expanded before that check is performed.

## Disambiguating queries

Index hints can also be used to disambiguate queries where a restricted column has multiple indexes that return 
different results. For example, we can have analyzed and not-analyzed indexes in the same column. An equality query on
that column would throw an exception due to the ambiguity:
```
CREATE TABLE t(k int PRIMARY KEY, v text);
CREATE CUSTOM INDEX not_analyzed_idx ON t(v) USING 'StorageAttachedIndex';
CREATE CUSTOM INDEX analyzed_idx ON t(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' };
SELECT * FROM t WHERE v = '...'; # rejected query due to ambiguity
```
But the query will work if we add hints to include or exclude one of the indexes. 
The following will use non-analyzed index and restrict according the exact equality semantics:
```
SELECT * FROM t WHERE v = '...' WITH included_indexes = {not_analyzed_idx};
SELECT * FROM t WHERE v = '...' WITH excluded_indexes = {analyzed_idx};
```
The following will use analyzed index and restrict according the analyzer-based matching semantics
```
SELECT * FROM t WHERE v = '...' WITH included_indexes = {analyzed_idx};
SELECT * FROM t WHERE v = '...' WITH excluded_indexes = {not_analyzed_idx};
```
A similar disambiguation can be done for `CONTAINS` queries where the column has both analyzed and not-analyzed indexes:
```
CREATE TABLE t(k int PRIMARY KEY, v set<text>);
CREATE CUSTOM INDEX not_analyzed_idx ON t(v) USING 'StorageAttachedIndex';
CREATE CUSTOM INDEX analyzed_idx ON t(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' };
INSERT INTO t(k, v) VALUES ( 0, {'apple banana'});
INSERT INTO t(k, v) VALUES ( 1, {'apple'});
```
By default, `CONTAINS` queries will use the not-analyzed index:
```
SELECT * FROM t WHERE v CONTAINS 'apple';
```
This will use the not-analyzed index and return one row only.
But we can use hints to force the use of the analyzed index:
```
SELECT * FROM t WHERE v CONTAINS 'apple' WITH included_indexes = {analyzed_idx};
```
This will use the analyzed index and return two rows instead.

## Unshading queries

The presence of indexes can shade queries that used to have a different behaviour without indexes.
For example, an analyzed index will shade `ALLOW FILTERING`'s full-value equality:
```
CREATE TABLE t(k int PRIMARY KEY, v text);
SELECT * FROM t WHERE v = '...' ALLOW FILTERING; # exact equality match
CREATE CUSTOM INDEX idx ON t(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' };
SELECT * FROM t WHERE v = '...' ALLOW FILTERING; # uses the analyzed index, shading the previous query
```
But we can use hints to exclude that index and get access to the not-indexed behaviour:
```
SELECT * FROM t WHERE v = '...' ALLOW FILTERING WITH excluded_indexes = {idx}; # uses not-analyzed filtering
```

## Choosing between index implementations

Columns can have multiple indexes with different implementations.
For example, we can have a legacy index and a SAI index on the same column:
```
CREATE TABLE t(k int PRIMARY KEY, v text);
CREATE INDEX legacy_idx ON t(v);
CREATE CUSTOM INDEX sai_idx ON t(v) USING 'StorageAttachedIndex';
SELECT * FROM t WHERE v = '...'; # uses the SAI index
```
The index manager will always prefer the SAI index over the legacy index. 
However, we can use hints to prefer the legacy index:
```
SELECT * FROM t WHERE v = '...' WITH included_indexes = {legacy_idx}; # uses the legacy index
SELECT * FROM t WHERE v = '...' WITH excluded_indexes = {sai_idx}; # also uses the legacy index
```