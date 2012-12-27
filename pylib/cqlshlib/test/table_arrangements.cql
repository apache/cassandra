-- type A: single-column PK, compact storage
-- FAILS: CREATE TABLE type_a_1 (a int PRIMARY KEY) WITH COMPACT STORAGE;
-- Bad Request: No definition found that is not part of the PRIMARY KEY
CREATE TABLE type_a_2 (a int PRIMARY KEY, b int) WITH COMPACT STORAGE;
CREATE TABLE type_a_3 (a int PRIMARY KEY, b int, c int) WITH COMPACT STORAGE;

-- type B: single-column PK, dynamic storage
CREATE TABLE type_b_1 (a int PRIMARY KEY);
CREATE TABLE type_b_2 (a int PRIMARY KEY, b int);
CREATE TABLE type_b_3 (a int PRIMARY KEY, b int, c int);

-- type C: compound PK, plain partition key, compact storage
CREATE TABLE type_c_2_2 (a int, b int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE;
CREATE TABLE type_c_3_3 (a int, b int, c int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE;
CREATE TABLE type_c_3_2 (a int, b int, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE;
-- FAILS: CREATE TABLE type_c_4_2 (a int, b int, c int, d int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE;
-- Bad Request: COMPACT STORAGE WITH composite PRIMARY KEY allows no more than one column not part of the PRIMARY KEY (got: d, c)
CREATE TABLE type_c_4_3 (a int, b int, c int, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE;

-- type D: compound PK, plain partition key, dynamic storage
CREATE TABLE type_d_2_2 (a int, b int, PRIMARY KEY (a, b));
CREATE TABLE type_d_3_2 (a int, b int, c int, PRIMARY KEY (a, b));
CREATE TABLE type_d_3_3 (a int, b int, c int, PRIMARY KEY (a, b, c));
CREATE TABLE type_d_4_2 (a int, b int, c int, d int, PRIMARY KEY (a, b));

-- type E: compound PK, multipart partition key, all key components used in partitioning,
-- compact storage
-- FAILS: CREATE TABLE type_e_2_2 (a int, b int, PRIMARY KEY ((a, b))) WITH COMPACT STORAGE;
-- Bad Request: No definition found that is not part of the PRIMARY KEY
CREATE TABLE type_e_3_2 (a int, b int, c int, PRIMARY KEY ((a, b))) WITH COMPACT STORAGE;
CREATE TABLE type_e_4_2 (a int, b int, c int, d int, PRIMARY KEY ((a, b))) WITH COMPACT STORAGE;
CREATE TABLE type_e_4_3 (a int, b int, c int, d int, PRIMARY KEY ((a, b, c))) WITH COMPACT STORAGE;

-- type F: compound PK, multipart partition key, all key components used in partitioning,
-- dynamic storage
CREATE TABLE type_f_2_2 (a int, b int, PRIMARY KEY ((a, b)));
CREATE TABLE type_f_3_2 (a int, b int, c int, PRIMARY KEY ((a, b)));
CREATE TABLE type_f_4_2 (a int, b int, c int, d int, PRIMARY KEY ((a, b)));
CREATE TABLE type_f_4_3 (a int, b int, c int, d int, PRIMARY KEY ((a, b, c)));

-- type G: compound PK, multipart partition key, not all key components used in partitioning,
-- compact storage
CREATE TABLE type_g_3_3_2 (a int, b int, c int, PRIMARY KEY ((a, b), c)) WITH COMPACT STORAGE;
CREATE TABLE type_g_4_3_2 (a int, b int, c int, d int, PRIMARY KEY ((a, b), c)) WITH COMPACT STORAGE;
CREATE TABLE type_g_4_4_2 (a int, b int, c int, d int, PRIMARY KEY ((a, b), c, d)) WITH COMPACT STORAGE;
CREATE TABLE type_g_4_4_3 (a int, b int, c int, d int, PRIMARY KEY ((a, b, c), d)) WITH COMPACT STORAGE;
-- FAILS: CREATE TABLE type_g_5_3_2 (a int, b int, c int, d int, e int, PRIMARY KEY ((a, b), c)) WITH COMPACT STORAGE;
-- Bad Request: COMPACT STORAGE with composite PRIMARY KEY allows no more than one column not part of the PRIMARY KEY (got: d, e)
CREATE TABLE type_g_5_4_2 (a int, b int, c int, d int, e int, PRIMARY KEY ((a, b), c, d)) WITH COMPACT STORAGE;

-- type H: compound PK, multipart partition key, not all key components used in partitioning,
-- dynamic storage
CREATE TABLE type_h_3_3_2 (a int, b int, c int, PRIMARY KEY ((a, b), c)); 
CREATE TABLE type_h_4_3_2 (a int, b int, c int, d int, PRIMARY KEY ((a, b), c));
CREATE TABLE type_h_4_4_2 (a int, b int, c int, d int, PRIMARY KEY ((a, b), c, d));
CREATE TABLE type_h_4_4_3 (a int, b int, c int, d int, PRIMARY KEY ((a, b, c), d));
CREATE TABLE type_h_5_3_2 (a int, b int, c int, d int, e int, PRIMARY KEY ((a, b), c));
CREATE TABLE type_h_5_4_2 (a int, b int, c int, d int, e int, PRIMARY KEY ((a, b), c, d));

-- type A with collections (these should fail, but don't)
CREATE TABLE type_aa_2_2 (a int PRIMARY KEY, b map<float, text>) WITH COMPACT STORAGE;
CREATE TABLE type_aa_3_2 (a int PRIMARY KEY, b map<float, text>, c int) WITH COMPACT STORAGE;

-- type B with collections
CREATE TABLE type_bb_2_2 (a int PRIMARY KEY, b map<float, text>);
CREATE TABLE type_bb_3_2 (a int PRIMARY KEY, b map<float, text>, c int);
CREATE TABLE type_bb_3_23 (a int PRIMARY KEY, b map<float, text>, c set<int>);

-- type C with collections
-- FAILS: CREATE TABLE type_cc_4_3_2 (a int, b map<float, text>, c int, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE;
-- Bad Request: Invalid collection type for PRIMARY KEY component b
-- FAILS: CREATE TABLE type_cc_4_3_4 (a int, b int, c int, d map<float, text>, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE;
-- Bad Request: Collection types are not supported with COMPACT STORAGE

-- type D with collections
-- FAILS: CREATE TABLE type_dd_3_2_1 (a map<float, text>, b int, c int, PRIMARY KEY (a, b));
-- Bad Request: Invalid collection type for PRIMARY KEY component a
-- FAILS: CREATE TABLE type_dd_3_2_2 (a int, b map<float, text>, c int, PRIMARY KEY (a, b));
-- Bad Request: Invalid collection type for PRIMARY KEY component b
CREATE TABLE type_dd_3_2_3 (a int, b int, c map<float, text>, PRIMARY KEY (a, b));
CREATE TABLE type_dd_4_3_4 (a int, b int, c int, d map<float, text>, PRIMARY KEY (a, b, c));
CREATE TABLE type_dd_5_3_4 (a int, b int, c int, d map<float, text>, e int, PRIMARY KEY (a, b, c));
CREATE TABLE type_dd_5_3_45 (a int, b int, c int, d map<float, text>, e list<int>, PRIMARY KEY (a, b, c));

-- type E with collections (these should all fail, but some don't)
-- FAILS: CREATE TABLE type_ee_3_2_2 (a int, b map<float, text>, c int, PRIMARY KEY ((a, b))) WITH COMPACT STORAGE;
-- Bad Request: Invalid collection type for PRIMARY KEY component b
CREATE TABLE type_ee_3_2_3 (a int, b int, c map<float, text>, PRIMARY KEY ((a, b))) WITH COMPACT STORAGE;
CREATE TABLE type_ee_4_3_4 (a int, b int, c int, d map<float, text>, PRIMARY KEY ((a, b, c))) WITH COMPACT STORAGE;
CREATE TABLE type_ee_5_3_45 (a int, b int, c int, d map<float, text>, e list<int>, PRIMARY KEY ((a, b, c))) WITH COMPACT STORAGE;

-- type F with collections
-- FAILS: CREATE TABLE type_ff_3_2_1 (a list<int>, b int, c int, PRIMARY KEY ((a, b)));
-- Bad Request: Invalid collection type for PRIMARY KEY component a
CREATE TABLE type_ff_3_2_3 (a int, b int, c map<float, text>, PRIMARY KEY ((a, b)));
CREATE TABLE type_ff_4_3_4 (a int, b int, c int, d map<float, text>, PRIMARY KEY ((a, b, c)));
CREATE TABLE type_ff_5_2_45 (a int, b int, c int, d map<float, text>, e list<int>, PRIMARY KEY ((a, b)));
CREATE TABLE type_ff_5_3_45 (a int, b int, c int, d map<float, text>, e list<int>, PRIMARY KEY ((a, b, c)));

-- type G with collections
-- FAILS: CREATE TABLE type_gg_4_3_2_1 (a set<int>, b int, c int, d int, PRIMARY KEY ((a, b), c)) WITH COMPACT STORAGE;
-- Bad Request: Invalid collection type for PRIMARY KEY component a
-- FAILS: CREATE TABLE type_gg_4_3_2_4 (a int, b int, c int, d list<int>, PRIMARY KEY ((a, b), c)) WITH COMPACT STORAGE;
-- Bad Request: Collection types are not supported with COMPACT STORAGE
-- FAILS: CREATE TABLE type_gg_5_3_2_4 (a int, b int, c int, d map<float, text>, e list<int>, PRIMARY KEY ((a, b), c)) WITH COMPACT STORAGE;
-- Bad Request: Collection types are not supported with COMPACT STORAGE

-- type H with collections
-- FAILS: CREATE TABLE type_hh_4_3_2_1 (a set<int>, b int, c int, d int, PRIMARY KEY ((a, b), c));
-- Bad Request: Invalid collection type for PRIMARY KEY component a
-- FAILS: CREATE TABLE type_hh_4_3_2_3 (a int, b int, c list<int>, d int, PRIMARY KEY ((a, b), c));
-- Bad Request: Invalid collection type for PRIMARY KEY component c
CREATE TABLE type_hh_4_3_2_4 (a int, b int, c int, d list<int>, PRIMARY KEY ((a, b), c));
CREATE TABLE type_hh_5_3_2_45 (a int, b int, c int, d map<float, text>, e list<int>, PRIMARY KEY ((a, b), c));
