// Without alias
SELECT int_as_blob(4) FROM t;

//  int_as_blob(4)
// ----------------
//  0x00000004

// With alias
SELECT int_as_blob(4) AS four FROM t;

//  four
// ------------
//  0x00000004
