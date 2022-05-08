// Without alias
SELECT intAsBlob(4) FROM t;

//  intAsBlob(4)
// --------------
//  0x00000004

// With alias
SELECT intAsBlob(4) AS four FROM t;

//  four
// ------------
//  0x00000004
