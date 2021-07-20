package org.apache.cassandra.hadoop.pig.udf;

import static org.junit.Assert.*;

import org.apache.cassandra.hadoop.pig.udf.URLEncode;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class UrlEncoderUDFTest {
	
    TupleFactory tf = TupleFactory.getInstance();
    String url = "http://example.com/query?q="+ "random word 500 bank";
    String expectedStr = "http%3A%2F%2Fexample.com%2Fquery%3Fq%3Drandom+word+500+bank";

	@Test
	public void test() throws Exception {
		Tuple t = tf.newTuple();
		t.append(url);
		String encoded = new URLEncode().exec(t);
		assertEquals(encoded, expectedStr);
	}

}
