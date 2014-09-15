package org.apache.cassandra.hadoop.pig.udf;

import java.io.IOException;
import java.net.URLEncoder;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class URLEncode extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
			String inputStr = (String) input.get(0);
			return URLEncoder.encode(inputStr,"UTF-8");
	}
}
