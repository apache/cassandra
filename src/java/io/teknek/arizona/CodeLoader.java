package io.teknek.arizona;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.teknek.arizona.transform.Increment;
import io.teknek.arizona.transform.SimpleTransformer;
import io.teknek.arizona.transform.Transformer;
import io.teknek.nit.NitDesc;
import io.teknek.nit.NitException;
import io.teknek.nit.NitFactory;

/**
 * We should be doing this into System keyspace and doing an annoucement. But for now we cheat.
 * @author edward
 *
 */
public class CodeLoader {

  public static CodeLoader INSTANCE = new CodeLoader();
  private static Map <String,Transformer> transformers = new ConcurrentHashMap<>();
  static {
    transformers.put("increment", new Increment());
    transformers.put("simple_transformer", new SimpleTransformer());
  }
  
  public void reloadTransformer(NitDesc desc, String name) throws NitException {
    Transformer t = NitFactory.construct(desc);
    transformers.put(name, t);
  }
  
  public Transformer getTransformer(String name){
    return transformers.get(name);
  }
  
}
