import java.util.List;
import java.util.Map;

import implementations.collections.JCLHashMap;

public class Test {
	public static void main(String [] args){
		Map<Integer,List<String>> mesureIndex = new JCLHashMap<Integer, List<String>>("mesureIndexT"+0);
		System.out.println(mesureIndex.get(108));
	}
}
