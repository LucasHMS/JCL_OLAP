package data.distribution;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import implementations.collections.JCLHashMap;;

public class Consumer {
	
	public void save(List<Entry<Integer, String>> buffer, int machineID){
		int localCores = Runtime.getRuntime().availableProcessors();
		int buffSize = buffer.size();
		
		int divide_factor = buffSize/localCores;
		int k = 0;
		int i = 0;
		for(;i<localCores;i++){
			Map<Integer, String> hm = new JCLHashMap<>(machineID+":"+i);
			System.out.println("core "+i);
			for (int j = 0; j < divide_factor; j++) {
			//	System.out.println(k);
				hm.put(buffer.get(k).getKey(), buffer.get(k).getValue());
				k++;
			}
		}
		if(buffSize%localCores != 0){
			Map<Integer, String> hm = new JCLHashMap<>(machineID+":"+i);
			for(;k<buffSize;k++){
				hm.put(buffer.get(k).getKey(), buffer.get(k).getValue());
			}
		}
		System.out.println("finalizou para maquina " + machineID);
	}
}