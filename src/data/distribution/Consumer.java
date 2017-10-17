package data.distribution;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import implementations.collections.JCLHashMap;

import util.FileManip; 

public class Consumer {

	public void save(List<Entry<Integer, String>> buffer, int machineID) throws IOException{
		System.out.println("maquina: " + machineID);
		int localCores = Runtime.getRuntime().availableProcessors();
		int buffSize = buffer.size();
		System.out.println("tamaho buffer: "+buffSize);
		int divide_factor = buffSize/localCores;
		System.out.println("divide factor: "+divide_factor);
		int k = 0;
		int i = 0;

		for(;i<localCores;i++){
			Int2ObjectMap<String> m = new Int2ObjectOpenHashMap<String>();

			System.out.println("core "+i);
			for (int j = 0; j < divide_factor; j++) {
				m.put(buffer.get(k).getKey(), buffer.get(k).getValue());
				k++;
			}

			//Map<Integer, String> hm = new JCLHashMap<>(machineID+":"+i);
			//hm.putAll(m);
			FileManip.writeTuplesTxt(m, i);
			
		}
		if(buffSize%localCores != 0){
			Int2ObjectMap<String> m = new Int2ObjectOpenHashMap<String>();
			for(;k<buffSize;k++){
				m.put(buffer.get(k).getKey(), buffer.get(k).getValue());
			}
			//Map<Integer, String> hm = new JCLHashMap<>(machineID+":"+0);
			//hm.putAll(m);
			FileManip.writeTuplesTxt(m, 0);
		}
		System.out.println("finalizou para maquina " + machineID);
	}
}
