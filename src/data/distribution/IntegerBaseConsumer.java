package data.distribution;

import java.io.IOException;
import java.util.List;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import javafx.util.Pair;

public class IntegerBaseConsumer {
	JCL_facade jcl = JCL_FacadeImpl.getInstance();

	@SuppressWarnings("unchecked")
	public void save(List<Pair<Integer, IntList>> buffer, int machineID, boolean saveToFile) throws IOException{
		int localCores = Runtime.getRuntime().availableProcessors();
		int buffSize = buffer.size();
		int divide_factor = buffSize/localCores;
		int k = 0;
		int i = 0;
		
		for(;i<localCores;i++){
			Int2ObjectMap<IntList> m = new Int2ObjectOpenHashMap<>();
			Int2ObjectMap<IntList> m_aux = new Int2ObjectOpenHashMap<>();
			
			for (int j = 0; j < divide_factor; j++) {
				m.put(buffer.get(k).getKey(), buffer.get(k).getValue());
				k++;
			}

			m_aux = (Int2ObjectMap<IntList>) jcl.getValueLocking(machineID+"_core_"+i).getCorrectResult();
			m_aux.putAll(m);
			jcl.setValueUnlocking(machineID+"_core_"+i, m_aux);
			
			if(saveToFile) util.FileManip.writeTuplesTxt(m, i);
		}
		if(buffSize%localCores != 0){
			Int2ObjectMap<IntList> m = new Int2ObjectOpenHashMap<>();
			for(;k<buffSize;k++){
				m.put(buffer.get(k).getKey(), buffer.get(k).getValue());
			}
			
			Int2ObjectMap<IntList> m_aux = (Int2ObjectMap<IntList>) jcl.getValueLocking(machineID+"_core_0").getCorrectResult();
			m_aux.putAll(m);
			jcl.setValueUnlocking(machineID+"_core_0", m_aux);

			if(saveToFile) util.FileManip.writeTuplesTxt(m, 0);
		}
		System.out.println("Finalizou Buffer de tamanho: " + buffer.size());
	}
	
	public void instanciateCoreMaps(int machineID) {
		int localCores = Runtime.getRuntime().availableProcessors();
		for (int i=0; i<localCores;i++) {
			Int2ObjectMap<IntList> m = new Int2ObjectOpenHashMap<IntList>();
			m.clear();
			jcl.instantiateGlobalVar(machineID+"_core_"+i,m);
		}
	}
	
	public int getMemory() {
		/*long memorySize = ((com.sun.management.OperatingSystemMXBean) 
							ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize();*/
		long memorySize = Runtime.getRuntime().maxMemory();
		return (int) Math.floor(1.0*memorySize/1000000000);
	}
}
