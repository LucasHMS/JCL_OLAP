package data.distribution;

import java.io.IOException;
import java.util.List;

import java.util.Map.Entry;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;

public class Consumer {
	JCL_facade jcl = JCL_FacadeImpl.getInstance();

	@SuppressWarnings("unchecked")
	public void save(List<Entry<Integer, String>> buffer, int machineID) throws IOException{
//		System.out.println("maquina: " + machineID);
		int localCores = Runtime.getRuntime().availableProcessors();
		int buffSize = buffer.size();
//		System.out.println("tamaho buffer: "+buffSize);
		int divide_factor = buffSize/localCores;
//		System.out.println("divide factor: "+divide_factor);
		int k = 0;
		int i = 0;
		
		for(;i<localCores;i++){
			Int2ObjectMap<String> m = new Int2ObjectOpenHashMap<String>();
			Int2ObjectMap<String> m_aux = new Int2ObjectOpenHashMap<String>();
			
//			System.out.println("core "+i);
			for (int j = 0; j < divide_factor; j++) {
				m.put(buffer.get(k).getKey(), buffer.get(k).getValue());
				k++;
			}

			//Map<Integer, String> hm = new JCLHashMap<>(machineID+":"+i);
			//hm.putAll(m);
						
			/*Object o = (Object) jcl.getValueLocking(machineID+"_core_"+i).getCorrectResult();
			if(o == null || o.toString().startsWith("No value found!")){
				//m_aux = (Int2ObjectMap<String>) jcl.getValueLocking(machineID+"_core_"+i).getCorrectResult();
				jcl.instantiateGlobalVar(machineID+"_core_"+i, new Int2ObjectOpenHashMap<String>());
				jcl.setValueUnlocking(machineID+"_core_"+i, new Int2ObjectOpenHashMap<String>(m));
			}else{
				m_aux = (Int2ObjectMap<String>) o;
				m_aux.putAll(m);
				jcl.setValueUnlocking(machineID+"_core_"+i, m_aux);
			}*/
			System.out.println("LOCK: M="+machineID+" - C="+i);
			m_aux = (Int2ObjectMap<String>) jcl.getValueLocking(machineID+"_core_"+i).getCorrectResult();
			m_aux.putAll(m);
//			System.out.println("PUTALL: M="+machineID+" - C="+i);
			jcl.setValueUnlocking(machineID+"_core_"+i, m_aux);
			System.out.println("UNLOCK: M="+machineID+" - C="+i);
			util.FileManip.writeTuplesTxt(m, i);
		}
		if(buffSize%localCores != 0){
			System.out.println("ESCREVENDO O QUE SOBROU");
			Int2ObjectMap<String> m = new Int2ObjectOpenHashMap<String>();
			for(;k<buffSize;k++){
				m.put(buffer.get(k).getKey(), buffer.get(k).getValue());
			}
			
			//Map<Integer, String> hm = new JCLHashMap<>(machineID+":"+0);
			//hm.putAll(m);
			
			Int2ObjectMap<String> m_aux = (Int2ObjectMap<String>) jcl.getValueLocking(machineID+"_core_0").getCorrectResult();
			m_aux.putAll(m);
			jcl.setValueUnlocking(machineID+"_core_0", m_aux);
			
			util.FileManip.writeTuplesTxt(m, 0);
		}
		System.out.println("finalizou para maquina " + machineID);
	}
	
	public void instanciateCoreMaps(int machineID) {
		System.out.println("**** intanciando as maps da maquina " + machineID + " ****");
		int localCores = Runtime.getRuntime().availableProcessors();
		for (int i=0; i<localCores;i++) {
			Int2ObjectMap<String> m = new Int2ObjectOpenHashMap<String>();
			m.clear();
			System.out.println(jcl.instantiateGlobalVar(machineID+"_core_"+i,m));
		}
		System.out.println("**** finalizou instanciacao maquina " + machineID + " ****");
	}
}
	
