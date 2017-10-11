package index;

import java.util.Map;
import java.util.Map.Entry;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import java.io.IOException;

import util.FileManip;

public class Index {
	JCL_facade jcl = JCL_FacadeImpl.getInstance();
	
	public Index() {
		jcl = JCL_FacadeImpl.getInstance();
		jcl.register(JCL_Index.class, "JCL_Index");		
	}

	// cria arquivos metadata em todas as maquinas do cluster
	public void loadMetadata()
	{
		String mesure = null, dimension = null;
		try {
			// le asrquivos metadata
			mesure = FileManip.metaDataToString("input/mesuresnames.mg");
			dimension = FileManip.metaDataToString("input/dimensionsnames.mg");
		} catch (IOException e) {
			e.printStackTrace();
		}

		Object [] args = {mesure, dimension};
		// escreve os arquivos metadata em todas as maquinas do cluster
		jcl.getAllResultBlocking(jcl.executeAll("fileManip", "writeMetaData", args));
		// cria as hashMaps com os metadados para cada maquina
		jcl.getAllResultBlocking(jcl.executeAll("fileManip", "readMetaData", null));
	}

	public void createIndex(){
		Map<Entry<String,String>,Integer> hosts = jcl.getAllDevicesCores();
		Object [][] arg = new Object[hosts.size()][];
		int i = 0;
		for(Entry<Entry<String, String>, Integer> e : hosts.entrySet()) {
			for(int j=0;j<e.getValue();j++) {
				Object [] a = {i,j};
				arg[i][j] = a; 
			}
		}
		
		jcl.getAllResultBlocking(jcl.executeAllCores("JCL_Index", "createIndex", arg));
	}
}
