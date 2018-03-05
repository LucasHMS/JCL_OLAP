package index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_result;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntCollection;

import java.io.File;
import java.io.IOException;

import util.FileManip;

public class Index {
	JCL_facade jcl = JCL_FacadeImpl.getInstance();

	public Index() {
		jcl = JCL_FacadeImpl.getInstance();
		//jcl.register(FileManip.class, "fileManip");
		/*File f1 = new File("lib/jclindex.jar");
		File [] f = { f1 };*/
		System.out.println("registro jcl index: "+jcl.register(JCL_Index.class, "JCL_Index"));
	}

	// cria arquivos metadata em todas as maquinas do cluster
	public void loadMetadata()
	{
		String mesure = null, dimension = null;
		try {
			// le asrquivos metadata
			mesure = FileManip.metaDataToString("input/measuresnames.mg");
			dimension = FileManip.metaDataToString("input/dimensionsnames.mg");
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<Entry<String,String>> hosts = jcl.getDevices();
		Object [][] args = new Object[hosts.size()][];
		for(int i=0;i < hosts.size(); i++) {
			Object [] a = {mesure, dimension};
			args[i] = a; 
		}
		//Object [] args = {mesure, dimension};

		//escreve os arquivos metadata em todas as maquinas do cluster
		jcl.getAllResultBlocking(jcl.executeAll("JCL_Index", "writeMetaData", args));

		//cria as hashMaps com os metadados para cada maquina
		jcl.getAllResultBlocking(jcl.executeAll("JCL_Index", "readMetaData"));

/*		try {
			jcl.execute("JCL_Index", "writeMetaData", args).get();
			jcl.execute("JCL_Index", "readMetaData").get();

		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}*/
	}

	public void createIndex(){
		/*			Map<Entry<String,String>,Integer> hosts = jcl.getAllDevicesCores();
		Object [] args = new Object[jcl.getClusterCores()];
		int i = 0;
		for(Entry<Entry<String, String>, Integer> e : hosts.entrySet()) {
			//args[i] = new Object[e.getValue()];
			for(int j=0;j<e.getValue();j++) {
				Object [] a = {i,j};
				//args[i][j] = new Object[2];
				args[i][j] = j; 
			}
			i++;
		}

		for(i=0;i<hosts.size();i++){
			for(int j=0;j<args[i].length;j++){
				System.out.print(args[i][j]+ " ");
			}
			System.out.println("\n");
		}
		 */
		//Object [] args = {};
		List<Entry<String,String>> devices = jcl.getDevices();
		List<Future<JCL_result>> tickets = new ArrayList<Future<JCL_result>>(); 
		for(Entry<String,String> e : devices) {
			int n = jcl.getDeviceCore(e);
			for(int i=0;i<n;i++) {
//				jcl.executeOnDevice(e, "JCL_Index", "teste");
				Object [] args = {new Integer(i)};
				tickets.add(jcl.executeOnDevice(e,"JCL_Index", "createIndexFromMap", args));
			}
				
		}
		jcl.getAllResultBlocking(tickets);
		
/*		for (Future<JCL_result> t : tickets) {
			try {
				JCL_result res = t.get();
				Exception ex = res.getErrorResult();
				ex.printStackTrace();
//				t.get().getErrorResult().printStackTrace();
			} catch (InterruptedException | ExecutionException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			};
		}*/
		/*Object [] args = {1};
		try {
			jcl.execute("JCL_Index", "createIndex", args).get();
		} catch (InterruptedException | ExecutionException e1) {
			e1.printStackTrace();
		}

		Map<String, IntCollection> m1 = JCL_FacadeImpl.GetHashMap("invertedIndex");
		Map<String, Int2DoubleOpenHashMap> m2 = JCL_FacadeImpl.GetHashMap("mesureIndex");

		System.out.println(m1.size() + " : " + m2.size());*/

	}
}
