package index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_result;

import java.io.IOException;

import util.FileManip;

public class Index {
	JCL_facade jcl = JCL_FacadeImpl.getInstance();

	public Index() {
		jcl = JCL_FacadeImpl.getInstance();
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

		//escreve os arquivos metadata em todas as maquinas do cluster
		jcl.getAllResultBlocking(jcl.executeAll("JCL_Index", "writeMetaData", args));

		//cria as hashMaps com os metadados para cada maquina
		jcl.getAllResultBlocking(jcl.executeAll("JCL_Index", "readMetaData"));
	}

	public void createIndex(String origin){
		List<Entry<String,String>> devices = jcl.getDevices();
		List<Future<JCL_result>> tickets = new ArrayList<Future<JCL_result>>(); 
		int j = 0;
		for(Entry<String,String> e : devices) {
			int n = jcl.getDeviceCore(e);
			for(int i=0;i<n;i++) {
				Object [] args = {new Integer(j), new Integer(i)};
				tickets.add(jcl.executeOnDevice(e,"JCL_Index", "createIndexFrom"+origin, args));
			}
			j++;
		}
		jcl.getAllResultBlocking(tickets);
		
		/*System.out.println("Inverted Index");
		for(int x=0;x<4;x++) 
			System.out.println("CORE " + x + " " + jcl.getValue(0+"_invertedIndex_"+x).getCorrectResult());
		System.out.println("Mesure Index");
		for(int x=0;x<4;x++)
			System.out.println("CORE " + x + " " + jcl.getValue(0+"_mesureIndex_"+x).getCorrectResult());*/
	}
}
