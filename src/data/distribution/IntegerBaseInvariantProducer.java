package data.distribution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_result;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import javafx.util.Pair;

public class IntegerBaseInvariantProducer implements Producer {

	private List<Pair<Integer, IntList>> buffer;
	private JCL_facade jcl;
	private List<Entry<String, String>> hosts;
	private int contHost;
	private int bufferSize;
	private boolean saveToFile;
	
	public IntegerBaseInvariantProducer(int bufferSize, boolean saveToFile) {	
		this.bufferSize = bufferSize;
		this.saveToFile = saveToFile;
		jcl = JCL_FacadeImpl.getInstance();
		contHost=0;
		buffer = new ArrayList<>();
		
		File f1 = new File("lib/integerconsumer.jar");
		File f4 = new File("lib/filemanip.jar");
		File [] jars = {f1, f4};
		jcl.register(jars, "IntegerBaseConsumer");

		hosts = jcl.getDevices();
		List<Future<JCL_result>> tickets = new ArrayList<Future<JCL_result>>(); 
		int j = 0;
		for(Entry<String,String> e : hosts) {
			Object [] args = {new Integer(j)};
			tickets.add(jcl.executeOnDevice(e,"IntegerBaseConsumer", "instanciateCoreMaps", args));
			j++;
		}
		jcl.getAllResultBlocking(tickets);
	}
	
	public void distributeDataBase(String fileName) throws IOException {
	//  inicio variaveis
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String line = br.readLine(); // desconsiderando cabeçalho
		int i = 0;
		int k = 0;
		
		//  recebe tupla do arquivo
		while((line = br.readLine()) != null) {
//			line = k+"|"+line;
			IntList values = new IntArrayList(Arrays.asList(line.split("\\|")).stream().mapToInt(Integer::parseInt).toArray());
			buffer.add(new Pair<Integer, IntList>(k,values));
		//  Passo-2: cria uma lista de tuplas
//			makeBuffer(values, k);
		//	incrementa a chave
			i++;
			k++;
			if(i == bufferSize) {
			//	Passo-3: envia para as maquinas
				sendBuffer();
				buffer.clear();
				i = 0;
			}
		}
	//	envia a colecao caso sobre tuplas no fim arquivo
		if(i != 0) {
			sendBuffer();
		}
		
		br.close();
	}
	
	private void sendBuffer() {
		int machineID;
	//	recebe o ID da maquina para onde a lista de tuplas ira
		if(contHost == hosts.size()) {
			machineID = contHost - hosts.size();
		}
		else if(contHost>hosts.size()) {
			machineID = contHost - hosts.size();
			while(machineID >= hosts.size()) machineID -= hosts.size();
		}
		else {
			machineID = contHost;
		}
		Object[] args= {new ArrayList<>(buffer), machineID, saveToFile};
	
	//	passa para a maquina do cluster todos os dados (lista de tuplas e ID)
		try {
			jcl.executeOnDevice(hosts.get(machineID),"IntegerBaseConsumer", "save", args).get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		contHost++;
	}
	
	public void deleteDistributedBase() {
		for(int i=0;i<hosts.size();i++) {
			int n = jcl.getDeviceCore(hosts.get(i));
			for(int j=0;j<n;j++) {
				jcl.deleteGlobalVar(i+"_core_"+j);
			}
		}
	}

}
