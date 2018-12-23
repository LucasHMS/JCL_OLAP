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
	private List<Future<JCL_result>> tickets;
	
	public IntegerBaseInvariantProducer(int bufferSize, boolean saveToFile) {	
		this.bufferSize = bufferSize;
		this.saveToFile = saveToFile;
		jcl = JCL_FacadeImpl.getInstance();
		contHost=0;
		buffer = new ArrayList<>();
		tickets = new ArrayList<>();
		
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
		
		tickets.forEach(jcl::removeResult);

	}
	
	public void distributeDataBase(String fileName) throws IOException, InterruptedException, ExecutionException {
	//  inicio variaveis
		BufferedReader br = new BufferedReader(new FileReader(fileName)); br.readLine(); // desconsiderando cabeçalho
		int baseSize = Integer.parseInt(br.readLine().trim());
		int portion = baseSize/hosts.size();
		int reminder = baseSize%hosts.size();
		
		distributeDataBaseUtil(br, portion, reminder);
		
		//distributeDataBaseUtil(br);
		
		br.close();
	}
	
	// le o arquivo em blocos gigantes divididos igualmente entre os hosts
	private void distributeDataBaseUtil(BufferedReader br, int portion, int reminder) throws IOException, InterruptedException, ExecutionException {
		String line;
		int i = 0;
		int k = 0;
		for(Entry<String,String> device : hosts) {
			System.out.println("lendo porcao do device " + i);
			for(int j=0; j<portion; j++) {
				line = br.readLine();
				IntList values = new IntArrayList(Arrays.asList(line.split("\\|")).stream().mapToInt(Integer::parseInt).toArray());
				buffer.add(new Pair<Integer, IntList>(k++,values));
			}
			sendBuffer(device, i++);
			buffer.clear();
		}
		if(reminder != 0) {
			while((line = br.readLine()) != null) {
				IntList values = new IntArrayList(Arrays.asList(line.split("\\|")).stream().mapToInt(Integer::parseInt).toArray());
				buffer.add(new Pair<Integer, IntList>(k++,values));
			}
			sendBuffer(hosts.get(0), 0);
			buffer.clear();
		}
		jcl.getAllResultBlocking(tickets);
		
		tickets.forEach(jcl::removeResult);
	}

	// le o arquivo em buffers enviados em lista circular para cara host
	private void distributeDataBaseUtil(BufferedReader br) throws IOException, InterruptedException, ExecutionException {
		String line;
		int i = 0;
		int k = 0;

		while((line = br.readLine()) != null) {
			IntList values = new IntArrayList(Arrays.asList(line.split("\\|")).stream().mapToInt(Integer::parseInt).toArray());
			buffer.add(new Pair<Integer, IntList>(k,values));
			i++;
			k++;
			if(i == bufferSize) {
				sendBuffer();
				buffer.clear();
				i = 0;
			}
		}
		if(i != 0) {
			sendBuffer();
		}

	}

	private void sendBuffer(Entry<String,String> device, int machineID) throws InterruptedException, ExecutionException {
		Object[] args= {new ArrayList<>(buffer), machineID, saveToFile};
		tickets.add(jcl.executeOnDevice(hosts.get(machineID),"IntegerBaseConsumer", "save", args));
	}
	
	private void sendBuffer() throws InterruptedException, ExecutionException {
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
		jcl.executeOnDevice(hosts.get(machineID),"IntegerBaseConsumer", "save", args).get();
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
