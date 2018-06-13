package data.distribution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_result;
import javafx.util.Pair;
import util.HostWrapper;
import util.MyEntry;

public class BalancedProducer implements Producer{
	private JCL_facade jcl;
	private List<Entry<String, String>> hostsJCL;
	private DistributionBalancer db;
	private BufferedReader file;
	private int bufferSize;
	private boolean saveToFile;
	private static int keyCounter = 0;
	
	public BalancedProducer(int bufferSize, boolean saveToFile) throws IOException {
		this.bufferSize = bufferSize;
		this.saveToFile = saveToFile;
		
		jcl = JCL_FacadeImpl.getInstance();
		File f1 = new File("lib/consumer.jar");
		File f2 = new File("lib/myentry.jar");
		File f4 = new File("lib/filemanip.jar");
		File [] jars = {f1,f2, f4};
		jcl.register(jars, "Consumer");
		
		
		List<Entry<String, String>> hostsJCL = jcl.getDevices();
		List<HostWrapper> hosts = new ArrayList<>();
		int i=0;
		for(Entry<String,String> h : hostsJCL) {
			HostWrapper hw = new HostWrapper(h, i++);
			hosts.add(hw);
		}
		
		List<Future<JCL_result>> tickets = new ArrayList<Future<JCL_result>>(); 
		for(HostWrapper h : hosts) {
			Object [] args = {new Integer(h.machineId)};
			tickets.add(jcl.executeOnDevice(h.jclHost,"Consumer", "instanciateCoreMaps", args));
		}
		jcl.getAllResultBlocking(tickets);

		db = new DistributionBalancer(hosts);
		db.caculateRatios();
	}
	
	public void distributeDataBase(String fileName) throws IOException {
		// inicializa um novo arquivo e descarta o cabecalho
		file = new BufferedReader(new FileReader(fileName)); file.readLine();
		boolean eofFlag = true;
		while(eofFlag) {
			for(Pair<Pair<List<HostWrapper>,List<HostWrapper>>,Pair<Integer,Integer>> hosts2ratio : db.calcRatios) {
				// recupera a lista dos hots com mais memoria e a quantidade calculada para eles
				int qtdH1 = hosts2ratio.getValue().getKey();
				for(HostWrapper host : hosts2ratio.getKey().getKey()) {
					for(int i=0;i<qtdH1;i++) {
						List<Entry<Integer, String>> buffer = readFile();
						if(buffer.size() != 0){
							sendTuples(host, buffer);
						}
						else {
							eofFlag = false;
							break;
						}
					}
					if(!eofFlag) break;
				}
				if(!eofFlag) break;
				
				// recupera a lista dos hots com menos memoria e a quantidade calculada para eles
				int qtdH2 = hosts2ratio.getValue().getValue();
				for(HostWrapper host : hosts2ratio.getKey().getValue()) {
					for(int i=0;i<qtdH2;i++) {
						List<Entry<Integer, String>> buffer = readFile();
						if(buffer.size() != 0){
							sendTuples(host, buffer);
						}
						else {
							eofFlag = false;
							break;
						}					
					}
					if(!eofFlag) break;
				}
				if(!eofFlag) break;
				
			}
		}
	}
	
	private List<Entry<Integer, String>> readFile() throws IOException {
		List<Entry<Integer, String>> buffer = new ArrayList<>();
		int i = 0; 
		
		String line;
		while((line = file.readLine()) != null){
			buffer.add(new MyEntry<Integer, String>(keyCounter, keyCounter+"|"+line));
			i++; keyCounter++;
			if(i == bufferSize){
				return buffer; // retorna um buffer completo
			}
		}
		return buffer; // retorna um buffer incompleto ou vazio (arquivo terminado)
	}
	
	private void sendTuples(HostWrapper host, List<Entry<Integer, String>> buffer) {
		try {
			Object[] args= {new ArrayList<>(buffer), host.machineId, saveToFile};
			jcl.executeOnDevice(host.jclHost,"Consumer", "save", args).get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	public void deleteDistributedBase() {
		for(int i=0;i<hostsJCL.size();i++) {
			int n = jcl.getDeviceCore(hostsJCL.get(i));
			for(int j=0;j<n;j++) {
				jcl.deleteGlobalVar(i+"_core_"+j);
			}
		}
	}
}
