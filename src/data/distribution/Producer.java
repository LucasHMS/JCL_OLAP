package data.distribution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.util.Map.Entry;
import java.util.concurrent.Future;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_result;
import util.MyEntry;

public class Producer 
{
	private List<Entry<Integer, String>> colection = new ArrayList<>();
	private JCL_facade jcl = JCL_FacadeImpl.getInstance();
	private List<Entry<String, String>> host;
	private String tuplas;
	private int contHost=0;
	private List<Future<JCL_result>> results = new ArrayList<>();
	
	public Producer()
	{	
		File f1 = new File("lib/consumer.jar");
		File f2 = new File("lib/myentry.jar");
		File f4 = new File("lib/filemanip.jar");
		File [] jars = {f1,f2, f4};
		jcl.register(jars, "Consumer");

		host = jcl.getDevices();
		List<Future<JCL_result>> tickets = new ArrayList<Future<JCL_result>>(); 
		int j = 0;
		for(Entry<String,String> e : host) {
				Object [] args = {new Integer(j)};
				tickets.add(jcl.executeOnDevice(e,"Consumer", "instanciateCoreMaps", args));
			j++;
		}
		jcl.getAllResultBlocking(tickets);
	}
	
	public void readTupla(int size, String fileName) throws IOException
	{
	//  inicio variaveis
		tuplas = fileName;
		BufferedReader br = new BufferedReader(new FileReader(tuplas));
		String line = br.readLine();
		int i = 0;
		int k = 0;
		
		//  recebe tupla do arquivo
		while((line = br.readLine()) != null)
		{
		//  Passo-2: cria uma lista de tuplas
			makeBuffer(line, k);
		//	incrementa a chave
			i++;
			k++;
			if(i == size)
			{
			//	Passo-3: envia para as maquinas
				System.out.println("enviou " + i);
				sendBuffer();
				System.out.println("finalizou envio");
				colection.clear();
				i = 0;
			}
		}
	//	envia a colecao caso sobre tuplas no fim arquivo
		if(i != 0)
		{
			System.out.println("enviou o que sobrou");
			System.out.println("enviou " + i);
			sendBuffer();
			System.out.println("finalizou envio");
		}
		
		System.out.println("Bloqueando para finalizar");
		
		jcl.getAllResultBlocking(results);
		System.out.println("finalizou");
		br.close();
	}
	
	public void makeBuffer(String line, int i)
	{
	//	cria uma Entry que recebe a chave da tupla e a linha do arquivo
		Entry<Integer, String> item = new MyEntry<Integer, String>(i, i+"|"+line);
	//	adiciona a Entry na lista de colection
		colection.add(item);
	}
	
	public void sendBuffer()
	{
		int machineID;
	//	recebe o ID da maquina para onde a lista de tuplas ira
		if(contHost == host.size())
		{
			machineID = contHost - host.size();
		}
		else if(contHost>host.size())
		{
			machineID = contHost - host.size();
			while(machineID >= host.size()) machineID -= host.size();
		}
		else
		{
			machineID = contHost;
		}
		Object[] args= {new ArrayList<>(colection), machineID};
	
	//	passa para a maquina do cluster todos os dados (lista de tuplas e ID)
		results.add(jcl.execute("Consumer", "save", args));
		System.out.println("enviou para maquina " + machineID);
		contHost++;
	}
}
