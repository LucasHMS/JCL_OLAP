package data.distribution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import util.CircularArrayList;

import java.util.Map.Entry;
import java.util.concurrent.Future;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_result;

public class Producer 
{
	private List<Entry<Integer, String>> colection = new ArrayList<>();
	private JCL_facade jcl = JCL_FacadeImpl.getInstance();
	private List<Entry<String, String>> host;
	private String tuplas;
	private int contHost=0;
	private List<Future<JCL_result>> results = new ArrayList<>();
	
	public Producer()
	{}
	
	public void readTupla(int size) throws IOException
	{
	//  inicio variaveis
		tuplas = "input/NorthwindSalesData.data";
		BufferedReader br = new BufferedReader(new FileReader(tuplas));
		String line = br.readLine();
		int i = 0;
		int k = 0;
	//  Passo-1: lista circular
		getHosts();
		
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
				sendBuffer();
				colection.clear();
				i = 0;
			}
		}
	//	envia a colecao caso sobre tuplas no fim arquivo
		if(i != 0)
		{
			sendBuffer();
		}
		jcl.getAllResultBlocking(results);
		jcl.cleanEnvironment();
		jcl.destroy();
	}
	
	public void makeBuffer(String line, int i)
	{
	//	cria uma Entry que recebe a chave da tupla e a linha do arquivo
		Entry<Integer, String> item = new MyEntry<Integer, String>(i, line);
	//	adiciona a Entry na lista de colection
		colection.add(item);
	}
	
	public void sendBuffer()
	{
		int machineID;
	//	recebe o ID da maquina para onde a lista de tuplas ira
		if(contHost >= host.size())
		{
			machineID = contHost - host.size();
			while(machineID >= host.size()) machineID = contHost - host.size();
		}
		else
		{
			machineID = contHost;
		}
		Object[] args= {new ArrayList<>(colection), machineID};
	//	passa para a maquina do cluster todos os dados (lista de tuplas e ID)
		jcl.executeOnDevice(host.get(contHost), "Consumer", "save", args);
		contHost++;
	}
	
	public void getHosts()
	{
		File f1 = new File("lib/Consumer.jar");
		File f2 = new File("lib/MyEntry.jar");
		File f3 = new File("lib/CircularArrayList.jar");
		File [] jars = {f1,f2,f3};
		jcl.register(jars, "Consumer");
		host = jcl.getDevices();
	}
	
}