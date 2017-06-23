package data.distribution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import util.CircularArrayList;

import java.util.Map.Entry;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;

public class Producer 
{
	private List<Entry<Integer, String>> colection = new ArrayList<>();
	private JCL_facade jcl = JCL_FacadeImpl.getInstance();
	private List<Entry<String, String>> host;
	private String tuplas;
	private int contHost;
	
	public Producer()
	{}
	
	public void readTupla() throws IOException
	{
	//  inicio variaveis
		tuplas = "input/NorthwindSalesData.data";
		BufferedReader br = new BufferedReader(new FileReader(tuplas));
		String line = br.readLine();
		int i = 0;
	//  Passo-1: lista circular
		getHosts();
		
		while(br.ready())
		{
		//  recebe tupla do arquivo
			line = br.readLine();
		//  Passo-2: cria uma lista de tuplas
			makeBuffer(line, i);
		//	incrementa a chave
			i++;
			if(i == 1000)
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
		}
		else
		{
			machineID = contHost;
		}
		Object[] args= {colection, machineID};
	//	passa para a maquina do cluster todos os dados (lista de tuplas e ID)
		jcl.executeOnDevice(host.get(contHost), "Consumer", "save", args);
		contHost++;
	}
	
	public void getHosts()
	{
	//	registra as classe externas
		jcl.register(MyEntry.class, "MyEntry");
		jcl.register(util.CircularArrayList.class, "CircularArrayList");
	//  cria uma lista circular dos devices do cluster
		host = new CircularArrayList<>();
		host = jcl.getDevices();
	//  registra a classe
		jcl.register(Consumer.class, "Consumer");
	}
	
}
