package data.distribution;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.util.Map.Entry;

import implementations.sm_kernel.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;

public class Producer 
{
	private List<Entry<Integer, String>> colection = new ArrayList<>();
	private JCL_facade jcl = JCL_FacadeImpl.getInstance();
	private String tuplas;
	
	public Producer()
	{}
	
	public void readTupla() throws IOException
	{
		BufferedReader br = new BufferedReader(new FileReader(tuplas));
		String line = br.readLine();
		int i = 0;
		
		while(br.ready())
		{
			line = br.readLine();
			makeBuffer(line, i);
			i++;
			if(i == 1000)
			{
				sendBuffer();
				colection.clear();
				i = 0;
			}
		}
		if(i != 0)
		{
			sendBuffer();
		}
	}
	
	public void makeBuffer(String line, int i)
	{
		Entry<Integer, String> item = new MyEntry<Integer, String>(i, line);
		colection.add(item);
	}
	
	public void sendBuffer()
	{
		
	}
	
}
