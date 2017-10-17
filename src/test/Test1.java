package test;

import java.io.IOException;

import data.distribution.Producer;
import index.Index;

public class Test1 {
	public static void main(String [] args){
		Producer p = new Producer();
		try {
			int size = 5000;
			long t1 = System.currentTimeMillis();
			p.readTupla(size);
			long t2 = System.currentTimeMillis();
			System.out.println("(CRIAR ARQUIVOS) Tempo gasto com " + size + ": " + ((t2-t1)*1.0/1000) + "s");
			
			Index i = new Index();
			
			t1 = System.currentTimeMillis();
			i.loadMetadata();
			t2 = System.currentTimeMillis();
			System.out.println("(CARREGAR METADADOS)" + ((t2-t1)*1.0/1000) + "s");
			
			t1 = System.currentTimeMillis();
			i.createIndex();
			t2 = System.currentTimeMillis();
			System.out.println("(CRIAR INDICES)" + ((t2-t1)*1.0/1000) + "s");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
}
