package test;

import java.io.IOException;

import data.distribution.Producer;

public class Test1 {
	public static void main(String [] args){
		Producer p = new Producer();
		try {
			int size = 150850;
			long t1 = System.currentTimeMillis();
			p.readTupla(size);
			long t2 = System.currentTimeMillis();
			System.out.println("Tempo gasto com " + size + ": " + ((t2-t1)*1.0/1000) + "s");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
