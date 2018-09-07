package test;

import data.distribution.IntegerBaseInvariantProducer;
import data.distribution.Producer;
import index.Index;
import query.IntegerBaseQueryDriver;
import static query.IntegerBaseQueryDriver.VERBOSITY;

public class Test4 {
	public static void main(String [] args){
		try {
			VERBOSITY = false;
			int size = 5000;
			boolean saveToFile = false;
			Producer p = new IntegerBaseInvariantProducer(size,saveToFile);
			long t1 = System.currentTimeMillis();
			p.distributeDataBase("input/IntBases/100_5_2/Base.data");
			long t2 = System.currentTimeMillis();
			System.out.println("(CRIAR ARQUIVOS "+ saveToFile +") Tempo gasto com " + size + ": " + ((t2-t1)*1.0/1000) + "s");
			
			Index i = new Index();
			t1 = System.currentTimeMillis();
			i.loadMetadata("input/IntBases/100_5_2/");
			t2 = System.currentTimeMillis();
			System.out.println("(CARREGAR METADADOS): " + ((t2-t1)*1.0/1000) + "s");
			
			t1 = System.currentTimeMillis();
			i.createIntegerIndex();
			t2 = System.currentTimeMillis();
			System.out.println("(CRIAR INDICES): " + ((t2-t1)*1.0/1000) + "s");
			
			IntegerBaseQueryDriver qd = new IntegerBaseQueryDriver();
			qd.readAndParse("A > '6' and B > '6' and C > '6'; max M1");
			t1 = System.currentTimeMillis();
			qd.filterQuery();
			t2 = System.currentTimeMillis();
			System.out.println("(FILTRAGEM): " + ((t2-t1)*1.0/1000) + "s");
			
			t1 = System.currentTimeMillis();
			qd.createCube();
			t2 = System.currentTimeMillis();
			System.out.println("(CRIAR CUBO): " + ((t2-t1)*1.0/1000) + "s");
		
			t1 = System.currentTimeMillis();
			qd.aggregateCubes();
			t2 = System.currentTimeMillis();
			System.out.println("(AGREGAÇÃO): " + ((t2-t1)*1.0/1000) + "s");
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
