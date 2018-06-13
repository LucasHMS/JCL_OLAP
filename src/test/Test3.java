package test;

import index.Index;
import query.QueryDriver;

public class Test3 {
public static void main(String [] args){
		
		try {
			long t1, t2;	
			
			Index i = new Index();
			t1 = System.currentTimeMillis();
			i.deleteIndices();
			t2 = System.currentTimeMillis();
			System.out.println("(DELETAR INDICES): " + ((t2-t1)*1.0/1000) + "s");
			
			QueryDriver qd = new QueryDriver();
			qd.readAndParse("Cargo contains \"OR\" and Loja contains \"SHOP\" and username contains \"2\"; max total_score;");
			t1 = System.currentTimeMillis();
			QueryDriver.VERBOSITY = false;
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
