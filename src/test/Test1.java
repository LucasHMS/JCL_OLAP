package test;


//import data.distribution.BalancedProducer;
import data.distribution.InvariantProducer;
import data.distribution.Producer;
import index.Index;
import query.QueryDriver;

public class Test1 {
	public static void main(String [] args){
		
		try {
			int size = 5000;
			boolean saveToFile = false;
			Producer p = new InvariantProducer(size,saveToFile);
			long t1 = System.currentTimeMillis();
			p.distributeDataBase("input/NorthwindSalesData1.data");
			long t2 = System.currentTimeMillis();
			System.out.println("(CRIAR ARQUIVOS "+ saveToFile +") Tempo gasto com " + size + ": " + ((t2-t1)*1.0/1000) + "s");
			
			Index i = new Index();
			t1 = System.currentTimeMillis();
			i.loadMetadata("input/");
			t2 = System.currentTimeMillis();
			System.out.println("(CARREGAR METADADOS): " + ((t2-t1)*1.0/1000) + "s");
			
			t1 = System.currentTimeMillis();
			i.createIndex("Map");
			t2 = System.currentTimeMillis();
			System.out.println("(CRIAR INDICES): " + ((t2-t1)*1.0/1000) + "s");
			
			QueryDriver qd = new QueryDriver();
			qd.readAndParse("Categoria > '5' and Pais startsWith 'B' and Produto endsWith 's' and Cidade startsWith 'Rio'; max PrecoUnitario");
			t1 = System.currentTimeMillis();
			qd.filterQuery();
			t2 = System.currentTimeMillis();
			System.out.println("(FILTRAGEM): " + ((t2-t1)*1.0/1000) + "s");
			
			t1 = System.currentTimeMillis();
			qd.createCube();
			t2 = System.currentTimeMillis();
			System.out.println("(CRIAR CUBO): " + ((t2-t1)*1.0/1000) + "s");
			
			QueryDriver.VERBOSITY = true;
			t1 = System.currentTimeMillis();
			qd.aggregateCubes();
			t2 = System.currentTimeMillis();
			System.out.println("(AGREGAÇÃO): " + ((t2-t1)*1.0/1000) + "s");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

/*
 * exemplos:
 *  (Base 0, 1 ou 2) "Categoria > '5' and Pais startsWith 'B' and Produto endsWith 's' and Cidade startsWith 'Rio'; max PrecoUnitario"
 *  (Base 3)"Pais startsWith 'S' and Cidade startsWith 'S' and Empresa startsWith 'R'; max PrecoUnitario"
 * 
 * "Cargo contains 'OR' and Loja contains 'SHOP' and username contains '2'; max total_score"
 * 
 * */
