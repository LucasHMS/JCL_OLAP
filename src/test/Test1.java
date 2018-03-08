package test;

import java.io.IOException;

import data.distribution.Producer;
import index.Index;
import query.QueryDriver;

public class Test1 {
	public static void main(String [] args){
		Producer p = new Producer();
		try {
			int size = 5000;
			long t1 = System.currentTimeMillis();
			p.readTupla(size,"input/NorthwindSalesData1.data");
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
			
			QueryDriver qd = new QueryDriver();
			qd.readAndParse("Categoria > \"5\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\"");
			t1 = System.currentTimeMillis();
			qd.filterQuery();
			t2 = System.currentTimeMillis();
			System.out.println("(FILTRAGEM)" + ((t2-t1)*1.0/1000) + "s");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

/*
 * exemplo de consulta restritiva:
 * 	Categoria > "5" and Pais startsWith "B" and Produto endsWith "s" and Cidade startsWith "Rio"
 * 
 * */
