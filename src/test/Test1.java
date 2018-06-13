package test;

import java.io.IOException;

import data.distribution.BalancedProducer;
import data.distribution.InvariantProducer;
import data.distribution.Producer;
import index.Index;
import query.QueryDriver;

public class Test1 {
	public static void main(String [] args){
		
		try {
			int size = 5000;
			boolean saveToFile = false;
			Producer p = new BalancedProducer(size,saveToFile);
			long t1 = System.currentTimeMillis();
			p.distributeDataBase("input//SaberFast/SaberFast.data");
			long t2 = System.currentTimeMillis();
			System.out.println("(CRIAR ARQUIVOS "+ saveToFile +") Tempo gasto com " + size + ": " + ((t2-t1)*1.0/1000) + "s");
			
			Index i = new Index();
			t1 = System.currentTimeMillis();
			i.loadMetadata("input//SaberFast/");
			t2 = System.currentTimeMillis();
			System.out.println("(CARREGAR METADADOS): " + ((t2-t1)*1.0/1000) + "s");
			
			t1 = System.currentTimeMillis();
			i.createIndex("Map");
			t2 = System.currentTimeMillis();
			System.out.println("(CRIAR INDICES): " + ((t2-t1)*1.0/1000) + "s");
			
			QueryDriver qd = new QueryDriver();
			qd.readAndParse("Cargo contains \"OR\" and Loja contains \"SHOP\" and username contains \"2\"; max total_score;");
			t1 = System.currentTimeMillis();
			qd.filterQuery();
			t2 = System.currentTimeMillis();
			System.out.println("(FILTRAGEM): " + ((t2-t1)*1.0/1000) + "s");
			
			t1 = System.currentTimeMillis();
			qd.createCube();
			t2 = System.currentTimeMillis();
			System.out.println("(CRIAR CUBO): " + ((t2-t1)*1.0/1000) + "s");
			
			QueryDriver.VERBOSITY = false;
			t1 = System.currentTimeMillis();
			qd.aggregateCubes();
			t2 = System.currentTimeMillis();
			System.out.println("(AGREGAÇÃO): " + ((t2-t1)*1.0/1000) + "s");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

/*
 * exemplos:
 * 	(Base 0, 1 ou 2) "Categoria > \"5\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\"; max PrecoUnitario;"
 * 	(Base 3)"Pais startsWith \"S\" and Cidade startsWith \"S\" and Empresa startsWith \"R\"; max PrecoUnitario;"
 * 
 * "Cargo contains \"OR\" and Loja contains \"SHOP\" and username contains \"2\"; max total_score;"
 * 
 * */
