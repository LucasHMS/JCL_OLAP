package test;

import java.io.IOException;
import java.util.Map;

import data.distribution.Producer;
import implementations.collections.JCLHashMap;
import index.Index;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntCollection;
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
			
			/*System.out.println("Inverted Index");
			Map<String, IntCollection> jclInvertedIndex = new JCLHashMap<String, IntCollection>("invertedIndex_"+0);
			for(Map.Entry<String, IntCollection> e : jclInvertedIndex.entrySet()) {
				System.out.println(e.getKey() + ": " + e.getValue());
			}
			
			System.out.println("Mesure Index");
			Map<String, Int2DoubleOpenHashMap> jclMesureIndex = new JCLHashMap<String, Int2DoubleOpenHashMap>("mesureIndex_"+1); 
			for(Map.Entry<String, Int2DoubleOpenHashMap> e : jclMesureIndex.entrySet()) {
				System.out.println(e.getKey() + ": " + e.getValue());
			}*/
			
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
