package test;

import java.io.IOException;
import java.util.Map;

import data.distribution.Producer;
import implementations.collections.JCLHashMap;
import implementations.dm_kernel.user.JCL_FacadeImpl;
import index.Index;
import interfaces.kernel.JCL_facade;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
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
			
			System.out.println("Inverted Index");
			JCL_facade jcl = JCL_FacadeImpl.getInstance();
//			Map<String, IntCollection> jclInvertedIndex = new JCLHashMap<String, IntCollection>("invertedIndex_"+0);
			for(int x=0;x<4;x++) 
				System.out.println("CORE " + x +jcl.getValue(0+"_invertedIndex_"+x).getCorrectResult());
			
/*			Object2ObjectMap<String, IntCollection> jclInvertedIndex = (Object2ObjectOpenHashMap<String, IntCollection>) jcl.getValue("invertedIndex_"+0).getCorrectResult();
			for(Map.Entry<String, IntCollection> e : jclInvertedIndex.entrySet()) {
				System.out.println(e.getKey() + ": " + e.getValue());
			}*/
			
			System.out.println("Mesure Index");
//			Map<String, Int2DoubleOpenHashMap> jclMesureIndex = new JCLHashMap<String, Int2DoubleOpenHashMap>("mesureIndex_"+1); 
			for(int x=0;x<4;x++)
				System.out.println("CORE " + x +jcl.getValue(0+"_mesureIndex_"+x).getCorrectResult());
/*			Object2ObjectMap<String, Int2DoubleOpenHashMap> jclMesureIndex = (Object2ObjectOpenHashMap<String, Int2DoubleOpenHashMap>) jcl.getValue("mesureIndex_"+0).getCorrectResult();
			for(Map.Entry<String, Int2DoubleOpenHashMap> e : jclMesureIndex.entrySet()) {
				System.out.println(e.getKey() + ": " + e.getValue());
			}*/
			
			/*QueryDriver qd = new QueryDriver();
			qd.readAndParse("Categoria > \"5\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\"");
			t1 = System.currentTimeMillis();
			qd.filterQuery();
			t2 = System.currentTimeMillis();
			System.out.println("(FILTRAGEM)" + ((t2-t1)*1.0/1000) + "s");*/
			
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
