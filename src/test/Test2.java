package test;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import data.distribution.Producer;
import index.Index;
import query.QueryDriver;

public class Test2 {
	public static void main(String [] args){
		try {
			
			int size = 5000;
			long t1,t2;
			PrintWriter file = new PrintWriter("resultados.txt", "UTF-8");
			
			Index i = new Index();
			QueryDriver qd = new QueryDriver();
			Producer p = new Producer();

			List<String> bases = new ArrayList<>();
			bases.add("Base1");
			bases.add("Base2");
			bases.add("Base3");
			
			List<String> consultas = new ArrayList<>();
			consultas.add("consulta1");
			consultas.add("consulta2");
			consultas.add("consulta3");
			
			for(String b : bases) {
				StringJoiner textoArquivo = new StringJoiner("\n");
				textoArquivo.add("\nTestes para base: " + b);
				
				// distribui base b
				t1 = System.currentTimeMillis();
				p.readTupla(size,b);
				t2 = System.currentTimeMillis();
				
				String s = "(DISTRIBUIR A BASE com buffer = " + size + ") " + ((t2-t1)*1.0/1000);
				System.out.println(s);
				textoArquivo.add(s); t1=t2=0;

				// gera os indices
				t1 = System.currentTimeMillis();
				i.loadMetadata();
				t2 = System.currentTimeMillis();
				
				s = "(CARREGAR METADADOS) " + ((t2-t1)*1.0/1000);
				System.out.println(s);
				textoArquivo.add(s); t1=t2=0;
				
				t1 = System.currentTimeMillis();
				i.createIndex("Map");
				t2 = System.currentTimeMillis();
				
				s = "(CRIAR INDICES) " + ((t2-t1)*1.0/1000); t1=t2=0;
				System.out.println(s);
				textoArquivo.add(s); t1=t2=0;
				
				int cont = 0;
				for(String c : consultas) {
					textoArquivo.add("Consulta: " + cont++);
					
					// realiza a filtragem com a consulta c
					qd.readAndParse(c); 
					t1 = System.currentTimeMillis();
					qd.filterQuery();
					t2 = System.currentTimeMillis();
					
					s = "(FILTRAGEM) " + ((t2-t1)*1.0/1000); t1=t2=0;
					System.out.println(s);
					textoArquivo.add(s); t1=t2=0;
					
					// cria o cubo
					t1 = System.currentTimeMillis();
					qd.createCube();
					t2 = System.currentTimeMillis();
					
					s = "(CRIAR CUBO) " + ((t2-t1)*1.0/1000); t1=t2=0;
					System.out.println(s);
					textoArquivo.add(s); t1=t2=0;
					
					// cacula as agregações
					t1 = System.currentTimeMillis();
					qd.aggregateCubes();
					t2 = System.currentTimeMillis();
					
					s = "(AGREGAÇÃO)" + ((t2-t1)*1.0/1000); t1=t2=0;
					System.out.println(s);
					textoArquivo.add(s); t1=t2=0;
					
					// limpa reultado da filtragem
					qd.deleteFilterResult();
				}
				file.println(textoArquivo.toString());

				// limpa base distribuida
				p.deleteDistributedBase();
				// limpa indices		
				i.deleteIndices();
			}
			file.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
