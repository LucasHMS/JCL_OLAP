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
			bases.add("input/NorthwindSalesData.data");
			bases.add("input/NorthwindSalesData2.data");
			bases.add("input/SaberFast/SaberFast.data");
			
			List<String> metadataSubPath = new ArrayList<>();
			metadataSubPath.add("input/");
			metadataSubPath.add("input/");
			metadataSubPath.add("input/SaberFast/");
			
			List<String> consultas = new ArrayList<>();
			consultas.add("consulta1");
			consultas.add("consulta2");
			consultas.add("consulta3");
			
			int cont1 = 0;
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
				i.loadMetadata(metadataSubPath.get(cont1++));
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

/*
 * 					Tamanho das Bases
 * 
 * ___BASE___|___TAMANHO___|__DIMENSÃO__|__MEASURES__|
 * 	   G	 |	 1055945   |	 8		|	  2	     |
 * 	   M	 |   525328	   |	 11		|	  5	     |
 *     P	 |	 150851	   |	 8		|	  2	     |
 *_____________________________________________________________
 * 
 * 		Quantidade de colunos por restritividade em cada base
 * 
 * 	RESTRITIVIDADE \ BASE |___1___|___2___|___3___|
 * 		Alta			  |	  6	  |	  8	  |   6	  |	
 * 		Media			  |	  4	  |	  6	  |	  4	  |
 * 		Baixa			  |	  2	  |	  3	  |	  2	  |
 *_____________________________________________________________
 * 
 * 						Consultas
 * 
 * - Base G.
 * 	- Rest A: "Categoria > \"5\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\" and Cliente startsWith \"HANAR\" and CEP startsWith \"05454\"; max PrecoUnitario;"
 * 	- Rest M: "Categoria > \"5\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\"; max PrecoUnitario;"
 * 	- Rest B: "Pais startsWith \"Br\" and Cidade startsWith \"Rio\"; max PrecoUnitario;"
 * 
 * - Base M.
 * 	- Rest A: 
 * 	- Rest M:
 * 	- Rest B: "Diretoria endsWith \"IZ\" and Colaborador startsWith \"RA\" and Cargo startsWith \"AUX\"; max total_score;" 
 * 
 * - Base P.
 * 	- Rest A: "Categoria > \"5\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\" and Cliente startsWith \"HANAR\" and CEP startsWith \"05454\"; max PrecoUnitario;"
 * 	- Rest M: "Categoria > \"5\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\"; max PrecoUnitario;"
 * 	- Rest B: "Pais startsWith \"Br\" and Cidade startsWith \"Rio\"; max PrecoUnitario;"
 * 
*/














