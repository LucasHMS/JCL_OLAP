package test;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import data.distribution.BalancedProducer;
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
			Producer p = new BalancedProducer(size, false);

			List<String> bases = new ArrayList<>();
			bases.add("input/NorthwindSalesData.data");
			bases.add("input/SaberFast/SaberFast.data");
			bases.add("input/NorthwindSalesData2.data");
			
			List<String> metadataSubPath = new ArrayList<>();
			metadataSubPath.add("input/");
			metadataSubPath.add("input/SaberFast/");
			metadataSubPath.add("input/");
			
			List<List<String>> consultas = new ArrayList<>();
			for(int j = 0; j < 3; j++)
			{
				List<String> l = new ArrayList<>();
				consultas.add(l);	
			}
			
		// Consulta arquivo Pequeno
			consultas.get(0).add("Categoria > \"7\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\" and Cliente startsWith \"HANAR\" and CEP startsWith \"05454\"; max PrecoUnitario;");
			consultas.get(0).add("Categoria > \"5\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\"; max PrecoUnitario;");
			consultas.get(0).add("Pais startsWith \"Br\" and Cidade startsWith \"Rio\"; max PrecoUnitario;");
		// Consulta arquivo Medio
			consultas.get(1).add("Diretoria startsWith \"VICTOR\" and Colaborador endsWith \"ALMEIDA\" and Cargo startsWith \"CAIXA\" and name startsWith \"ROBERTA\" and Loja startsWith \"GUARULHOS\" and Regional startsWith \"SPIII\" and area endsWith \"ja\" and Cod_Record = \"250907\"; max total_score;");
			consultas.get(1).add("Diretoria startsWith \"VICTOR\" and Colaborador endsWith \"ALMEIDA\" and Cargo startsWith \"CAIXA\" and name startsWith \"ROBERTA\" and Loja startsWith \"GUARULHOS\" and Regional startsWith \"SPIII\"; max total_score;");
			consultas.get(1).add("Diretoria endsWith \"IZ\" and Colaborador startsWith \"RA\" and Cargo startsWith \"AUX\"; max total_score;");
		// Consulta arquivo Grande
			consultas.get(2).add("Categoria > \"7\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\" and Cliente startsWith \"HANAR\" and CEP startsWith \"05454\"; max PrecoUnitario;");
			consultas.get(2).add("Categoria > \"5\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\"; max PrecoUnitario;");
			consultas.get(2).add("Pais startsWith \"Br\" and Cidade startsWith \"Rio\"; max PrecoUnitario;");
			
			
			int cont1 = 0;
			for(String b : bases) {
				StringJoiner textoArquivo = new StringJoiner("\n");
				textoArquivo.add("\nTestes para base: " + b);
				
			// distribui base b
				t1 = System.currentTimeMillis();
				p.distributeDataBase(b);
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
				
				s = "(CRIAR INDICES) " + ((t2-t1)*1.0/1000);
				System.out.println(s);
				textoArquivo.add(s); t1=t2=0;
				
				int cont = 0;
				for(List<String> c : consultas)
				{
					for(String str : c)
					{
						textoArquivo.add("Consulta: " + cont++);
						
					// realiza a filtragem com a consulta c
						qd.readAndParse(str); 
						t1 = System.currentTimeMillis();
						qd.filterQuery();
						t2 = System.currentTimeMillis();
						
						s = "(FILTRAGEM) " + ((t2-t1)*1.0/1000);
						System.out.println(s);
						textoArquivo.add(s); t1=t2=0;
						
					// cria o cubo
						t1 = System.currentTimeMillis();
						qd.createCube();
						t2 = System.currentTimeMillis();
						
						s = "(CRIAR CUBO) " + ((t2-t1)*1.0/1000); 
						System.out.println(s);
						textoArquivo.add(s); t1=t2=0;
						
					// cacula as agregações
						t1 = System.currentTimeMillis();
						qd.aggregateCubes();
						t2 = System.currentTimeMillis();
						
						s = "(AGREGAÇÃO)" + ((t2-t1)*1.0/1000); 
						System.out.println(s);
						textoArquivo.add(s); t1=t2=0;
						
					// limpa reultado da filtragem
						qd.deleteFilterResult();
					}
					file.println(textoArquivo.toString());
	
				// limpa base distribuida
					p.deleteDistributedBase();
				// limpa indices		
//					i.deleteIndices();
				}
			}
			file.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

/*
 * 					Tamanho das Bases
 * 
 * ___BASE___|___TAMANHO___|__DIMENSÃO__|__MEASURES__|
 * 	   G	 |	 1064770   |	 8		|	  2	     |
 * 	   M	 |   520010	   |	 11		|	  5	     |
 *     P	 |	 152110	   |	 8		|	  2	     |
 *_____________________________________________________________
 * 
 * 		Quantidade de colunos por restritividade em cada base
 * 
 * 	RESTRITIVIDADE \ BASE |___G___|___M___|___P___|
 * 		Alta			  |	  6	  |	  8	  |   6	  |	
 * 		Media			  |	  4	  |	  6	  |	  4	  |
 * 		Baixa			  |	  2	  |	  3	  |	  2	  |
 *_____________________________________________________________
 * 
 * 						Consultas
 * 
 * - Base G.
 * 	- Rest A: "Categoria > \"7\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\" and Cliente startsWith \"HANAR\" and CEP startsWith \"05454\"; max PrecoUnitario;"
 * 	- Rest M: "Categoria > \"5\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\"; max PrecoUnitario;"
 * 	- Rest B: "Pais startsWith \"Br\" and Cidade startsWith \"Rio\"; max PrecoUnitario;"
 * 
 * - Base M.
 * 	- Rest A: "Diretoria startsWith \"VICTOR\" and Colaborador endsWith \"ALMEIDA\" and Cargo startsWith \"CAIXA\" and name startsWith \"ROBERTA\" and Loja startsWith \"GUARULHOS\" and Regional startsWith \"SPIII\" and area endsWith \"ja\" and Cod_Record = \"250907\"; max total_score;"
 * 	- Rest M: "Diretoria startsWith \"VICTOR\" and Colaborador endsWith \"ALMEIDA\" and Cargo startsWith \"CAIXA\" and name startsWith \"ROBERTA\" and Loja startsWith \"GUARULHOS\" and Regional startsWith \"SPIII\"; max total_score;"
 * 	- Rest B: "Diretoria endsWith \"IZ\" and Colaborador startsWith \"RA\" and Cargo startsWith \"AUX\"; max total_score;" 
 * 
 * - Base P.
 * 	- Rest A: "Categoria > \"5\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\" and Cliente startsWith \"HANAR\" and CEP startsWith \"05454\"; max PrecoUnitario;"
 * 	- Rest M: "Categoria > \"5\" and Pais startsWith \"B\" and Produto endsWith \"s\" and Cidade startsWith \"Rio\"; max PrecoUnitario;"
 * 	- Rest B: "Pais startsWith \"Br\" and Cidade startsWith \"Rio\"; max PrecoUnitario;"
 * 
*/














