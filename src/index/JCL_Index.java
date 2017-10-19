package index;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import implementations.collections.JCLHashMap;
import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

public class JCL_Index {

	public void stubLambari(){
		JCL_facade jcl = JCL_FacadeImpl.getInstanceLambari();
		
		int localCores = Runtime.getRuntime().availableProcessors();
		Object [][] args = new Object[localCores][1];
		for(int i=0; i<localCores; i++){
			Object [] a = {i};
			args[i][0] = a;
		}
		
		System.out.println("chegou stub");
		
		//System.out.println(jcl.register(StubLambari.class, "StubLambari"));
		
		jcl.getAllResultBlocking(jcl.executeAllCores("StubLambari", "createIndex", args));
	}
	// funcao do index
	public void createIndex(int fileID) throws IOException
	{
		System.out.println("criando indice apartir do arquivo " + fileID);
		// map dos metadados
		Map<String, Integer> mesureMeta = JCL_FacadeImpl.GetHashMap("Mesure");
		Map<String, Integer> dimensionMeta = JCL_FacadeImpl.GetHashMap("Dimesion");

		int qtdMesure = mesureMeta.size();

		String line = null;
		FileReader f = null;
		try
		{
			f = new FileReader("arq_"+fileID+".txt");
		}
		catch(FileNotFoundException e)
		{
			e.printStackTrace();
		}

		// map dos indices invertidos
		Object2ObjectOpenHashMap<String, IntCollection> invertedIndex = new Object2ObjectOpenHashMap<String, IntCollection>();
		// map do jcl para dar putAll
		Map<String, IntCollection> jclInvertedIndex = new JCLHashMap<String, IntCollection>("invertedIndex_"+fileID);

		// map dos mesure index
		Object2ObjectOpenHashMap<String, Int2DoubleOpenHashMap> mesureIndex = new Object2ObjectOpenHashMap<String, Int2DoubleOpenHashMap>(); 
		// map do jcl para dar putAll
		Map<String, Int2DoubleOpenHashMap> jclMesureIndex = new JCLHashMap<String, Int2DoubleOpenHashMap>("mesureIndex_"+fileID); 

		BufferedReader reader = new BufferedReader(f);
		reader.readLine();
		// le linha a linha do arquivo
		while((line = reader.readLine()) != null)
		{
			// separa a tupla
			String [] splitArr = line.split("\\|");

			// valor de onde a coluna das dimensions comeca. Aqui pegamos a quantidade de colunas de mesure + 1(coluna das PK)
			int col = qtdMesure + 1;
			// rodamos a quantidade de colunas das dimensions
			for(int i=0;i<dimensionMeta.size();i++) {
				// pega o conteudo da coluna
				String d = splitArr[col++];
				// verifica se este conteudo ja existe na minha map(no caso, o conteudo e' a chave da map)
				if(invertedIndex.get(d) == null) {
					// cria uma lista que guarda as PK onde houve a ocorrencia de d(chave da map)
					IntArrayList dimensionList = new IntArrayList();
					// guarda na map de indice invertido a chave d e a lista vazia
					invertedIndex.put(d, dimensionList);
				}
				// pesquisa pela chave d, caso exista, adiciono mais um PK a lista
				invertedIndex.get(d).add(Integer.parseInt(splitArr[0]));
			}

			// rodamos a quantidade de colunas da mesure
			for(int i = 1; i <= qtdMesure; i++)
			{
				// pega o conteudo da coluna
				String m = splitArr[i];	
				// verifica se a map de mesures possui a chave (que nesse caso e' nossa PK)
				if(mesureIndex.get(splitArr[0]) == null)
				{
					// cria uma map int para double
					Int2DoubleOpenHashMap mesureMap = new Int2DoubleOpenHashMap();
					// adiciona a map das mesures a chave (PK) e a map vazia que acabou de ser criada
					mesureIndex.put(splitArr[0], mesureMap);
				}
				// Pesquisa pela chave PK, caso exista, adiciona os valores para a map int2double e adiciona ela para a map principal
				mesureIndex.get(splitArr[0]).put(i, Double.parseDouble(m));			
			}
		}
		jclInvertedIndex.putAll(invertedIndex);
		jclMesureIndex.putAll(mesureIndex);
	}

	// Escreve os metadados em todas as maquinas do cluster
	public static void writeMetaData(String mesure, String dimensions) throws IOException
	{
		FileWriter f1 = null;
		FileWriter f2 = null;
		try
		{
			f1 = new FileWriter("mesurenames.mg");
			f2 = new FileWriter("dimensionsnames.mg");
		}
		catch(FileNotFoundException e)
		{
			e.printStackTrace();
		}
		BufferedWriter writer1 = new BufferedWriter(f1);
		BufferedWriter writer2 = new BufferedWriter(f2);
		writer1.write(mesure);
		writer2.write(dimensions);
		writer1.close();
		f1.close();
		writer2.close();
		f2.close();
	}

	// Le os metadados que estão na maquina especifica
	public static void readMetaData() throws IOException
	{
		String line = null;
		FileReader f1 = null;
		FileReader f2 = null;
		try
		{
			f1 = new FileReader("mesurenames.mg");
			f2 = new FileReader("dimensionsnames.mg");
		}
		catch(FileNotFoundException e)
		{
			e.printStackTrace();
		}
		BufferedReader reader1 = new BufferedReader(f1);
		BufferedReader reader2 = new BufferedReader(f2);
		Map<String, Integer> mesure = new JCLHashMap<>("Mesure");
		Map<String, Integer> dimension = new JCLHashMap<>("Dimension");
		while((line = reader1.readLine()) != null)
		{
			String [] lineArr = line.split("=");
			mesure.put((lineArr[1]), Integer.parseInt(lineArr[0]));
		}
		while((line = reader2.readLine()) != null)
		{
			String [] lineArr = line.split("=");
			dimension.put((lineArr[1]), Integer.parseInt(lineArr[0]));
		}
		f1.close();
		f2.close();
		
		System.out.println(mesure.size() + " - " + dimension.size());
		System.out.println("Mesure Map:\n " + mesure.toString());
		System.out.println("Dimension Map:\n " + dimension.toString());
	}

}
