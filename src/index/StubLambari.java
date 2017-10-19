package index;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import implementations.collections.JCLHashMap;
import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

public class StubLambari {
	public void stubLambari(){
	JCL_facade jcl = JCL_FacadeImpl.getInstanceLambari();
		
		int localCores = Runtime.getRuntime().availableProcessors();
		Object [][] args = new Object[localCores][1];
		for(int i=0; i<localCores; i++){
			Object [] a = {i};
			args[i][0] = a;
		}
		
		System.out.println("chegou stub");
		
//		File f1 = new File("lib/jclindex.jar");
//		File [] jars = {f1};
//		
		System.out.println(jcl.register(StubLambari.class, "StubLambari"));
		
		jcl.getAllResultBlocking(jcl.executeAllCores("StubLambari", "createIndex", args));
	}
	
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
}
