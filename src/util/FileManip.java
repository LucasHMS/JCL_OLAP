package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Map.Entry;

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import implementations.collections.JCLHashMap;
import implementations.dm_kernel.user.JCL_FacadeImpl;

public class FileManip {

	public static void writeTuplesTxt(Map<Integer, String> tuples, int core_num) throws IOException {
		File f = new File("arq_"+core_num+".txt");
		FileWriter fw = new FileWriter(f,true);
		BufferedWriter bw = new BufferedWriter(fw);

		for(Entry<Integer,String> e : tuples.entrySet()) {
			String aux = e.getKey() + "|" + e.getValue();
			bw.write(aux);
			bw.newLine();
		}

		bw.close();
		fw.close();
	}

	public static void writeTuplesBin(Map<Integer, String> tuples, int core_num) throws IOException {
		File f = new File("arq_"+core_num+".bin");
		FileOutputStream fos = new FileOutputStream(f, true);
		ObjectOutputStream out = new ObjectOutputStream(fos);
		out.writeObject(tuples);

		out.close();
		fos.close();
	}

	public static void loadTuplesTxt(int machineID) throws IOException {
		int localCores = Runtime.getRuntime().availableProcessors();
		for(int i=0;i<localCores;i++) {
			File f = new File("arq_"+i+".txt");
			FileReader fr = null;
			try {
				fr = new FileReader(f);
			}catch(FileNotFoundException e) {
				continue;
			}
			BufferedReader reader = new BufferedReader(fr);

			String line = null;
			Int2ObjectMap<String> m = new Int2ObjectOpenHashMap<String>();
 			while ((line = reader.readLine()) != null) {
 				String [] lineArr = line.split("\\|",2);
 				m.put(Integer.parseInt(lineArr[0]), lineArr[1]);
			}			
 			Map<Integer, String> hm = new JCLHashMap<>(machineID+":"+i);
 			hm.putAll(m);
 			
			reader.close();
			fr.close();
		}
	}

	@SuppressWarnings("unchecked")
	public static void loadTuplesBin(int machineID) throws IOException, ClassNotFoundException {
		int localCores = Runtime.getRuntime().availableProcessors();
		for(int i=0;i<localCores;i++) {
			FileInputStream f = null;
			try {
				f = new FileInputStream("arq_"+i+".bin");
			}catch(FileNotFoundException e) {
				continue;
			}
			ObjectInputStream in = new ObjectInputStream(f);

			Int2ObjectMap<String> m = (Int2ObjectMap<String>) in.readObject();

			Map<Integer, String> hm = new JCLHashMap<>(machineID+":"+i);
			hm.putAll(m);

			in.close();
			f.close();
		}
	}
	
// funcao do index
	public static void readIndex(int machineID, int fileID) throws IOException
	{
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
		Map<String, IntCollection> jclInvertedIndex = new JCLHashMap<String, IntCollection>("invertedIndex");

		// map dos mesure index
		Object2ObjectOpenHashMap<String, Int2DoubleOpenHashMap> mesureIndex = new Object2ObjectOpenHashMap<String, Int2DoubleOpenHashMap>(); 
		// map do jcl para dar putAll
		Map<String, Int2DoubleOpenHashMap> jclMesureIndex = new JCLHashMap<String, Int2DoubleOpenHashMap>("mesureIndex"); 
		
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
		f1.close();
		f2.close();
	}
	
// le o arquivo metaData pela primeira vez
	public static String metaDataToString(String nome) throws IOException
	{
		String line = null;
		FileReader f = null;
		try
		{
			f = new FileReader(nome);
		}
		catch(FileNotFoundException e)
		{
			e.printStackTrace();
		}
		StringBuffer b = new StringBuffer();
		BufferedReader reader = new BufferedReader(f);
		while((line = reader.readLine()) != null)
		{
			b.append(line+'\n');
		}
		f.close();
		return b.toString();
		
	}
	
}
