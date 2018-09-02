package index;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import implementations.collections.JCLHashMap;
import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class JCL_IntegerBaseIndex {
		// funcao do index
		@SuppressWarnings("unchecked")
		public void createIndex(int machineID, int coreID) {
			System.out.println("***** criando indices apartir de maps ******");
			JCL_facade jcl = JCL_FacadeImpl.getInstance();

			// map dos metadados
//			Map<String, Integer> mesureMeta = JCL_FacadeImpl.GetHashMap("Mesure");
			Map<String, Integer> dimensionMeta = JCL_FacadeImpl.GetHashMap("Dimension");	
			
			Int2ObjectMap<IntList> map_core = (Int2ObjectMap<IntList>) jcl.getValue(machineID+"_core_"+coreID).getCorrectResult();
			System.out.println("tamanho da map do core "+coreID+": "+map_core.size());

			// map dos mesure index
			Int2ObjectMap<Int2FloatOpenHashMap> mesureIndex = new Int2ObjectOpenHashMap<Int2FloatOpenHashMap>(); 

			// map dos indices invertidos
			int nDimensions = dimensionMeta.size();
			List<Int2ObjectMap<IntList>> invertedIndexes = new ArrayList<>();
			for(int i=0; i<nDimensions; i++) invertedIndexes.add(new Int2ObjectOpenHashMap<>());
			
			System.out.println("***** iniciou a criação dos indices ******");

			for(Int2ObjectMap.Entry<IntList> e : map_core.int2ObjectEntrySet()){
				int pk = e.getIntKey();
				IntList tupleValues = e.getValue();

				// rodamos a quantidade de colunas das dimensions ## maior valor 9999 com 17 dimensoes
				for(int i=0;i<nDimensions;i++) {
					// pega o conteudo da coluna
					int value = tupleValues.get(i);
					// verifica se este conteudo ja existe na minha map(no caso, o conteudo e' a chave da map)
					if(!invertedIndexes.get(i).containsKey(value)) {
						// guarda na map de indice invertido a chave d e a lista vazia
						invertedIndexes.get(i).put(value, new IntArrayList());
					}
					// pesquisa pela chave d, caso exista, adiciono mais um PK a lista
					invertedIndexes.get(i).get(value).add(pk);
				}
				
				// rodamos a quantidade de colunas da mesure
				for(int i=nDimensions; i<tupleValues.size(); i++){
					// pega o conteudo da coluna
					int value = tupleValues.get(i);	
					// verifica se a map de mesures possui a chave (que nesse caso e' nossa PK)
					if(!mesureIndex.containsKey(pk)){
						// adiciona a map das mesures a chave (PK) e a map vazia que acabou de ser criada
						mesureIndex.put(pk, new Int2FloatOpenHashMap());
					}
					// Pesquisa pela chave PK, caso exista, adiciona os valores para a map int2double e adiciona ela para a map principal
					Int2FloatOpenHashMap aux = mesureIndex.get(pk);
					aux.put(i-nDimensions, value);
					mesureIndex.put(pk,aux);			
				}
			}
			
			for(int i=0; i<nDimensions; i++)
				jcl.instantiateGlobalVar(machineID+"_invertedIndex_"+i+"_"+coreID, invertedIndexes.get(i));
			jcl.instantiateGlobalVar(machineID+"_mesureIndex_"+coreID, mesureIndex);

			System.out.println("***** finalizou a criação dos indices ******");
		}

		//Escreve os metadados em todas as maquinas do cluster

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
		}

		public void teste() {
			System.out.println();
		}
}
