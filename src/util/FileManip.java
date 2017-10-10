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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import implementations.collections.JCLHashMap;

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
