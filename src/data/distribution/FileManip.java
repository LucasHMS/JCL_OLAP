package data.distribution;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class FileManip {
	private String tuplasF;
	private String dimensioNamesF;
	private String mesureNamesF;
	private String dimensionTypesF; 

	public FileManip(String tuplasF, String dimensioNamesF, String mesureNamesF, String dimensionTypesF){
		this.tuplasF = tuplasF;
		this.dimensioNamesF = dimensioNamesF;
		this.mesureNamesF = mesureNamesF;
		this.dimensionTypesF = dimensionTypesF;
	}
	
	public Map<String, String> header() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(tuplasF));;
		String line = br.readLine();

		String [] fline = line.split("\\|");
		
		Map<String, String> header = new HashMap<String, String>();
		header.put(fline[0].substring(1),0+"");
		for(int i=1;i<fline.length;i++){
			String key = fline[i];
			header.put(key, i+"");
		}
				
		br.close();
		return header;
	}
	
	public List<String []> readTuplas() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(tuplasF));;
		String line = br.readLine();
	
		String [] fline = line.split("\\|");
		Map<String, Integer> mesureNames = new HashMap<String, Integer>();
		for(int i=0;i<fline.length;i++){
			mesureNames.put(fline[i], i);
		}
		
		String [] values = null;
		List<String[]> tuplas = new ArrayList<String []>(); 
		while ((line = br.readLine()) != null) {
			values = line.split("\\|");
			tuplas.add(values);
		}
		
		br.close();
		return tuplas;	
	}
	
	public Map<Integer, String> readDimensionNames() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(dimensioNamesF));;
		String line;
		
		String [] values = null;
		Map<Integer, String> dimensionNames = new HashMap<Integer, String>(); 
		while ((line = br.readLine()) != null) {
			values = line.split("\\=");
			dimensionNames.put(Integer.parseInt(values[0]), values[1]);
			//System.out.println(values[0]+" - "+values[1]);
		}
		
		br.close();
		return dimensionNames;	
	}
	
	public Map<Integer, String> readMesureNames() throws IOException{ 
		BufferedReader br = new BufferedReader(new FileReader(mesureNamesF));;
		String line;
		
		String [] values = null;
		Map<Integer, String> mesureNames = new HashMap<Integer, String>(); 
		while ((line = br.readLine()) != null) {
			values = line.split("\\=");
			mesureNames.put(Integer.parseInt(values[0]), values[1]);
		}
		/*for(Entry<Integer, String> s : mesureNames.entrySet()){
			System.out.println("chave " +s.getKey());
			System.out.println("valor "+ s.getValue());
			
		}*/
		br.close();
		return mesureNames;
	}
	
	public Map<String,String> readDimensionTypes() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(dimensionTypesF));;
		String line;
		
		String [] values = null;
		Map<String,String> dimensionTypes = new HashMap<String, String>(); 
		while ((line = br.readLine()) != null) {
			values = line.split("\\=");
			dimensionTypes.put(values[0], values[1]);
			//System.out.println(values[0]+" - "+values[1]);
		}
		
		br.close();
		return dimensionTypes;
	}
	
	public static void main(String [] args){
		FileManip fm;
		try {
			fm = new FileManip("input/NorthwindSalesData.data", "input/dimensionsnames.mg",
								"input/measuresnames.mg", "input/dimensionstypes.mg");
			fm.readTuplas();
			fm.readDimensionNames();
			fm.readDimensionTypes();
			fm.readMesureNames();
			fm.header();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
