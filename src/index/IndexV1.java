package index;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import implementations.collections.JCLHashMap;

public class IndexV1{
	/*inverted index
	 mesure index
	base cuboid

	cade para cada core disponivel, criar-se um JCL_HashMap para cada index necessario. Cada core terá, 
	portanto 3 Maps indetificadas unicamente por seu nome e representando cada um dos indices citados*/

	private List<String[]> tuplas;
	private Map<Integer, String> dimensionNames;
	private Map<Integer, String> mesureNames;
	private Map<String,String> dimensionTypes;
	private Map<String, String> header;

	int coreCount;
	int partitionSize;

	public void setFields(List<String[]> tuplas, Map<Integer, String> dimensionNames, Map<Integer, String> mesureNames,
							Map<String,String> diemensionTypes, Map<String, String> header){
		this.tuplas = tuplas;
		this.dimensionNames = new JCLHashMap<Integer, String>("dn");
		this.mesureNames = new JCLHashMap<Integer, String>("mn");
		this.dimensionTypes = new JCLHashMap<String, String>("dt");
		this.header = new HashMap<String, String>();

		this.dimensionNames = dimensionNames;
		this.mesureNames = mesureNames;
		this.dimensionTypes = diemensionTypes;
		this.header = header;
		
		System.out.println("set index");
	}

	public void invertedIndex(int coreID){

	}

	public void mesureIndex(int coreID, int start, int end){
		Map<Integer,List<String>> mesureIndex = new JCLHashMap<Integer, List<String>>("mesureIndexT"+coreID);
		System.out.println("Computing Mesure Index for core: " + coreID);
		
		for(int j=start;j<end;j++){
			List<String> values = new ArrayList<String>();
			for(Entry<Integer, String> s : mesureNames.entrySet()){			
				values.add(tuplas.get(j)[Integer.parseInt(header.get(s.getValue()))]);
			}
			mesureIndex.put(j, values);
			values.clear();
			values = null;
		}
		System.out.println("Mesure Index Computed for Core: " + coreID);
	}

	public void baseCuboid(int coreID){

	}

}
