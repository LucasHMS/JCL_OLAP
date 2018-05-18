package cube;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import java.util.StringJoiner;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

@SuppressWarnings("unchecked")
public class Cube {
	private JCL_facade jcl;

	public Cube() {
		jcl = JCL_FacadeImpl.getInstance();
	}

	public void createCube(int machineID, int coreID, int nDimensions) {
        Object2ObjectMap<String, IntCollection> filtResult = (Object2ObjectMap<String, IntCollection>) jcl.getValue(machineID+"_filter_core_"+coreID).getCorrectResult();
        Object2ObjectMap<String, IntCollection> aux = new Object2ObjectOpenHashMap<>();
		for(int i=0; i<nDimensions; i++) {
			for(Entry<String,IntCollection> e : filtResult.entrySet()){
				String newTuple = arrangeTuple(e.getKey(), i);
				if(!aux.containsKey(newTuple)) {
					aux.put(newTuple, new IntArrayList());
				}
				aux.get(newTuple).addAll(e.getValue());
			}
			filtResult.putAll(aux);
			aux.clear();
		}
		
		jcl.setValueUnlocking(machineID+"_filter_core_"+coreID, filtResult);
	}
	
	public void createCube(int machineID, int coreID) {
		System.out.println("CRIANDO CUBO PARA " + machineID + " - " + coreID);
		Object2ObjectMap<String, IntCollection> filtResult = (Object2ObjectMap<String, IntCollection>) jcl.getValue(machineID+"_filter_core_"+coreID).getCorrectResult();
		Object2ObjectMap<String, IntCollection> aux = new Object2ObjectOpenHashMap<>();
		for(Entry<String,IntCollection> e : filtResult.entrySet()) {
			String tuple = e.getKey();
			String [] values = tuple.split("\\|");
			List<String> combinations = arrCombination(values);
			
			for(String s: combinations) {
				if(aux.containsKey(s)) continue;
				
				aux.put(s, new IntArrayList());
				for(Entry<String,IntCollection> fr : filtResult.entrySet()) {
					if(checkContent(fr.getKey(),s)) {
						aux.get(s).addAll(fr.getValue());
					}
				}
			}
		}
		
		filtResult.putAll(aux);
		jcl.setValueUnlocking(machineID+"_filter_core_"+coreID, filtResult);
	}
	
	public boolean checkContent(String target, String subStrings) {
		String [] splitedStrs = subStrings.split("\\|");
		int count = 0; 
		for(int i=0; i<splitedStrs.length; i++) {
			if(target.contains(splitedStrs[i])) {
				count++;
			}
		}
		if (count == splitedStrs.length) return true;
				
		return false;
	}

	public String arrangeTuple(String tuple, int dimension) {
		String [] splitedStrs = tuple.split("\\|");
		splitedStrs[dimension] = "ALL";
		StringJoiner joiner = new StringJoiner("|");
		for(int i=0; i<splitedStrs.length;i++) {
//			if(i == dimension) continue;
			joiner.add(splitedStrs[i]);
		}

		return joiner.toString();
	}
	
	public void combinationUtil(String arr[], String [] data, int start, int end, int index, int r, List<String> combinations){
		if (index == r){
			StringJoiner joiner = new StringJoiner("|");
			for (int j=0; j<r; j++) {
				joiner.add(data[j]);
			}
			combinations.add(joiner.toString());
			return;
		}

		for (int i=start; i<=end && end-i+1 >= r-index; i++){
			data[index] = arr[i];
			combinationUtil(arr, data, i+1, end, index+1, r, combinations);
		}
	}

	public List<String> arrCombination(String arr[]){
		List<String> combinations = new ArrayList<>();
		for(int i=1;i<=arr.length;i++) {
			String [] data = new String[i];
			combinationUtil(arr, data, 0, arr.length-1, 0, i, combinations);
		}
		return combinations;
	}


    public Object2ObjectMap<String, DoubleCollection> getMeasureValues(int machineID, int coreID, List<Integer> aggregationColumns){
        Object2ObjectMap<String, IntCollection> filtResult = (Object2ObjectMap<String, IntCollection>) jcl.getValue(machineID+"_filter_core_"+coreID).getCorrectResult();
        Object2ObjectMap<String, Int2DoubleOpenHashMap> measureIndex = (Object2ObjectMap<String, Int2DoubleOpenHashMap>) jcl.getValue(machineID+"_mesureIndex_"+coreID).getCorrectResult();
        Object2ObjectMap<String, DoubleCollection> aggregationValues = new Object2ObjectOpenHashMap<>(); 
        
        for(Entry<String, IntCollection> e : filtResult.entrySet()) {
            for(Integer i : e.getValue()) {
                if(!aggregationValues.containsKey(e.getKey())) {
                    DoubleCollection values = new DoubleArrayList();
                    aggregationValues.put(e.getKey(), values);
                }
                double value = measureIndex.get(i.toString()).get(aggregationColumns.get(0).intValue());
                aggregationValues.get(e.getKey()).add(value);
            }
        }
                
        return aggregationValues;
    }
	
	/*public static void main (String[] args) {
		Cube c = new Cube();
		String [] values = {"casa","arvore","animal","lancha"};
		List<String> combinations = c.arrCombination(values);

		for(String s : combinations) {
			System.out.println(s);
		}	
	}*/
}
