package cube;

import java.util.List;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatCollection;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

public class IntegerBaseCube {
	private JCL_facade jcl;

	public IntegerBaseCube() {
		jcl = JCL_FacadeImpl.getInstance();
	}

	@SuppressWarnings("unchecked")
	public void createCube(int machineID, int coreID, int nDimensions) {
		Object2ObjectMap<IntList, IntList> filterResults = (Object2ObjectMap<IntList, IntList>) 
															jcl.getValue(machineID+"_filter_core_"+coreID).getCorrectResult();
        Object2ObjectMap<IntList, IntList> aux = new Object2ObjectOpenHashMap<>();
		
        for(int i=0; i<nDimensions; i++) {
			for(Object2ObjectMap.Entry<IntList, IntList> e : filterResults.object2ObjectEntrySet()){
				IntList newTuple = arrangeTuple(e.getKey(), i);
				if(!aux.containsKey(newTuple)) {
					aux.put(newTuple, new IntArrayList());
				}
				aux.get(newTuple).addAll(e.getValue());
			}
			filterResults.putAll(aux);
			aux.clear();
		}
		
		jcl.setValueUnlocking(machineID+"_filter_core_"+coreID, filterResults);
	}

	public IntList arrangeTuple(IntList tuple, int dimension) {
		int [] tupleArr = tuple.toIntArray();
		tupleArr[dimension] = -1;

		return new IntArrayList(tupleArr);
	}
	
    @SuppressWarnings("unchecked")
	public Object2ObjectMap<IntList, FloatCollection> getMeasureValues(int machineID, int coreID, List<Integer> aggregationColumns){
        Object2ObjectMap<IntList, IntList> filterResults = (Object2ObjectMap<IntList, IntList>) 
        													jcl.getValue(machineID+"_filter_core_"+coreID).getCorrectResult();
        Int2ObjectMap<Int2FloatOpenHashMap> measureIndex = (Int2ObjectMap<Int2FloatOpenHashMap>) 
        													jcl.getValue(machineID+"_mesureIndex_"+coreID).getCorrectResult();
        Object2ObjectMap<IntList, FloatCollection> aggregationValues = new Object2ObjectOpenHashMap<>(); 
        
        for(Object2ObjectMap.Entry<IntList, IntList> e : filterResults.object2ObjectEntrySet()) {
            for(int i : e.getValue()) {
                if(!aggregationValues.containsKey(e.getKey())) {
                    FloatCollection values = new FloatArrayList();
                    aggregationValues.put(e.getKey(), values);
                }
                float value = measureIndex.get(i).get(aggregationColumns.get(0).intValue());
                aggregationValues.get(e.getKey()).add(value);
            }
        }
                
        return aggregationValues;
    }
}
