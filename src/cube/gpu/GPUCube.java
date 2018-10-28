package cube.gpu;

import java.util.ArrayList;
import java.util.List;

import com.aparapi.Range;
import com.aparapi.device.Device;
import com.aparapi.internal.opencl.*;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatCollection;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap.Entry;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;


/* *
 * TODO
 * 1 - metodo que faz a redução da filtragem em um unico recurso
 * 2 - tranformar a coleção original com as tuplas filtradas para o formato de array continuo para GPU (pode melhorar)
 * 3 - sobre o array continuo, gerar as agregaçoes
 * 4 - retornar o array com as agregações para CPU
 * 5 - com base na quantidade de tuplas que é gerada para cada tupla original, gerar as intercessoes do cubo remapeando as TIDs
 * 6 - metodo que devolve os measure values associados a cada tupla gerada da colecao final
 *  
 * */

public class GPUCube {

	private JCL_facade jcl;
	Object2ObjectMap<IntList, FloatCollection> resource;
    int [] filterResultArr;;


	public GPUCube() {
		jcl = JCL_FacadeImpl.getInstance();
		resource = new Object2ObjectOpenHashMap<>();
	}
	
	public void createCube(int machineID, int nDimensions, int measureCol) {
		
		aggregateFilterResults(machineID, measureCol);
		
		filterResult2Arr(nDimensions);
		
		int LINES = resource.size();
	    int COLS = nDimensions;
	    
	    CubeKernel kernel = new CubeKernel(LINES, COLS, filterResultArr);
        Device device = Device.bestGPU();
        Range range = device.createRange(LINES);
        kernel.execute(range);
        
        filterResultArr = null;
        filterResultArr = kernel.getResult();

        int N_GEN_TUPLES = kernel.getNGenTuples();
//      int MAX_SIZE = kernel.getMaxSize();
        
        reMapCube(LINES, COLS, N_GEN_TUPLES, nDimensions);
        
        jcl.instantiateGlobalVar("resource_"+machineID, resource);
        
	}
	
	public boolean hasWorkingGPU() {
		try {
			List<OpenCLPlatform> platforms = new ArrayList<>();
			platforms = (new OpenCLPlatform()).getOpenCLPlatforms();
			if (platforms.isEmpty() || !platforms.get(0).getVendor().contains("NVIDIA")) {
				System.out.println("NOT GPU CAPABLE");
				return false;
			}
			return true;	
		} catch (Exception e) {
			System.out.println("NOT GPU CAPABLE");
			return false;
		}
	}
	
	@SuppressWarnings("unchecked")
	private void aggregateFilterResults(int machineID, int measureCol) {
		int nCores = Runtime.getRuntime().availableProcessors();
		for (int i=0; i<nCores; i++) {
			Object2ObjectMap<IntList, IntList> filtRes = (Object2ObjectMap<IntList, IntList>) 
														  jcl.getValue(machineID+"_filter_core_"+i).getCorrectResult();
			Int2ObjectMap<Int2FloatOpenHashMap> measureIndex = (Int2ObjectMap<Int2FloatOpenHashMap>) 
														        jcl.getValue(machineID+"_mesureIndex_"+i).getCorrectResult();
			
			jcl.setValueUnlocking(machineID+"_filter_core_"+i, null);
			jcl.setValueUnlocking(machineID+"_f_mesureIndex_"+i, null);
			
			for (Object2ObjectMap.Entry<IntList, IntList> e : filtRes.object2ObjectEntrySet()) {
				FloatCollection measureValues = new FloatArrayList();
				measureValues = getMeasureValues(measureIndex, e.getValue(), measureCol);
				if(!resource.containsKey(e.getKey())){
					resource.put(e.getKey(), new FloatArrayList());
				}
				resource.get(e.getKey()).addAll(measureValues);
			}
			
			jcl.deleteGlobalVar(machineID+"_filter_core_"+i);
		}
	}
	
	private FloatCollection getMeasureValues(Int2ObjectMap<Int2FloatOpenHashMap> measureIndex, IntList tids, int measureCol) {
		FloatCollection measureValues = new FloatArrayList();
		for(int tid : tids) {
			measureValues.add(measureIndex.get(tid).get(measureCol));
		}
		
		return measureValues;
	}
	
	private void filterResult2Arr(int nDimensions) {
		int nTuples = resource.size();
		filterResultArr = new int [nTuples*nDimensions];
        ObjectIterator<Entry<IntList,FloatCollection>> it = resource.object2ObjectEntrySet().iterator();
		for(int i=0; i<nTuples; i++) {
			int [] tuple = it.next().getKey().toIntArray();
			for(int j=0; j<nDimensions; j++) {
				filterResultArr[nDimensions*i+j] = tuple[j];
			}
		}
	}
	
	private void reMapCube(int LINES, int COLS, int N_GEN_TUPLES, int nDimensions) {
		for (int i=0; i<LINES; i++) {
			int currLine = N_GEN_TUPLES*i;
			IntList originalTuple = new IntArrayList(nDimensions);
			for (int j=0; j<nDimensions; j++)
				originalTuple.add(filterResultArr[COLS*currLine+j]);
			
			for (int j=1; j<N_GEN_TUPLES; j++) {
				IntList genTuple = new IntArrayList(nDimensions);
				for(int k=0; k<nDimensions; k++) {
					genTuple.add(filterResultArr[COLS*(currLine+j)+k]);
				}
				
				if (!resource.containsKey(genTuple)) 
					resource.put(genTuple, new FloatArrayList());
				resource.get(genTuple).addAll(resource.get(originalTuple));
			}
		}
	}
}



























