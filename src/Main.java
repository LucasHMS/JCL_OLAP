
import implementations.util.Entry;

import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_result;
import implementations.sm_kernel.JCL_FacadeImpl;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import data.distribution.FileManip;

public class Main {
	public static void main (String [] args){
		/*
		 * Invoca a leitura do arquivo e apartir das tuplas retornadas,
		 * Chama executes lambari para criar os indices
		 **/
		JCL_facade jcl = JCL_FacadeImpl.getInstance();

		jcl.register(Index.class, "Index");

		FileManip fm;
		List<String []>tuplas = null;
		Map<Integer, String> dimensionNames = null;
		Map<Integer, String> mesureNames = null;
		Map<String,String> dimensionTypes = null;
		Map<String, String> header = null;

		try {
			fm = new FileManip("input/NorthwindSalesData.data", "input/dimensionsnames.mg",
					"input/measuresnames.mg", "input/dimensionstypes.mg");
			tuplas = fm.readTuplas();
			dimensionNames = fm.readDimensionNames();
			dimensionTypes =  fm.readDimensionTypes();
			mesureNames = fm.readMesureNames();
			header = fm.header();
		} catch (IOException e) {
			e.printStackTrace();
		}

		Object [] arg = {tuplas, dimensionNames, mesureNames, dimensionTypes, header};
		Future<JCL_result> result = jcl.execute("Index", "setFields",arg);
		try {
			result.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		
		int coreCount = jcl.getClusterCores();
		int partitionSize = tuplas.size()/coreCount;
		Object [][] args1 = new Object[coreCount][];
		int j=0,i=0;

		for(;i<coreCount;i++,j+=partitionSize){
			Object [] arg1 = {i,j,j+partitionSize};
			args1[i] = arg1;
		}
		if(tuplas.size()%coreCount != 0){
			Object [] arg1 = {i-1,j-partitionSize,tuplas.size()-1};
			args1[coreCount-1] = arg1;
		}

		List<Future<JCL_result>> results = jcl.executeAllCores("Index", "mesureIndex" ,args1);
		jcl.getAllResultBlocking(results);

		/*for(i=0;i<coreCount;i++){
			for(j=0;j<3;j++){
				System.out.print(args1[i][j] + " : ");
			}
			System.out.println("");
		}*/

		jcl.cleanEnvironment();
		jcl.destroy();

	}
}
