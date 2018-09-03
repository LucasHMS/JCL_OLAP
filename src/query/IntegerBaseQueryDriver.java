package query;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import cube.IntegerBaseCube;
import cube.aggregation.IntegerBaseAggregation;
import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_result;
import it.unimi.dsi.fastutil.floats.FloatCollection;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2FloatMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;

public class IntegerBaseQueryDriver {
	private JCL_facade jcl;
	private List<Entry<String,String>> devices;
	private QueryElements elements;
	
	public static boolean VERBOSITY = false;

	public IntegerBaseQueryDriver() {
		jcl = JCL_FacadeImpl.getInstance();
		devices = jcl.getDevices();
		
		File f1 = new File("lib/integerfilter.jar");
		File f2 = new File("lib/filteroperators.jar");
		File [] jars = { f1, f2 };
		jcl.register(jars, "IntegerBaseFilter");
		jcl.register(IntegerBaseCube.class, "IntegerBaseCube");
	}
	
	public void readAndParse(String queryText) {
		Interpreter i = new Interpreter();
		try {
			i.parse(queryText);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		elements = i.getElements();
		if(VERBOSITY) System.out.println(elements);
	}
	
	@SuppressWarnings("unchecked")
	public void filterQuery() {
		List<Future<JCL_result>> tickets = new ArrayList<Future<JCL_result>>(); 
		for(int i=0;i<devices.size();i++) {
			Entry<String,String> e = devices.get(i);
			int n = jcl.getDeviceCore(e);
			for(int j=0;j<n;j++) {
				Object [] args = {elements.getColumnList(), elements.getOperatorList(), elements.getOpArgList(),
									elements.getIntraOpFilter(), new Integer(i), new Integer(j)};
				tickets.add(jcl.executeOnDevice(e, "IntegerBaseFilter", "filtra", args));
			}
		}
		jcl.getAllResultBlocking(tickets);
		
		if(VERBOSITY) {
			for(int i=0;i<devices.size();i++) {
				int n = jcl.getDeviceCore(devices.get(i));
				for(int j=0;j<n;j++) {
					System.out.println("M="+i + " - C= " + j);
					Object2ObjectMap<IntList, IntList> filterResults = (Object2ObjectMap<IntList, IntList>) 
																		jcl.getValue(i+"_filter_core_"+j).getCorrectResult();
					for(Object2ObjectMap.Entry<IntList, IntList> e : filterResults.object2ObjectEntrySet()) {
						System.out.println(e.getKey() + " => " + e.getValue());
					}
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void createCube() {
		List<Future<JCL_result>> tickets = new ArrayList<Future<JCL_result>>();
		int nDimensions = elements.getColumnList().size();
		for(int i=0;i<devices.size();i++) {
			Entry<String,String> e = devices.get(i);
			int n = jcl.getDeviceCore(e);
			for(int j=0;j<n;j++) {
				Object [] args = {i,j,nDimensions};
				tickets.add(jcl.executeOnDevice(e,"IntegerBaseCube", "createCube", args));
			}
		}
		jcl.getAllResultBlocking(tickets);
		
		if(VERBOSITY) {
			for(int i=0;i<devices.size();i++) {
				int n = jcl.getDeviceCore(devices.get(i));
				for(int j=0;j<n;j++) {
					System.out.println("M="+i + " - C= " + j);
					Object2ObjectMap<IntList, IntList> filterResults = (Object2ObjectMap<IntList, IntList>) 
																			jcl.getValue(i+"_filter_core_"+j).getCorrectResult();
					for(Object2ObjectMap.Entry<IntList, IntList> e : filterResults.object2ObjectEntrySet()) {
						System.out.println(e.getKey() + " => " + e.getValue());
					}
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void aggregateCubes() {
		List<Future<JCL_result>> tickets = new ArrayList<Future<JCL_result>>();
		List<Integer> aggColuns = elements.getAgregationColumns();
		for(int i=0;i<devices.size();i++) {
			Entry<String,String> e = devices.get(i);
			int n = jcl.getDeviceCore(e);
			for(int j=0;j<n;j++) {
				Object [] args = {i,j,aggColuns};
				tickets.add(jcl.executeOnDevice(e,"IntegerBaseCube", "getMeasureValues", args));
			}
		}
		List<JCL_result> results = jcl.getAllResultBlocking(tickets);
		
		List<Object2ObjectMap<IntList, FloatCollection>> aggregationValues = new ArrayList<>();
		for(JCL_result r: results) {
			aggregationValues.add((Object2ObjectMap<IntList, FloatCollection>) r.getCorrectResult());
		}
		
		IntegerBaseAggregation ag = new IntegerBaseAggregation(elements.getAgregationOp().get(0));
		ag.mergeSubCubes(aggregationValues);
		Object2FloatMap<IntList>  aggResult = ag.aggregate();
		
		if(VERBOSITY) {
			for(Object2FloatMap.Entry<IntList> e : aggResult.object2FloatEntrySet()) {
				System.out.println(e.getKey() + " => " + e.getFloatValue());
			}
		}
	}
	
	public void deleteFilterResult() {
		for(int i=0;i<devices.size();i++) {
			int n = jcl.getDeviceCore(devices.get(i));
			for(int j=0;j<n;j++) {
				jcl.deleteGlobalVar(i+"_filter_core_"+j);
			}
		}
	}
}
