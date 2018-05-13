package query;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import cube.Cube;
import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_result;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;

public class QueryDriver {
	JCL_facade jcl = JCL_FacadeImpl.getInstance();
	private QueryElements elements;

	public QueryDriver() {
		File f1 = new File("lib/filter.jar");
		File f2 = new File("lib/filteroperators.jar");
//		File f3 = new File("lib/queryelements.jar");
		File [] jars = { f1, f2 };
		System.out.println("registro Filter: " + jcl.register(jars, "Filter"));
		jcl.register(Cube.class, "Cube");
		
	}
	
	public void readAndParse() {
		Interpreter i = new Interpreter();
		i.readQuery();
		try {
			i.parseQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		elements = i.getElements();
//		System.out.println(elements);
	}
	
	public void readAndParse(String queryText) {
		Interpreter i = new Interpreter();
		try {
			i.parseQuery(queryText);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		elements = i.getElements();
//		System.out.println(elements);
	}
	
	@SuppressWarnings("unchecked")
	public void filterQuery() {
		List<Entry<String,String>> devices = jcl.getDevices();
		List<Future<JCL_result>> tickets = new ArrayList<Future<JCL_result>>(); 
		for(int i=0;i<devices.size();i++) {
			Entry<String,String> e = devices.get(i);
			int n = jcl.getDeviceCore(e);
			for(int j=0;j<n;j++) {
				Object [] args = {elements.getColumnList(), elements.getOperatorList(), elements.getOpArgList(),
									elements.getIntraOpFilter(), new Integer(i), new Integer(j)};
				tickets.add(jcl.executeOnDevice(e,"Filter", "filtra", args));
			}
		}
		jcl.getAllResultBlocking(tickets);
		
		for(int i=0;i<devices.size();i++) {
			int n = jcl.getDeviceCore(devices.get(i));
			for(int j=0;j<n;j++) {
				System.out.println("M="+i + " - C= " + j);
				Object2ObjectMap<String, IntCollection> filterResults = (Object2ObjectMap<String, IntCollection>) jcl.getValue(i+"_filter_core_"+j).getCorrectResult();
				for(Entry<String, IntCollection> e : filterResults.entrySet()) {
					System.out.println(e.getKey() + " => " + e.getValue());
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void createCube() {
		List<Entry<String,String>> devices = jcl.getDevices();
		List<Future<JCL_result>> tickets = new ArrayList<Future<JCL_result>>();
		int nDimensions = elements.getColumnList().size();
		for(int i=0;i<devices.size();i++) {
			Entry<String,String> e = devices.get(i);
			int n = jcl.getDeviceCore(e);
			for(int j=0;j<n;j++) {
				Object [] args = {i,j,nDimensions};
				tickets.add(jcl.executeOnDevice(e,"Cube", "createCube", args));
			}
		}
		jcl.getAllResultBlocking(tickets);
		
		for(int i=0;i<devices.size();i++) {
			int n = jcl.getDeviceCore(devices.get(i));
			for(int j=0;j<n;j++) {
				System.out.println("M="+i + " - C= " + j);
				Object2ObjectMap<String, IntCollection> filterResults = (Object2ObjectMap<String, IntCollection>) jcl.getValue(i+"_filter_core_"+j).getCorrectResult();
				for(Entry<String, IntCollection> e : filterResults.entrySet()) {
					System.out.println(e.getKey() + " => " + e.getValue());
				}
			}
		}
	}
}
