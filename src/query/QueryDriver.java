package query;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_result;

public class QueryDriver {
	JCL_facade jcl = JCL_FacadeImpl.getInstance();
	private QueryElements elements;

	public QueryDriver() {
		File f1 = new File("lib/filter.jar");
		File f2 = new File("lib/filteroperators.jar");
		File [] jars = { f1, f2 };
		System.out.println("registro Filter: " + jcl.register(jars, "Filter"));
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
		/*System.out.println("colum list: " + elements.getColumnList());
		System.out.println("op list: " + elements.getOperatorList());
		System.out.println("arg list: " + elements.getOpArgList());
		System.out.println("intra op list: " + elements.getIntraOpFilter());*/
	}
	
	public void readAndParse(String queryText) {
		Interpreter i = new Interpreter();
		try {
			i.parseQuery(queryText);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		elements = i.getElements();
		/*System.out.println("colum list: " + elements.getColumnList());
		System.out.println("op list: " + elements.getOperatorList());
		System.out.println("arg list: " + elements.getOpArgList());
		System.out.println("intra op list: " + elements.getIntraOpFilter());*/
	}
	
	public void filterQuery() {
		List<Entry<String,String>> devices = jcl.getDevices();
		List<Future<JCL_result>> tickets = new ArrayList<Future<JCL_result>>(); 
		int j = 0;
		for(Entry<String,String> e : devices) {
			int n = jcl.getDeviceCore(e);
			for(int i=0;i<n;i++) {
				Object [] args = {elements.getColumnList(), elements.getOperatorList(),
									elements.getOpArgList(), elements.getIntraOpFilter(), new Integer(j), new Integer(i)};
				tickets.add(jcl.executeOnDevice(e,"Filter", "filtra", args));
			}
			j++;
		}
		jcl.getAllResultBlocking(tickets);
		try {
			Thread.sleep(500);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		for(int x=0;x<4;x++) 
			System.out.println("CORE " + x +jcl.getValue(0+"_filter_core_"+x).getCorrectResult());
		
	}
}
