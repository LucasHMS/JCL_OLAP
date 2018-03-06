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
	}
	
	public void filterQuery() {
		List<Entry<String,String>> devices = jcl.getDevices();
		List<Future<JCL_result>> tickets = new ArrayList<Future<JCL_result>>(); 
		for(Entry<String,String> e : devices) {
			int n = jcl.getDeviceCore(e);
			for(int i=0;i<n;i++) {
				Object [] args = {elements.getColumnList(), elements.getOperatorList(),
									elements.getOpArgList(), elements.getIntraOpFilter(), new Integer(i)};
				tickets.add(jcl.executeOnDevice(e,"Filter", "filtra", args));
			}
		}
		jcl.getAllResultBlocking(tickets);
	}
}
