package cube;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_result;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;

public class CubeDriver {
	private JCL_facade jcl;
	
	public CubeDriver() {
		jcl = JCL_FacadeImpl.getInstance();
		jcl.register(Cube.class, "Cube");
	}
	
	@SuppressWarnings("unchecked")
	public void exeCube() {
		List<Entry<String,String>> devices = jcl.getDevices();
		List<Future<JCL_result>> tickets = new ArrayList<Future<JCL_result>>(); 
		for(int i=0;i<devices.size();i++) {
			Entry<String,String> e = devices.get(i);
			int n = jcl.getDeviceCore(e);
			for(int j=0;j<n;j++) {
				Object [] args = {i,j};
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
