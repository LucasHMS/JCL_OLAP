package util;

import java.util.Map.Entry;

public class HostWrapper {
	public Entry<String, String> jclHost;
	public int machineId;
	
	public HostWrapper(Entry<String, String> jclHost, int machineId){
		this.jclHost = jclHost;
		this.machineId = machineId;
	}	
}
