package data.distribution;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import org.apache.commons.math3.util.ArithmeticUtils;
import util.HostWrapper;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import javafx.util.Pair;

public class DistributionBalancer {
	private List<HostWrapper> hosts;
	private SortedMap<Integer, List<HostWrapper>> capacityMap;
	public List<Pair<
					 Pair<
						 List<HostWrapper>, 
						 List<HostWrapper>
	     			 >, 
					 Pair<
						 Integer, 
						 Integer>
					 >
				> 
	calcRatios;
	private JCL_facade jcl;

	public DistributionBalancer(List<HostWrapper> hosts) {
		jcl = JCL_FacadeImpl.getInstance();
		this.hosts = hosts;
		capacityMap = new TreeMap<>();
		calcRatios = new ArrayList<>();
	}
	
	public void getCapacities() throws InterruptedException, ExecutionException {
		for(HostWrapper hw : hosts) {
			Object [] args = {};
			int capacity = (int) jcl.executeOnDevice(hw.jclHost, "Consumer", "getMemory", args).get().getCorrectResult();
			if(!capacityMap.containsKey(capacity)){
				capacityMap.put(capacity, new ArrayList<>());
			}
			capacityMap.get(capacity).add(hw);
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void caculateRatios() {
		try {
			getCapacities();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		int n = capacityMap.size();
		List<Entry<Integer, List<HostWrapper>>> capacities = new ArrayList<>(capacityMap.entrySet()); 
		for(int i=0,j=n-1; i<j; i++,j--) {
			List<HostWrapper> h1 = capacities.get(i).getValue();
			List<HostWrapper> h2 = capacities.get(j).getValue();
			Pair<List<HostWrapper>,List<HostWrapper>> hs = new Pair<>(h1,h2);
			
			int cap1 = capacities.get(i).getKey();
			int cap2 = capacities.get(j).getKey();
			int gcd = ArithmeticUtils.gcd(cap1, cap2);
			cap1 = cap1/gcd;
			cap2 = cap2/gcd;
			Pair<Integer, Integer> caps = new Pair<>(cap1,cap2);
			
			Pair hs_p = new Pair<>(hs,caps);
			
			calcRatios.add(hs_p);
		}
		if(n%2 != 0) {
			List<HostWrapper> h1 = capacities.get(n/2).getValue();
			Pair<List<HostWrapper>,List<HostWrapper>> hs = new Pair<>(h1,h1);
			Pair<Integer, Integer> caps = new Pair<>(1,0);
			Pair hs_p = new Pair<>(hs,caps);
			calcRatios.add(hs_p);
		}
	}
	
	public static void main (String [] args) {
		JCL_facade jcl = JCL_FacadeImpl.getInstance();
		jcl.register(Consumer.class, "Consumer");
		List<HostWrapper> hosts = new ArrayList<>();
		int i=0;
		for(Entry<String,String> h : jcl.getDevices()) {
			HostWrapper hw = new HostWrapper(h, i);
			hosts.add(hw);
		}
		DistributionBalancer db = new DistributionBalancer(hosts);
		try {
			db.getCapacities();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
	
}
