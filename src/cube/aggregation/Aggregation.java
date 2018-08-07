package cube.aggregation;

import java.util.List;
import java.util.Map.Entry;

import cube.aggregation.operators.AggregationOperator;
import cube.aggregation.operators.*;
import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

public class Aggregation {
	private AggregationOperator op;
	private Object2ObjectMap<String, DoubleCollection> merged_subcubes;
	private Object2DoubleMap<String> agg_cube;
	
	public Aggregation(int op) {
		instantiateOperator(op);
		this.merged_subcubes = new Object2ObjectOpenHashMap<String, DoubleCollection>();
		agg_cube = new Object2DoubleOpenHashMap<>(); 
	}
	
	public Object2DoubleMap<String> aggregate() {
		for(Entry<String, DoubleCollection> e : merged_subcubes.entrySet()) {
			double d = op.compute(e.getValue());
			agg_cube.put(e.getKey(), d);
		}
		
		return agg_cube;
	}
	
	public Object2DoubleMap<String> getAggregatedCube(){
		return agg_cube;
	}
	
	public void instantiateOperator(int opCode) {
		switch (opCode) {
		case 1:
			op = new MinV();
		case 2:
			op = new MaxV();
		case 3:
			op = new SumV();
		case 4:
			op = new MeanV();
		case 5:
			op = new MedianV();
		case 6:
			op = new ModeV();
		case 7:
			op = new SkewnessV();
		case 8:
			op = new StdV();
		case 9:
			op = new VarianceV();
		case 10:
			op = new GeometricMeanV();
		default:
			break;
		}
	}
	
	public void mergeSubCubes(List<Object2ObjectMap<String, DoubleCollection>> subCubes) {
		merged_subcubes.putAll(subCubes.remove(0));
		for(Object2ObjectMap<String, DoubleCollection> subCube : subCubes) {
			for(Entry<String, DoubleCollection> e: subCube.entrySet()) {
				if(!merged_subcubes.containsKey(e.getKey())) {
					merged_subcubes.put(e.getKey(),e.getValue());
				}else {
					merged_subcubes.get(e.getKey()).addAll(e.getValue());
				}
			}
		}
	}
}
