package cube.aggregation;

import java.util.List;

import cube.aggregation.operators.*;
import it.unimi.dsi.fastutil.floats.FloatCollection;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2FloatMap;
import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

public class IntegerBaseAggregation {
	private AggregationOperator op;
	private Object2ObjectMap<IntList, FloatCollection> merged_subcubes;
	private Object2FloatMap<IntList> agg_cube;
	
	public IntegerBaseAggregation(int op) {
		instantiateOperator(op);
		this.merged_subcubes = new Object2ObjectOpenHashMap<IntList, FloatCollection>();
		agg_cube = new Object2FloatOpenHashMap<>(); 
	}
	
	public Object2FloatMap<IntList> aggregate() {
		for(Object2ObjectMap.Entry<IntList, FloatCollection> e : merged_subcubes.object2ObjectEntrySet()) {
			float d = op.compute(e.getValue());
			agg_cube.put(e.getKey(), d);
		}
		
		return agg_cube;
	}
	
	public Object2FloatMap<IntList> getAggregatedCube(){
		return agg_cube;
	}
	
	public void instantiateOperator(int opCode) {
		switch (opCode) {
		case 1:
			op = new MinV();
		case 2:
			op = new MaxV();
			break;
		case 3:
			op = new SumV();
			break;
		case 4:
			op = new MeanV();
			break;
		case 5:
			op = new MedianV();
			break;
		case 6:
			op = new ModeV();
			break;
		case 7:
			op = new SkewnessV();
			break;
		case 8:
			op = new StdV();
			break;
		case 9:
			op = new VarianceV();
			break;
		case 10:
			op = new GeometricMeanV();
			break;
		default:
			break;
		}
	}
	
	public void mergeSubCubes(List<Object2ObjectMap<IntList, FloatCollection>> subCubes) {
		merged_subcubes.putAll(subCubes.remove(0));
		for(Object2ObjectMap<IntList, FloatCollection> subCube : subCubes) {
			for(Object2ObjectMap.Entry<IntList, FloatCollection> e: subCube.object2ObjectEntrySet()) {
				if(!merged_subcubes.containsKey(e.getKey())) {
					merged_subcubes.put(e.getKey(),e.getValue());
				}else {
					merged_subcubes.get(e.getKey()).addAll(e.getValue());
				}
			}
		}
	}
}
