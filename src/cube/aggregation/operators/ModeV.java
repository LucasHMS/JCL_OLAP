package cube.aggregation.operators;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.floats.FloatCollection;

import org.apache.commons.math3.stat.StatUtils;

public class ModeV implements AggregationOperator{

	@Override
	public float compute(Object args) {
		FloatCollection values = (FloatCollection) args;		
		DoubleList dl = new DoubleArrayList();
		values.forEach(v -> dl.add((double) v));
		
		double[] d = StatUtils.mode(dl.toDoubleArray());
		
		if(d.length > 1) 
			return (float) StatUtils.mean(d);
		
		return (float) d[0];
	}

}
