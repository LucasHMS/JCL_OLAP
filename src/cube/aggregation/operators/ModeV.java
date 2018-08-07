package cube.aggregation.operators;

import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import org.apache.commons.math3.stat.StatUtils; 

public class ModeV implements AggregationOperator{

	@Override
	public double compute(Object args) {
		DoubleCollection values = (DoubleCollection) args;
		double [] v_arr = values.toDoubleArray();
		double[] d = StatUtils.mode(v_arr);
		
		if(d.length > 1) 
			return StatUtils.mean(d);
		
		return d[0];
	}

}
