package cube.aggregation.operators;

import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import org.apache.commons.math3.stat.descriptive.rank.Max;

public class MaxV implements AggregationOperator{

	@Override
	public double compute(Object args) {
		DoubleCollection values = (DoubleCollection) args;
		double [] v_arr = values.toDoubleArray();
		Max m = new Max();
		double d = m.evaluate(v_arr);
		
		return d;
	}

}
