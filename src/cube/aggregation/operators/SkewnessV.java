package cube.aggregation.operators;

import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;

public class SkewnessV implements AggregationOperator{

	@Override
	public double compute(Object args) {
		DoubleCollection values = (DoubleCollection) args;
		double [] v_arr = values.toDoubleArray();
		Skewness m = new Skewness();
		double d = m.evaluate(v_arr);
		
		return d;
	}

}
