package cube.aggregation.operators;

import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import org.apache.commons.math3.stat.descriptive.rank.Median;

public class MedianV implements AggregationOperator{

	@Override
	public double compute(Object args) {
		DoubleCollection values = (DoubleCollection) args;
		double [] v_arr = values.toDoubleArray();
		Median m = new Median();
		double d = m.evaluate(v_arr);
		
		return d;
	}

}
