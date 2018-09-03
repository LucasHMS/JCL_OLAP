package cube.aggregation.operators;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.floats.FloatCollection;

import org.apache.commons.math3.stat.descriptive.moment.Variance;

public class VarianceV implements AggregationOperator{

	@Override
	public float compute(Object args) {
		FloatCollection values = (FloatCollection) args;
		DoubleList dl = new DoubleArrayList();
		values.forEach(v -> dl.add((double) v));

		Variance m = new Variance();
		double d = m.evaluate(dl.toDoubleArray());
		float f = (float) d;
		
		return f;
	}

}
