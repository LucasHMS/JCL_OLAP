package cube.aggregation.operators;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.floats.FloatCollection;

import org.apache.commons.math3.stat.descriptive.moment.Skewness;

public class SkewnessV implements AggregationOperator{

	@Override
	public float compute(Object args) {
		FloatCollection values = (FloatCollection) args;
		DoubleList dl = new DoubleArrayList();
		values.forEach(v -> dl.add((double) v));

		Skewness m = new Skewness();
		double d = m.evaluate(dl.toDoubleArray());
		float f = (float) d;
		
		return f;
	}

}
