package query.filter.operators;

public class EndsWithOp implements FilterOperator {

	@Override
	public boolean op(String record, String usrArg) {
    	boolean x = true;
    	
    	x = record.endsWith(usrArg);
    	
    	return x;
	}

	@Override
	public boolean op(int record, int usrArg) {
		return false;
	}

	@Override
	public boolean op(float record, float usrArg) {
		return false;
	}

}
