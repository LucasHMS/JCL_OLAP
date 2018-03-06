package query.filter.operators;

public class StartsWithOp implements FilterOperator {

	@Override
	public boolean op(String record, String usrArg) {
		boolean x = true;
    	
    	x = record.startsWith(usrArg);
    	
    	return x;
	}

}
