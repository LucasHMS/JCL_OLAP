package query.filter.operators;

public class ContainsOp implements FilterOperator {

	@Override
	public boolean op(String record, String usrArg) {
		boolean x = true;
    	
    	x = record.contains(usrArg);
    	
    	return x;
	}

}
