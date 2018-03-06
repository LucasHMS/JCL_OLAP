package query.filter.operators;

public class EqualsToOp implements FilterOperator {

	@Override
	public boolean op(String record, String usrArg) {
    	boolean x = false;
    	float d = Float.parseFloat(record);
    	float p = Float.parseFloat(usrArg);
    	if(d == p){
    		x = true;
    	}
    	
    	return x;
	}

}
