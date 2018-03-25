package query.filter.operators;

public class LessThanOp implements FilterOperator {
	
	@Override
	public boolean op(String record, String usrArg) {
		boolean x = false;
		if(record.contains("[0-9]+") && usrArg.contains("[0-9]+"))
    	{
    		float d = Float.parseFloat(record);
        	float p = Float.parseFloat(usrArg);
        	if(d < p){
        		x = true;
        	}
    	}
    	else
    	{
    		int i = record.compareTo(usrArg);
    		if(i<0)
    		{
        		x = true;
        	}
    	}
		    	
		return x;
	}
}
