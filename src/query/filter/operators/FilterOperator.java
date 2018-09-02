package query.filter.operators;

public interface FilterOperator {
	public boolean op(String record, String usrArg);
	public boolean op(int record, int usrArg);
	public boolean op(float record, float usrArg);
}
