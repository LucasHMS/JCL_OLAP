package query.filter.operators;

public interface FilterOperator {
	public boolean op(String record, String usrArg);
}
