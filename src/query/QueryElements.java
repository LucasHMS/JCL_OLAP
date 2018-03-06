package query;

import java.util.ArrayList;
import java.util.List;

public class QueryElements {
	private List<String> columnList;
	private List<Integer> operatorList;
	private List<String> opArgList;
	private List<Integer> intraOpFilter;
	
	public QueryElements() {
		columnList = new ArrayList<>();
		operatorList = new ArrayList<>();
		opArgList = new ArrayList<>();
		intraOpFilter= new ArrayList<>();
	}
	
	public List<String> getColumnList() {
		return columnList;
	}
	public void setColumnList(String column) {
		columnList.add(column);
	}
	public List<Integer> getOperatorList() {
		return operatorList;
	}
	public void setOperatorList(Integer operator) {
		operatorList.add(operator);
	}
	public List<String> getOpArgList() {
		return opArgList;
	}
	public void setOpArgList(String arg) {
		opArgList.add(arg);
	}
	public List<Integer> getIntraOpFilter() {
		return intraOpFilter;
	}
	public void setIntraOpFilter(Integer intraOp) {
		intraOpFilter.add(intraOp);
	}
}
