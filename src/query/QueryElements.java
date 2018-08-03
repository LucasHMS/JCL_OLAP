package query;

import java.util.ArrayList;
import java.util.List;

public class QueryElements {
	private List<String> columnList;
	private List<Integer> operatorList;
	private List<String> opArgList;
	private List<Integer> intraOpFilter;
	private List<Integer> agregationOp;
	private List<Integer> agregationColumns;
	
	public QueryElements() {
		columnList = new ArrayList<>();
		operatorList = new ArrayList<>();
		opArgList = new ArrayList<>();
		intraOpFilter= new ArrayList<>();
		agregationOp = new ArrayList<>();
		agregationColumns = new ArrayList<>();
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
	
	public List<Integer> getAgregationOp() {
		return agregationOp;
	}

	public void setAgregationOp(Integer agrgOp) {
		agregationOp.add(agrgOp);
	}

	public List<Integer> getAgregationColumns() {
		return agregationColumns;
	}

	public void setAgregationColumns(Integer column) {
		agregationColumns.add(column);
	}

	
	
	public void setColumnList(List<String> columnList) {
		this.columnList = columnList;
	}

	public void setOperatorList(List<Integer> operatorList) {
		this.operatorList = operatorList;
	}

	public void setOpArgList(List<String> opArgList) {
		this.opArgList = opArgList;
	}

	public void setIntraOpFilter(List<Integer> intraOpFilter) {
		this.intraOpFilter = intraOpFilter;
	}

	public void setAgregationOp(List<Integer> agregationOp) {
		this.agregationOp = agregationOp;
	}

	public void setAgregationColumns(List<Integer> agregationColumns) {
		this.agregationColumns = agregationColumns;
	}

	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append("colum list: " + columnList + "\n");
		s.append("op list: " + operatorList + "\n");
		s.append("arg list: " + opArgList + "\n");
		s.append("intra op list: " + intraOpFilter + "\n");
		s.append("agregation op: " + agregationOp + "\n");
		s.append("agregation colum: " + agregationColumns);
		
		return s.toString();
	}
}
