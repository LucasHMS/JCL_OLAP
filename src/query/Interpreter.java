package query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import implementations.dm_kernel.user.JCL_FacadeImpl;

public class Interpreter {
	private Map<String, Integer> dimensionMeta;
	private Map<String, Integer> mesureMeta;
	private QueryElements elements;
	
	public Interpreter() {
		elements = new QueryElements();
		dimensionMeta = JCL_FacadeImpl.GetHashMap("Dimension");	
		mesureMeta = JCL_FacadeImpl.GetHashMap("Mesure");
	}
	
	public void parse(String queryText) throws Exception {
		List<String> filters = tokenize(queryText.split(";")[0]);
		for (int i=0;i<filters.size();) {
			i = parseFilter(i, filters);
			if (i >= filters.size()) break;
			i = parseIntraFilter(i,filters);
		}

		List<String> aggregations = tokenize(queryText.split(";")[1]);		
		for(int i=0;i<aggregations.size();) {
			i = parseAggregation(i, aggregations);
		}
		
		if(elements.getOperatorList().contains(7))
			orderInquire();
	}
	
	private void orderInquire() {
		List<Integer> opList = elements.getOperatorList();
		List<String> columnList = elements.getColumnList();
		List<Integer> auxOpList = new ArrayList<>();
		List<String> auxColumnList = new ArrayList<>();
		while(opList.contains(7)) {
			int indexInq = opList.lastIndexOf(7);
			opList.remove(indexInq);
			String col = columnList.remove(indexInq);
			auxOpList.add(7);
			auxColumnList.add(col);
		}
		opList.addAll(auxOpList);
		columnList.addAll(auxColumnList);

		elements.setOperatorList(opList);
		elements.setColumnList(columnList);
	}
	
	private List<String> tokenize(String queryText){
		List<String> tokens = new ArrayList<String>();
		Matcher m = Pattern.compile("([^\'^ ]\\S*|\'.+?\')\\s*").matcher(queryText);
		while (m.find())
		    tokens.add(m.group(1).replace("\'", "")); 

		return tokens;
	}
	
	
	private int parseIntraFilter(int i, List<String> tokens) throws Exception {
		int op = checkIntraOp(tokens.get(i));
		
		if(op == -1) throw new Exception("Invalid Column " + tokens.get(i));
	
		elements.setIntraOpFilter(op);
		i++;
		
		return i;
	}

	// Parsing filters
	private int parseFilter(int i, List<String> tokens) throws Exception {
		parseDimension(tokens.get(i));
		i++;
		int op = parseOperator(tokens.get(i));
		i++;
		if (op != 7) {
			parseArg(tokens.get(i));
			i++;
		}
		
		return i;
	}
	
	private int parseDimension(String column) throws Exception {
		if(!checkDimension(column)) throw new Exception("Invalid Column " + column);
		
		elements.setColumnList(column);
		
		return 1;
	}

	private int parseOperator(String operator) throws Exception {
		int op = checkOperator(operator);
		
		if(op == -1) throw new Exception("Invalid Operator " + operator);
		
		elements.setOperatorList(op);
		
		return op;
	}

	private int parseArg(String arg) {
		elements.setOpArgList(arg);
		
		return 1;
	}
		
	// Parsing Aggregations
	private int parseAggregation(int i, List<String> tokens) throws Exception {
		parseAggregator(tokens.get(i));
		i++;
		parseMeasure(tokens.get(i));
		i++;
		return i;
	}

	private int parseAggregator(String aggregator) throws Exception {
		int op = checkAgregationOP(aggregator);
		
		if(op == -1) throw new Exception("Invalid Operator " + aggregator);
		
		elements.setAgregationOp(op);
		
		return 1;
	}
	
	private int parseMeasure(String measure) throws Exception {
		int pos = checkMesure(measure);

		if(pos == -1) throw new Exception("Invalid Column " + measure);
		
		elements.setAgregationColumns(pos);
		
		return 1;
	}
	
	private boolean checkDimension(String column) {
		return dimensionMeta.containsKey(column);
	}
	
	private int checkMesure(String column) {
		Integer pos = mesureMeta.get(column);
		if (pos == null) return -1;
		
		return pos+1;/*mesureMeta.containsKey(column);*/
	}
	
	private int checkOperator(String op) {
		switch (op.toLowerCase()) {
		case "startswith":
			return 1;
		case "endswith":
			return 2;
		case ">":
			return 3;
		case "<":
			return 4;
		case "=":
			return 5;
		case "contains":
			return 6;
		case "inquire":
			return 7;
		default:
			return -1;
		}
	}
	
	private int checkIntraOp(String op) {
		switch (op.toLowerCase()) {
		case "and":
			return 1;
		case "or":
			return 2;
		default:
			return -1;
		}
	}
	
	private int checkAgregationOP(String op) {
		switch (op.toLowerCase()) {
		case "min":
			return 1;
		case "max":
			return 2;
		case "sum":
			return 3;
		case "mean":
			return 4;
		case "median":
			return 5;
		case "mode":
			return 6;
		case "skewness":
			return 7;
		case "std":
			return 8;
		case "variance":
			return 9;
		case "geometric":
			return 10;
			
		default:
			return -1;
		}
	}

	public QueryElements getElements() {
		return elements;
	}
	
/*	public static void main(String [] args) {
		Map<String, Integer> dimensionMeta = JCL_FacadeImpl.GetHashMap("Dimension");	
		dimensionMeta.put("Data", 0);
		dimensionMeta.put("Pais", 1);
		dimensionMeta.put("Cidade",2 );
		dimensionMeta.put("Empresa",3 );
		dimensionMeta.put("Cliente", 4);
		dimensionMeta.put("CEP", 5);
		dimensionMeta.put("Categoria",6 );
		dimensionMeta.put("Produto", 7);
		
		Map<String, Integer> mesureMeta = JCL_FacadeImpl.GetHashMap("Mesure");	
		mesureMeta.put("PrecoUnitario", 0);
		mesureMeta.put("Quantidade", 1);
		
		Interpreter parser = new Interpreter();
		try {
			parser.parse("CEP inquire and Pais startsWith 'S fsd' and Cidade inquire and Empresa endsWith 'R sdfs'; max PrecoUnitario min Quantidade");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}*/
}
