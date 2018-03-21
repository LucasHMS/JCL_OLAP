package query;

import java.util.Map;
import java.util.Scanner;

import implementations.dm_kernel.user.JCL_FacadeImpl;

public class Interpreter {
	private QueryElements elements;
	private Map<String, Integer> dimensionMeta = JCL_FacadeImpl.GetHashMap("Dimension");	
	private String queryText;
	
	public Interpreter() {
		elements = new QueryElements();
		queryText = "";
	}
	
	public QueryElements getElements() {
		return elements;
	}
	
	public void readQuery() {
		Scanner keyboard = new Scanner(System.in);
		queryText = keyboard.nextLine();
		keyboard.close();
	}
	
	public void parseQuery() throws Exception {
		for(int i=0; i<queryText.length();i++){
			i = parseColumn(i);
			i = parseOperator(i);
			i = parseArg(i);
			if (i < queryText.length())
				i = parseOpIntraFilter(i);
		}
	}
	
	public void parseQuery(String queryText) throws Exception {
		this.queryText = queryText;
		for(int i=0; i<queryText.length();i++){
			i = parseColumn(i);
			i = parseOperator(i);
			i = parseArg(i);
			if (i < queryText.length())
				i = parseOpIntraFilter(i);
		}
	}
	
	private int parseColumn(int pos) throws Exception {
		String column = "";
		char c = queryText.charAt(pos++);
		do {
			column += c;
			c = queryText.charAt(pos++);
		}while(c != ' ');
		
		if (!checkColumn(column)) {
			throw new Exception("Invalid Column " + column);
		}
		
		elements.setColumnList(column);		
		
		return pos;
	}
	
	
	private int parseOperator(int pos) throws Exception {
		String op = "";
		char c = queryText.charAt(pos++);
		do {
			op += c;
			c = queryText.charAt(pos++);
		}while(c != ' ');
		
		int opID = checkOperator(op);
		if (opID == -1) {
			throw new Exception("Invalid Operator " + op);
		}
		
		elements.setOperatorList(opID);

		return pos;	
	}
	
	private int parseArg(int pos) {
		String arg = "";
		char c = queryText.charAt(++pos); pos++;
		do {
			arg += c;
			c = queryText.charAt(pos++);
		}while(c != '"');
		
		elements.setOpArgList(arg);
		
		return ++pos;
	}
	
	private int parseOpIntraFilter(int pos) throws Exception {
		String op = "";
		char c = queryText.charAt(pos++);
		do {
			op += c;
			c = queryText.charAt(pos++);
		}while(c != ' ');
		
		int opID = checkIntraOp(op);
		if (opID == -1) {
			throw new Exception("Invalid Operator " + op);
		}
		
		elements.setIntraOpFilter(opID);
		
		return --pos;
	}
	
	private boolean checkColumn(String column) {
		return dimensionMeta.containsKey(column);
	}
	
	private int checkOperator(String op) {
		switch (op) {
		case "startsWith":
			return 1;
		case "endsWith":
			return 2;
		case ">":
			return 3;
		case "<":
			return 4;
		case "=":
			return 5;
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
		return 0;
	}
	
	public static void main(String [] args) {
		Map<String, Integer> dimensionMeta = JCL_FacadeImpl.GetHashMap("Dimension");	
		dimensionMeta.put("Data", 0);
		dimensionMeta.put("Pais", 1);
		dimensionMeta.put("Cidade",2 );
		dimensionMeta.put("Empresa",3 );
		dimensionMeta.put("Cliente", 4);
		dimensionMeta.put("CEP", 5);
		dimensionMeta.put("Categoria",6 );
		dimensionMeta.put("Produto", 7);
		
		Interpreter i = new Interpreter();
		i.readQuery();
		try {
			i.parseQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
