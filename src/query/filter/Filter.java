package query.filter;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import query.filter.operators.*;

public class Filter
{
	private JCL_facade jcl;
	private Map<String, Integer> mesureMeta;
	private Map<String, Integer> dimensionMeta;
//	private Object2ObjectMap<String, IntCollection> jclInvertedIndex;
	
	public Filter() {
    	jcl = JCL_FacadeImpl.getInstance();
    	// map dos metadados
		mesureMeta = JCL_FacadeImpl.GetHashMap("Mesure");
		dimensionMeta = JCL_FacadeImpl.GetHashMap("Dimension");
	}
	
    /* Recebe 3 lists, a primeira com os nomes das colunas, a segunda com o tipo de filtro e a terceira com
     * os parametros do filtro. A minha classe irá ver se essas informações estão na hash do jcl.
     * Depois de achar a peirmeira coluna eu aplico o filtro, após a aplicacao do primeiro filtro eu irei
     * salvar em uma nova map apenas as tuplas que passaram no filtro. Quando for aplicar o segudo filtro,
     * irei verificar na map resultante da passagem do primeiro filtro. Após passar o segundo
     * filtro terei uma nova map, e caso tenha um terceiro filtro, aplicarei ele em minha map resultante
     * do segundo filtro. No fim, terei uma map com apenas os ANDs ds meus parametros.
     * Mais pra frente tambem receberei os filtros das mesures*/
    @SuppressWarnings("unchecked")
	public void filtra(List<String> columns, List<Integer> operators, List<String> args, 
			List<Integer> intraOpFilter, int machineID, int coreID){
    	
//		List<Integer> intraOps = query.getIntraOpFilter();
		System.out.println("Iniciando Filter. core " + coreID);

		Int2ObjectMap<String> map_core = (Int2ObjectMap<String>) jcl.getValue(machineID+"_core_"+coreID).getCorrectResult();
		Object2ObjectMap<String, IntCollection> jclInvertedIndex =  (Object2ObjectMap<String, IntCollection>) 
																	jcl.getValue(machineID+"_invertedIndex_"+coreID).getCorrectResult();

	  /*List<String> inquireCols = new ArrayList<>();
		while(operators.contains(7)) {
			operators.remove(operators.size()-1);
			String col = columns.remove(columns.size()-1);
			inquireCols.add(col);
		}*/
		
		// lista para salvar as maps de resultado de cada filtro da consulta
		
		int cubeDim = 0;
		
		Int2ObjectMap<String> filterResults = new Int2ObjectOpenHashMap<>();
		Object [] returns = runFilter(map_core, jclInvertedIndex, columns.get(0), operators.get(0), args.get(0));
		filterResults = (Int2ObjectMap<String>) returns[0];
		cubeDim *= (new Integer(returns[1].toString()));
		
		for (int i=0; i<operators.size()&&operators.get(i)!=7; i++) {
			Int2ObjectMap<String> aux = new Int2ObjectOpenHashMap<String>();
			Object [] returns1 = runFilter(filterResults, jclInvertedIndex, columns.get(i), operators.get(i), args.get(i));
			aux = (Int2ObjectMap<String>) returns1[0];
			cubeDim *= (new Integer(returns1[1].toString()));

			returns1[0] = null;
			returns1[1] = null;
			filterResults.clear();
			filterResults = null;
			filterResults = new Int2ObjectOpenHashMap<>(aux);
		}
		
		/*if(inquireCols.size() > 0) {
			columns.addAll(inquireCols);
		}*/
		
		if(operators.indexOf(7) >= 0) {
			System.out.println("Calculando a cardinalidade INQUIRE. core " + coreID);		
			for(int i=operators.indexOf(7);i<operators.size();i++) {
				int card = cardilanityInquire(map_core, jclInvertedIndex.keySet(), columns.get(i));
				cubeDim *= card;
			}
		}
		
		cubeDim += 1;
		
		Object2ObjectMap<String, IntCollection> cleanResult = generateReult(filterResults, columns);
		
		jcl.deleteGlobalVar(machineID+"_core_"+coreID);
		map_core = null;
		
		jcl.instantiateGlobalVar(machineID+"_filter_core_"+coreID, cleanResult);
		jcl.instantiateGlobalVar(machineID+"_cubeDim_"+coreID, cubeDim);
		
		System.out.println(machineID+"_cubeDim_"+coreID+": " + cubeDim);
		System.out.println("Finalizou a Filtragem. core " + coreID);
    }
    
    private /*Int2ObjectMap<String>*/Object [] runFilter(Int2ObjectMap<String> tuples, Object2ObjectMap<String, IntCollection> jclInvertedIndex,
    																				String column, int opID, String arg) {
    	Set<String> cardinality = new HashSet<>();
    	Int2ObjectMap<String> filterResult = new Int2ObjectOpenHashMap<>();
    	FilterOperator filtOp = checkOperator(opID);
    	int real_pos = realColumPos(column);
    	int colPos = dimensionMeta.get(column);
		for(Entry<Integer, String> e : tuples.entrySet()){
			if (filterResult.containsKey(e.getKey())) continue;
			String [] splitArr = e.getValue().split("\\|");
			String columnVal = splitArr[real_pos];
			boolean result = filtOp.op(columnVal, arg);
			if (result) {
				IntCollection intCol = jclInvertedIndex.get("$" + colPos + "$" + columnVal);
				intCol.forEach(tid -> filterResult.put(tid, e.getValue()));
				cardinality.add(columnVal);
//				filterResult.put(e.getKey(), e.getValue());
			}
		}
		int card = cardinality.size();

		if (card == 0) card = 1;
		Object [] returns = {filterResult, card};
		return returns;
    }
    
    private Object2ObjectMap<String, IntCollection> generateReult(Int2ObjectMap<String> rawReults, List<String> columns){
    	Object2ObjectMap<String, IntCollection> finalResult = new Object2ObjectOpenHashMap<String, IntCollection>();
		for(Entry<Integer, String> e : rawReults.entrySet()){
			String cleanedTuple = cleanTuple(e.getValue().split("\\|"), columns);
			if(!finalResult.containsKey(cleanedTuple)) {
				IntCollection keyList = new IntArrayList();
				keyList.add(e.getKey());
				finalResult.put(cleanedTuple, keyList);
			}else
				finalResult.get(cleanedTuple).add(e.getKey());
		}
		return finalResult;
    }
    
    private String cleanTuple(String [] splitedTuple, List<String> columns) {
    	StringBuilder cleanTuple = new StringBuilder(); 
    		
    	for(String c : columns) {
    		int real_pos = realColumPos(c);
    		String validColumn = splitedTuple[real_pos];
    		cleanTuple.append("|"+validColumn);
    	}
    	cleanTuple.deleteCharAt(0);
    	/*
    	for (int i=0; i<mesureMeta.size()+1;i++)
    		cleanTuple.append("|"+splitedTuple[i]);*/
    	
    	return cleanTuple.toString();
    }
    
    int cardilanityInquire(Int2ObjectMap<String> tuples, Set<String> invertedIndexKeySet,String column) {
    	Set<String> cardinality = new HashSet<>();
    	int real_pos = realColumPos(column);
    	for(Entry<Integer,String> e : tuples.entrySet()) {
    		String [] splitArr = e.getValue().split("\\|");
			String columnVal = splitArr[real_pos];
    		cardinality.add(columnVal);
    	}
    	
    	/*int posCol = mesureMeta.get(column);
    	Set<String> cardinality = invertedIndexKeySet.stream()
					                	 .filter(s -> s.startsWith("$" + posCol + "$"))
					                	 .collect(Collectors.toSet());*/
    	
    	int card = cardinality.size();
    	return card;
    }
    
    private int realColumPos(String column) {
    	int pos = dimensionMeta.get(column);
		return mesureMeta.size()+1+pos;
    }
	
    /*
	 * 1 = starts with
     * 2 = ends with
     * 3 = >
     * 4 = <
	 * 5 = =
	
	 * 1 = and
	 * 2 = or
	 */
    private FilterOperator checkOperator(int opID) {
    	switch (opID) {
		case 1:
			return new StartsWithOp();
		case 2:
			return new EndsWithOp();
		case 3:
			return new GreaterThanOp();
		case 4:
			return new LessThanOp();
		case 5:
			return new EqualsToOp();
		case 6:
			return new ContainsOp();
		
		default:
			return null;
		}
    }
    
    int hash(String s) {
    	int h = s.hashCode();
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }
 }
