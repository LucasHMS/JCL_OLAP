package query.filter;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
	private Object2ObjectMap<String, IntCollection> jclInvertedIndex;
	
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
		jclInvertedIndex =  (Object2ObjectMap<String, IntCollection>) jcl.getValue(machineID+"_invertedIndex_"+coreID).getCorrectResult();

		// lista para salvar as maps de resultado de cada filtro da consulta
		Int2ObjectMap<String> filterResults = new Int2ObjectOpenHashMap<>();
		filterResults = runFilter(map_core, columns.get(0), operators.get(0), args.get(0));
		
		for (int i=0; i<columns.size(); i++) {
			Int2ObjectMap<String> aux = new Int2ObjectOpenHashMap<String>();
			aux = runFilter(filterResults, columns.get(i), operators.get(i), args.get(i));
			
			filterResults.clear();
			filterResults = null;
			filterResults = new Int2ObjectOpenHashMap<>(aux);
		}
		
		Object2ObjectMap<String, IntCollection> cleanResult = generateReult(filterResults, columns);
		
		jcl.deleteGlobalVar(machineID+"_core_"+coreID);
		map_core = null;
		jcl.instantiateGlobalVar(machineID+"_filter_core_"+coreID, cleanResult);
		System.out.println("Finalizou a Filtragem. core " + coreID);
    }
    
    private Int2ObjectMap<String> runFilter(Int2ObjectMap<String> tuples, String column, int opID, String arg) {
    	Int2ObjectMap<String> filterResult = new Int2ObjectOpenHashMap<>();
    	FilterOperator filtOp = checkOperator(opID);
    	int real_pos = realColumPos(column);
		for(Entry<Integer, String> e : tuples.entrySet()){
			String [] splitArr = e.getValue().split("\\|");
			String columnVal = splitArr[real_pos];
			boolean result = filtOp.op(columnVal, arg);
			if (result)
				filterResult.put(e.getKey(), e.getValue());

		}
		return filterResult;
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
 }
