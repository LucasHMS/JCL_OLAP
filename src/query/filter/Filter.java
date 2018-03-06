package query.filter;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import implementations.collections.JCLHashMap;
import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntCollection;
import query.filter.operators.*;

public class Filter
{
	/*
	 * 1 = starts with
     * 2 = ends with
     * 3 = >
     * 4 = <
	 * 5 = =
	
	 * 1 = and
	 * 2 = or
	 */
    
    /* Recebe 3 lists, a primeira com os nomes das colunas, a segunda com o tipo de filtro e a terceira com
     * os parametros do filtro. A minha classe irá ver se essas informações estão na hash do jcl.
     * Depois de achar a peirmeira coluna eu aplico o filtro, após a aplicacao do primeiro filtro eu irei
     * salvar em uma nova map apenas as tuplas que passaram no filtro. Quando for aplicar o segudo filtro,
     * irei verificar na map resultante da passagem do primeiro filtro. Após passar o segundo
     * filtro terei uma nova map, e caso tenha um terceiro filtro, aplicarei ele em minha map resultante
     * do segundo filtro. No fim, terei uma map com apenas os ANDs ds meus parametros.
     * Mais pra frente tambem receberei os filtros das mesures*/
    @SuppressWarnings("unchecked")
	public void filtra(List<String> coluna, List<Integer> operatorID, List<String> parameters, 
						List<Integer> intraOpFilter, int coreID)
	{   	
    	JCL_facade jcl = JCL_FacadeImpl.getInstanceLambari();
    	// map dos metadados
		Map<String, Integer> mesureMeta = JCL_FacadeImpl.GetHashMap("Mesure");
		Map<String, Integer> dimensionMeta = JCL_FacadeImpl.GetHashMap("Dimension");
		// map de tuplas
		Int2ObjectMap<String> map_core = (Int2ObjectMap<String>) jcl.getValueLocking("core_"+coreID).getCorrectResult();
		
		// map do indice invertido
		Map<String, IntCollection> jclInvertedIndex = new JCLHashMap<String, IntCollection>("invertedIndex_"+coreID);
		
		// lista para salvar as maps de resultado de cada filtro da consulta
		Int2ObjectMap<String> filterResults = new Int2ObjectOpenHashMap<>();
		
		// pega a posicao da coluna
		int posicao = dimensionMeta.get(coluna.get(0));
		// pega a posicao que esta coluna esta na minha map de tuplas
		int posicao_real = mesureMeta.size()+1+posicao;
		FilterOperator operator = checkOperator(operatorID.get(0));
		for(Entry<Integer, String> e : map_core.entrySet()){
			// caso a tupla já tenha sido adicionada ao resultado, nao é preciso continuar
			if(filterResults.containsKey(e.getKey()))
				continue;
			
			String [] splitArr = e.getValue().split("\\|");
			// verifica apenas os values da coluna que foi passada
			String d = splitArr[posicao_real];
			// verifica se o valor da coluna nessa tupla passa pelo filtro
			boolean x = operator.op(d, parameters.get(0)); 
			if(x == true){	
				// caso passe pelo filtro, adiciona todas as tuplas com esse valor a map de resultado
				IntCollection allIds = jclInvertedIndex.get(d);
				for(int id : allIds) {
					filterResults.put(id, map_core.get(id));
				}
			}
		}
		
		for(int i=1; i<coluna.size(); i++) {
			Int2ObjectMap<String> aux = new Int2ObjectOpenHashMap<String>();
			// pega a posicao da coluna
			posicao = dimensionMeta.get(coluna.get(i));
			// pega a posicao que esta coluna esta na minha map de tuplas
			posicao_real = mesureMeta.size()+1+posicao;
			FilterOperator filtOp = checkOperator(operatorID.get(i));
			// percorre map com as tuplas do resultado parcial
			for(Entry<Integer, String> e : filterResults.entrySet()){
				// caso a tupla já tenha sido adicionada ao resultado, nao é preciso continuar
				if(aux.containsKey(e.getKey()))
					continue;
				
				String [] splitArr = e.getValue().split("\\|");
				// verifica apenas os values da coluna que foi passada
				String d = splitArr[posicao_real];
				// verifica se o valor da coluna nessa tupla passa pelo filtro
				boolean x = filtOp.op(d, parameters.get(i)); 
				if(x == true){	
					// caso passe pelo filtro, adiciona todas as tuplas com esse valor a map de resultado
					IntCollection allIds = jclInvertedIndex.get(d);
					for(int id : allIds) {
						aux.put(id, map_core.get(id));
					}
				}
			}
			filterResults.clear();
			filterResults = null;
			filterResults = new Int2ObjectOpenHashMap<>(aux);
		}
		jcl.instantiateGlobalVar("filter_core_"+coreID, filterResults.get(0));
    }
       
    public FilterOperator checkOperator(int opID) {
    	switch (opID) {
		case 1:
			return new StartsWithOp();
		case 2:
			return new EndsWithOp();
		case 3:
			return new LessThanOp();
		case 4:
			return new GreaterThanOp();
		case 5:
			return new EqualsToOp();
		
		default:
			return null;
		}
    }
 }
