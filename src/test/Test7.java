package test;

import static query.IntegerBaseQueryDriver.VERBOSITY;

import java.io.FileReader;
import java.io.FileWriter;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import data.distribution.IntegerBaseInvariantProducer;
import data.distribution.Producer;
import index.Index;
import query.IntegerBaseQueryDriver;

public class Test7 {
	@SuppressWarnings("unchecked")
	public static void main(String [] args){
		try {
			String json_file = "testes";
			JSONParser parser = new JSONParser();
			Object obj = parser.parse(new FileReader(json_file+".json"));
			JSONObject jsonObject = (JSONObject) obj;
			JSONArray tests = (JSONArray) jsonObject.get("tests");
			JSONObject base_size = (JSONObject) tests.get(0); // TROCAR TAMANHO DA BASE
			JSONArray runs = (JSONArray) base_size.get("runs");
			JSONObject curr_run = (JSONObject) runs.get(0); //{"load_metadata":5.0,"indexing":0.0,"create_cube":0.0,"filtering":0.0,"distribute_base":0.0,"aggregate":0.0}

			VERBOSITY = false;
			int size = 5000;
			boolean saveToFile = false;
			Producer p = new IntegerBaseInvariantProducer(size,saveToFile);
			long t1 = System.currentTimeMillis();
			p.distributeDataBase("input/plano/teste1/Base-4000000.data");
			long t2 = System.currentTimeMillis();
			System.out.println("(CRIAR ARQUIVOS "+ saveToFile +") Tempo gasto com " + size + ": " + ((t2-t1)*1.0/1000) + "s");
			curr_run.put("distribute_base", ((t2-t1)*1.0/1000));
			
			Index i = new Index();
			t1 = System.currentTimeMillis();
			i.loadMetadata("input/plano/teste1/");
			t2 = System.currentTimeMillis();
			System.out.println("(CARREGAR METADADOS): " + ((t2-t1)*1.0/1000) + "s");
			curr_run.put("load_metadata", ((t2-t1)*1.0/1000));
			
			t1 = System.currentTimeMillis();
			i.createIntegerIndex();
			t2 = System.currentTimeMillis();
			System.out.println("(CRIAR INDICES): " + ((t2-t1)*1.0/1000) + "s");
			curr_run.put("indexing", ((t2-t1)*1.0/1000));
			
			IntegerBaseQueryDriver qd = new IntegerBaseQueryDriver();
			qd.readAndParse("A > '600' and B > '650' and C > '800' and D INQUIRE and E INQUIRE; max M1");
			t1 = System.currentTimeMillis();
			qd.filterQuery();
			t2 = System.currentTimeMillis();
			System.out.println("(FILTRAGEM): " + ((t2-t1)*1.0/1000) + "s");
			curr_run.put("filtering", ((t2-t1)*1.0/1000));
			
			t1 = System.currentTimeMillis();
			qd.hybridCubeCreation();
			t2 = System.currentTimeMillis();
			System.out.println("(CRIAR CUBO): " + ((t2-t1)*1.0/1000) + "s");
			curr_run.put("create_cube", ((t2-t1)*1.0/1000));
		
			// VERBOSITY = true;
			t1 = System.currentTimeMillis();
			qd.hybridSubCubeAggregation();
			t2 = System.currentTimeMillis();
			System.out.println("(AGREGAÇÃO): " + ((t2-t1)*1.0/1000) + "s");
			curr_run.put("aggregate", ((t2-t1)*1.0/1000));
			
			//i.deleteIndices(qd.getQueryElements().getColumnList().size());
			//qd.jclDestroy();
			
			FileWriter file = new FileWriter(json_file+".json");
			file.write(jsonObject.toJSONString(1));
            file.flush();
            file.close();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
