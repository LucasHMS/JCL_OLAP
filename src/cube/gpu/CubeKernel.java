package cube.gpu;

import com.aparapi.Kernel;

public class CubeKernel  extends Kernel {
	
	private int LINES;			 // quantidade de tuplas
	private int COLS;			 // quantidade de dimensoes	
	private int N_GEN_TUPLES;	 // quantidade de novas tupalas geradas para cada tupla original (2^COLS)
	private int MAX_SIZE; 		 // quantidade total de tuplas geradas com todas agregacoes (N_GEN_TUPLES*LINES)
	private int [] arr;			 // array com as tuplas originais
	private int [] result;		 // array para as tuplas geradas 
	public static int AGG_WILDCARD = 0;

	public CubeKernel(int LINES, int COLS, int [] arr) {
		this.LINES = LINES;
		this.COLS = COLS;
		this.arr = arr;

		N_GEN_TUPLES = (int) Math.pow(2, COLS);
		MAX_SIZE = N_GEN_TUPLES*this.LINES;
		result = new int[MAX_SIZE*COLS];
		
		put(this.arr).put(result);
	}
	
	@Override
	public void run() {
		int gId = getGlobalId();
		aggregate(gId);
	}
	
	public void aggregate(int gId) {
		int lastTuple = 0;
		int j = 0;
		copyLines(N_GEN_TUPLES*gId, gId);
		lastTuple++;
		for(int i=0; i<COLS; i++) {
			for(j=0; j<lastTuple; j++) {
				generateAggregation(N_GEN_TUPLES*gId+(lastTuple+j), N_GEN_TUPLES*gId+j, i);
			}
			lastTuple += j;
		}
	}
	
	private void generateAggregation(int lineArr1, int lineArr2, int col) {
		for(int i=0; i<COLS; i++) {
			if (i == col) {
				result[lineArr1*COLS+i] = AGG_WILDCARD;
			}else {
				result[lineArr1*COLS+i] = result[lineArr2*COLS+i];
			}
		}
	}
	
	private void copyLines(int lineArr1, int lineArr2) {
		for(int i=0; i<COLS; i++) {
			result[lineArr1*COLS+i] = arr[lineArr2*COLS+i];
		}
	}
	
	public int [] getResult() {
		return result;
	}
	
	public int getNGenTuples() {
		return N_GEN_TUPLES;
	}
	
	public int getMaxSize() {
		return MAX_SIZE;
	}
	
	@Override
	public String toString() {
		String s = "";
		for(int i=0; i<MAX_SIZE;i++) {
			for(int j=0; j<COLS; j++) {
				s += (result[i*COLS+j]+ " ");
			}
			s += '\n';
		}
		s += ("size: " + result.length/COLS);
		
		return s;
	}
}
