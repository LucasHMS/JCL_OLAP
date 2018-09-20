package cube.gpu;

import com.aparapi.Kernel;

public class CubeKernel  extends Kernel {
	
	private int LINES;			 // quantidade de tuplas
	private int COLS;			 // quantidade de dimensoes	
	private int N_GEN_TUPLES;	 // quantidade de novas tupalas geradas para cada tupla original (2^COLS)
	private int MAX_SIZE; 		 // quantidade total de tuplas geradas com todas agregacoes (N_GEN_TUPLES*LINES)
	private int HASH_SIZE;		 // tamanho maximo da hashTable, baseado na cardinalidade das colunas e na funca de hash
	private int HASH_CONSTANT;	 // constante usada para calcular a hash de uma tupla
	private int [] arr;			 // array com as tuplas originais
	private int [] result;		 // array para as tuplas geradas 
	private int [] hashTable;	 // tabela (esparsa) com as tuplas unicas 

	public CubeKernel(int LINES, int COLS, int [] arr, int [] cardinalities) {
		this.LINES = LINES;
		this.COLS = COLS;
		this.arr = arr;

		N_GEN_TUPLES = (int) Math.pow(2, COLS);
		MAX_SIZE = N_GEN_TUPLES*this.LINES;
		result = new int[MAX_SIZE*COLS];
		
		HASH_CONSTANT = 3;
		HASH_SIZE = 0;
		for (int i=0; i<this.COLS; i++) {
			HASH_SIZE = (HASH_CONSTANT * HASH_SIZE + ( cardinalities[i] * (i+1) + HASH_CONSTANT ) );
        }
		HASH_SIZE -= (LINES-1); // nao e necessario alocar multiplos espacos na tabela para as tuplas ALL ALL ... ALL 
		
		hashTable = new int [HASH_SIZE*COLS];
	}
	
	@Override
	public void run() {
		int gId = getGlobalId();
		if (gId < LINES){
			aggregate(gId);
		}else {
			gId -= LINES;
			reduce(gId);
		}
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
	
	public void reduce(int gId) {
		for (int i=0; i<N_GEN_TUPLES; i++) {
			int line = N_GEN_TUPLES*gId+i;
			int key = hashTuple(line);
			putTuple(key, line);
		}
	}
	
	private void generateAggregation(int lineArr1, int lineArr2, int col) {
		for(int i=0; i<COLS; i++) {
			if (i == col) {
				result[lineArr1*COLS+i] = -1;
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

	private int hashTuple(int line) {
    	int res = 0;
    	for (int j=0; j<COLS; j++) {
            res = (HASH_CONSTANT * res + ( result[line*COLS+j] * (j+1) + HASH_CONSTANT ) );
        }
    	
    	return res;
    }
	
	private void putTuple(int key, int line) {
		if (hashTable[key*COLS] != 0) return;
		for (int i=0; i<COLS; i++) {
			hashTable[key*COLS+i] = result[line*COLS+i];
		}
	}
	
	public int [] getTable() {
		return hashTable;
	}
	
	public void printResult() {
		for(int i=0; i<MAX_SIZE;i++) {
			for(int j=0; j<COLS; j++) {
				System.out.print(result[i*COLS+j]+ " ");
			}
			System.out.println();
		}
		System.out.println("size: " + result.length/COLS);
	}
	
	public void printHashTable() {
		int cont = 0;
		for(int i=0; i<HASH_SIZE; i++) {
			if(hashTable[i*COLS+0] == 0) continue;
			cont++;
			System.out.print(i + "-> ");
			for(int j=0; j<COLS; j++) {
				System.out.print(hashTable[i*COLS+j] + " ");
			}
			System.out.println();
		}
		System.out.println("size: " + cont);
	}
}
