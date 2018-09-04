package data.distribution;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface Producer {
	public abstract void distributeDataBase(String fileName) throws IOException, InterruptedException, ExecutionException;
	public abstract void deleteDistributedBase();
}
