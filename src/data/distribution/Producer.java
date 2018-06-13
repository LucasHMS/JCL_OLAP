package data.distribution;

import java.io.IOException;

public interface Producer {
	public abstract void distributeDataBase(String fileName) throws IOException;
	public abstract void deleteDistributedBase();
}
