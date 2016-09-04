package hoolee.sort;

import java.util.ArrayList;
import java.util.Map;

public interface Sampler {
	public void doSample() throws Exception;
	public long getLinesReadCount();
	public long getLinesSampedCount();
	public Map<String, ArrayList<String>> getKeysValuesMap();
}
