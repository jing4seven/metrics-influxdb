package metrics_influxdb.measurements;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface Measurement {
	public String getName();
	public Map<String, String> getTags();
	public Map<String, String> getValues();
	public long getTimestamp();
	public TimeUnit getTimePrecision();
}
