package metrics_influxdb.measurements;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Clock;

import metrics_influxdb.misc.Miscellaneous;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Measure implements Measurement {
	private final static Logger LOGGER = LoggerFactory.getLogger(Measurement.class);

	private String name;
	private Map<String, String> tags;
	private Map<String, String> values;
	private long timestamp;
	private TimeUnit timePrecision;
	
	public Measure(String name) {
		this(name, (Map<String, String>) null, (Map<String, String>) null, Clock.defaultClock().getTime(),
				TimeUnit.NANOSECONDS);
	}
	
	public Measure(String name, Map<String, String> tags, Map<String, String> values, long timestamp,
				   TimeUnit precision) {
		super();
		this.name = name;
		this.tags = new HashMap<String, String>();
		this.values = new HashMap<String, String>();
		this.timestamp = timestamp;
		this.timePrecision = precision;
		
		if (tags != null) {
			this.tags.putAll(tags);
		}
		if (values != null) {
			this.values.putAll(values);
		}
	}
	
	public Measure(String name, Map<String, String> tags, long value) {
		this(name, tags, value, Clock.defaultClock().getTime());
	}
	
	public Measure(String name, Map<String, String> tags, long value, long timestamp) {
		this(name, tags, Collections.singletonMap("value", value + "i"), timestamp, TimeUnit.NANOSECONDS);
	}
	
	public Measure(String name, long value, long timestamp) {
		this(name, Collections.<String, String>emptyMap(), value, timestamp);
	}
	
	public Measure(String name, long value) {
		this(name, value, Clock.defaultClock().getTime());
	}

	public Measure(String name, Map<String, String> tags, int value) {
		this(name, tags, Long.valueOf(value), Clock.defaultClock().getTime());
	}
	
	public Measure(String name, Map<String, String> tags, int value, long timestamp) {
		this(name, tags, Long.valueOf(value), timestamp);
	}
	
	public Measure(String name, int value, long timestamp) {
		this(name, null, Long.valueOf(value), timestamp);
	}
	
	public Measure(String name, int value) {
		this(name, Long.valueOf(value), Clock.defaultClock().getTime());
	}

	public Measure(String name, Map<String, String> tags, double value) {
		this(name, tags, value, Clock.defaultClock().getTime());
	}
	
	public Measure(String name, Map<String, String> tags, double value, long timestamp) {
		this(name, tags, Collections.singletonMap("value", "" + value), timestamp, TimeUnit.NANOSECONDS);
	}
	
	public Measure(String name, double value, long timestamp) {
		this(name, Collections.<String, String>emptyMap(), value, timestamp);
	}
	
	public Measure(String name, double value) {
		this(name, value, Clock.defaultClock().getTime());
	}	

	public Measure(String name, Map<String, String> tags, float value) {
		this(name, tags, Double.valueOf(value), Clock.defaultClock().getTime());
	}
	
	public Measure(String name, Map<String, String> tags, float value, long timestamp) {
		this(name, tags, Double.valueOf(value), timestamp);
	}
	
	public Measure(String name, float value, long timestamp) {
		this(name, null, Double.valueOf(value), timestamp);
	}
	
	public Measure(String name, float value) {
		this(name, Double.valueOf(value), Clock.defaultClock().getTime());
	}	
	
	public Measure(String name, Map<String, String> tags, String value) {
		this(name, tags, value, Clock.defaultClock().getTime());
	}
	
	public Measure(String name, Map<String, String> tags, String value, long timestamp) {
		this(name, tags, Collections.singletonMap("value", asStringValue(value)), timestamp, TimeUnit.NANOSECONDS);
	}
	
	public Measure(String name, String value, long timestamp) {
		this(name, null, value, timestamp);
	}
	
	public Measure(String name, String value) {
		this(name, value, Clock.defaultClock().getTime());
	}

	public Measure(String name, Map<String, String> tags, boolean value) {
		this(name, tags, value, Clock.defaultClock().getTime());
	}
	
	public Measure(String name, Map<String, String> tags, boolean value, long timestamp) {
		this(name, tags, Collections.singletonMap("value", ""+value), timestamp, TimeUnit.NANOSECONDS);
	}
	
	public Measure(String name, boolean value, long timestamp) {
		this(name, null, value, timestamp);
	}
	
	public Measure(String name, boolean value) {
		this(name, value, Clock.defaultClock().getTime());
	}
	
	private static String asStringValue(String value) {
		return "\"" + Miscellaneous.escape(value, '"') + "\"";
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Map<String, String> getTags() {
		return tags;
	}

	@Override
	public Map<String, String> getValues() {
		return values;
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public TimeUnit getTimePrecision() {
		return timePrecision;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setTags(Map<String, String> tags) {
		this.tags.clear();
		if (tags != null) {
			this.tags.putAll(tags);
		}
	}

	public void setValues(Map<String, String> values) {
		this.values.clear();
		if (values != null) {
			this.values.putAll(values);
		}
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public Measure timePrecision(TimeUnit precision) {
		if (precision != TimeUnit.SECONDS && precision != TimeUnit.MILLISECONDS || precision != TimeUnit.MICROSECONDS) {
			LOGGER.error("time precision can be only set to SECONDS, MILLISECONDS or MICROSECONDS");
		}

		this.timePrecision = precision;
		return this;
	}
	
	public Measure timestamp(long timestamp) {

		switch (timePrecision) {
			case SECONDS:
				setTimestamp(TimeUnit.SECONDS.convert(timestamp, TimeUnit.MILLISECONDS));
				break;
			case MILLISECONDS:
				setTimestamp(TimeUnit.MILLISECONDS.convert(timestamp, TimeUnit.MILLISECONDS));
				break;
			case MICROSECONDS:
				setTimestamp(TimeUnit.MICROSECONDS.convert(timestamp, TimeUnit.MILLISECONDS));
				break;
			default:
				// Convert milliseconds timestamp to nanoseconds for influxdb version 0.9
				setTimestamp(TimeUnit.NANOSECONDS.convert(timestamp, TimeUnit.MILLISECONDS));
		}

	    return this;
	}
	
	public Measure addTag(String tagKey, String tagValue) {
		tags.put(tagKey, tagValue);
		return this;
	}
	public Measure addTag(Map<String, String> tags) {
	    this.tags.putAll(tags);
	    return this;
	}
	public Measure addValue(String key, String value) {
		values.put(key, asStringValue(value));
		return this;
	}
	public Measure addValue(String key, float value) {
		return addValue(key, Double.valueOf(value));
	}
	public Measure addValue(String key, double value) {
		values.put(key, ""+value);
		return this;
	}
	public Measure addValue(String key, int value) {
		return addValue(key, Long.valueOf(value));
	}
	public Measure addValue(String key, long value) {
		values.put(key, value+"i");
		return this;
	}
	public Measure addValue(String key, boolean value) {
		values.put(key, ""+value);
		return this;
	}
}
