package metrics_influxdb.api.protocols;

import java.util.concurrent.TimeUnit;

public class HttpInfluxdbProtocol implements InfluxdbProtocol {
    public final static String DEFAULT_HOST = "127.0.0.1";
    public final static int DEFAULT_PORT = 8086;
    public final static String DEFAULT_DATABASE = "metrics";
    public final static TimeUnit DEFAULT_TIME_PRECISION = TimeUnit.MILLISECONDS;
    private final String user;
    private final String password;
    private final String host; 
    private final int port;
    private final boolean secured;
    private final String database;
    private final String timePrecision;

    public static String toTimePrecision(TimeUnit t) {
        switch (t) {
            case SECONDS:
                return "s";
            case MILLISECONDS:
                return "ms";
            case MICROSECONDS:
                return "u";
            default:
                throw new IllegalArgumentException("time precision should be SECONDS or MILLISECONDS or MICROSECONDS");
        }
    }

    public HttpInfluxdbProtocol(String host, int port, String user, String password, String db) {
        this(host, port, user, password, db, DEFAULT_TIME_PRECISION);
    }
    
    public HttpInfluxdbProtocol(String host, int port, String user, String password, String db, TimeUnit precision) {
        super();
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.database = db;
        this.secured = (user != null) && (password != null);
        this.timePrecision = toTimePrecision(precision);
    }
    
    public HttpInfluxdbProtocol(String host) {
        this(host, DEFAULT_PORT);
    }
    
    public HttpInfluxdbProtocol(String host, int port) {
        this(host, port, null, null);
    }
    
    public HttpInfluxdbProtocol(String host, int port, String database) {
        this(host, port, null, null, database, DEFAULT_TIME_PRECISION);
    }
    
    public HttpInfluxdbProtocol() {
        this(DEFAULT_HOST, DEFAULT_PORT);
    }
    
    public HttpInfluxdbProtocol(String host, int port, String user, String password) {
        this(host, port, user, password, DEFAULT_DATABASE, DEFAULT_TIME_PRECISION);
    }
    
    public String getUser() {
        return user;
    }
    public String getPassword() {
        return password;
    }
    public String getHost() {
        return host;
    }
    public int getPort() {
        return port;
    }
    public boolean isSecured() {
        return secured;
    }

    public String getDatabase() {
        return database;
    }

    public String getTimePrecision() {
        return timePrecision;
    }
}
