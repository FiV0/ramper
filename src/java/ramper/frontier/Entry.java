package ramper.frontier;

import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.http.cookie.Cookie;

public class Entry implements Delayed {
    /** A singleton empty cookie array. */
    public final static Cookie[] EMPTY_COOKIE_ARRAY = {};

    public String schemeAuthority;
    /** The IP address of this workbench entry, computed by {@link DnsResolver#resolve(String)}. */
    public byte[] ipAddress;

    protected volatile long nextFetch;
    /** The cookies of this entry. */
    public Cookie[] cookies;

    public Exception lasException;
    public int retries;

    /** The path+queries that must be visited for this entry. */
    private final transient ObjectArrayFIFOQueue<String> pathQueries;

    public Entry(String schemeAuthority, String [] pathQueries){
        this.schemeAuthority = schemeAuthority;
        this.nextFetch = 0;
        this.cookies = EMPTY_COOKIE_ARRAY;

        this.retries = 0;
        this.pathQueries = new ObjectArrayFIFOQueue<>();
    }

    public Entry(String schemeAuthority){
        this(schemeAuthority, new String[]{});
    }

    public boolean isEmpty(){
        return pathQueries.isEmpty();
    }

    public void addPathQuery(String pathQuery){
        pathQueries.enqueue(pathQuery);
    }

    public String getPathQuery(){
        assert !isEmpty();
        return pathQueries.dequeue();
    }

    /** Computes the size (i.e., number of URLs) in this entry.
     *
     * @return the size.
     */
    public synchronized int size() {
        return pathQueries.size();
    }

    public long nextFetch(){
        return nextFetch;
    }

    @Override
    public long getDelay(final TimeUnit unit) {
        return unit.convert(Math.max(0, nextFetch()), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(final Delayed o) {
        return Long.signum(nextFetch() - ((Entry)o).nextFetch());
    }
}
