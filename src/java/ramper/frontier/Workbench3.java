package ramper.frontier;

import java.util.concurrent.DelayQueue;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

public class Workbench3 {
    /** The map of {@linkplain Entry workbench entries}. */
    protected final Object2ObjectOpenHashMap<String,Entry> schemeAuthority2Entry;
    /** The workbench. */
    private final DelayQueue<Entry> entries;


    public Workbench3(){
        this.schemeAuthority2Entry = new Object2ObjectOpenHashMap<>();
        this.entries = new DelayQueue<>();
    }

    public Entry getEntry(String schemeAuthority){
        Entry entry;
        synchronized (schemeAuthority2Entry) {
            entry = schemeAuthority2Entry.get(schemeAuthority);
            if (entry == null) schemeAuthority2Entry.put(schemeAuthority, entry = new Entry(schemeAuthority));
        }
        return entry;
    }

    public void addEntry(Entry entry){
        entries.add(entry);
    }

    public int numberOfEntries() {
        synchronized (schemeAuthority2Entry) {
            return schemeAuthority2Entry.size();
        }
    }

    public Entry popEntry() throws InterruptedException {
        final Entry entry = entries.take();
        assert ! entry.isEmpty();
        return entry;
    }

    public boolean purgeEntry(Entry entry){
        return schemeAuthority2Entry.remove(entry.schemeAuthority, entry);
    }

    public int pathQueryLimit(){
        return 1000;
    }
}
