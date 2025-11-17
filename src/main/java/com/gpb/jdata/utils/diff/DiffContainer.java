package com.gpb.jdata.utils.diff;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DiffContainer {

    private final Set<Long> updated = ConcurrentHashMap.newKeySet();
    private final Set<String> deleted = ConcurrentHashMap.newKeySet();
    private final Set<Long> deletedOids = ConcurrentHashMap.newKeySet();

    public void addUpdated(Long oid) {
        updated.add(oid);
    }

    public void addAllUpdated(Collection<Long> oids) {
        updated.addAll(oids);
    }

    public void addDeleted(String fqn) {
        deleted.add(fqn);
    }

    public void addDeletedOids(Long oid) {
        deletedOids.add(oid);
    }

    public Set<Long> getUpdated() {
        return Set.copyOf(updated); // Защитная копия
    }

    public Set<String> getDeleted() {
        return Set.copyOf(deleted); // Защитная копия
    }

    public Set<Long> getDeletedOids() {
        return Set.copyOf(deletedOids); // Защитная копия
    }

    public boolean containsInDeletedOids(Long oid) {
        return deletedOids.contains(oid);
    }

    public boolean containsInDeleted(String fqn) {
        return deleted.contains(fqn);
    }

    public void clear() {
        updated.clear();
        deleted.clear();
        deletedOids.clear();
    }

    public boolean isEmpty() {
        return updated.isEmpty() && deleted.isEmpty();
    }
}
