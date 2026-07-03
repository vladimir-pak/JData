package com.gpb.jdata.utils.diff;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DiffContainer {

    private final Set<Long> updated = ConcurrentHashMap.newKeySet();
    private final Set<String> deleted = ConcurrentHashMap.newKeySet();
    private final Set<Long> deletedOids = ConcurrentHashMap.newKeySet();

    private final Map<Long, String> deletedByOid = new ConcurrentHashMap<>();

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

    public void putDeletedByOid(Long oid, String fqn) {
        deletedByOid.put(oid, fqn);
    }

    public void putAllDeletedByOid(Map<Long, String> values) {
        deletedByOid.putAll(values);
    }

    public Optional<String> getDeletedByOid(Long oid) {
        return Optional.ofNullable(deletedByOid.get(oid));
    }

    public Set<Long> getUpdated() {
        return Set.copyOf(updated);
    }

    public Set<String> getDeleted() {
        return Set.copyOf(deleted);
    }

    public Set<Long> getDeletedOids() {
        return Set.copyOf(deletedOids);
    }

    public Map<Long, String> getDeletedByOid() {
        return Map.copyOf(deletedByOid);
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
        deletedByOid.clear();
    }

    public boolean isEmpty() {
        return updated.isEmpty()
                && deleted.isEmpty()
                && deletedOids.isEmpty()
                && deletedByOid.isEmpty();
    }
}