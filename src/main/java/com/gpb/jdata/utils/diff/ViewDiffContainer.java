package com.gpb.jdata.utils.diff;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.gpb.jdata.orda.dto.ViewDTO;

@Component("viewDiff")
public class ViewDiffContainer {

    private final List<ViewDTO> updated = new ArrayList<>();
    private final List<ViewDTO> deleted = new ArrayList<>();

    private final Object updatedLock = new Object();
    private final Object deletedLock = new Object();

    public void addUpdated(ViewDTO view) {
        synchronized (updatedLock) {
            updated.add(view);
        }
    }

    public void addDeleted(ViewDTO view) {
        synchronized (deletedLock) {
            deleted.add(view);
        }
    }

    public List<ViewDTO> getUpdated() {
        synchronized (updatedLock) {
            return List.copyOf(updated);
        }
    }

    public List<ViewDTO> getDeleted() {
        synchronized (deletedLock) {
            return List.copyOf(deleted);
        }
    }

    public void clear() {
        updated.clear();
        deleted.clear();
    }
}
