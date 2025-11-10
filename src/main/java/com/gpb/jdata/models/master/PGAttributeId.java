package com.gpb.jdata.models.master;

import java.io.Serializable;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PGAttributeId implements Serializable {
    private Long attrelid;
    private String attname;

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PGAttributeId that = (PGAttributeId) o;
        return Objects.equals(attrelid, that.attrelid) && Objects.equals(attname, that.attname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attrelid, attname);
    }

    @Override
    public String toString() {
        return String.format("PGAttrdefId{attrelid=%s, attnum=%s}", attrelid, attname);
    }
}
