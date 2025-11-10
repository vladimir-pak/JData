package com.gpb.jdata.models.master;

import java.io.Serializable;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PGAttrdefId implements Serializable {

    private Long adrelid;
    private Long adnum;

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PGAttrdefId that = (PGAttrdefId) o;
        return Objects.equals(adrelid, that.adrelid) && Objects.equals(adnum, that.adnum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(adrelid, adnum);
    }

    @Override
    public String toString() {
        return String.format("PGAttrdefId{attrelid=%s, attnum=%s}", adrelid, adnum);
    }
    
}
