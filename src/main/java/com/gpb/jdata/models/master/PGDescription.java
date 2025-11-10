package com.gpb.jdata.models.master;

import java.io.Serial;
import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "pg_description", schema = "pg_catalog")
@AllArgsConstructor
@NoArgsConstructor
public class PGDescription implements Serializable {
    @Serial
    private static final Long serialVersionUID = 5625969424342967545L;

    @EmbeddedId
    private PGDescriptionId id;

    @Column(name = "description", length = 10000)
    private String description;

    @Override
    public String toString() {
        return String.format(
            "PGDescription{id=%s, description='%s'}", id, description);
    }
}
