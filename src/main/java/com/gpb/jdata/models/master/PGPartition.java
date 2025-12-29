package com.gpb.jdata.models.master;

import java.io.Serial;
import java.io.Serializable;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "pg_partition", schema = "pg_catalog")
@AllArgsConstructor
@NoArgsConstructor
public class PGPartition implements Serializable {
    @Serial
    private static final Long serialVersionUID = -1382412189378913121L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long oid;

    private Long parrelid;

    private Integer parnatts;

    /**
	 * Метод партиционирования:
	 * h = hash, r = range, l = list
	 **/
    private String parkind;

    private int[] paratts;

    private Integer parlevel;

    private Boolean paristemplate;

    @Override
    public String toString() {
        return String.format(
            "PGPartition{oid=%s, parrelid=%s"
            + ", parnatts=%s, parkind='%s', paratts=%s}", 
            oid, parrelid, parnatts, parkind, paratts);
    }
}
