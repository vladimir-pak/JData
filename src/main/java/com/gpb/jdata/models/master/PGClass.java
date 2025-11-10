package com.gpb.jdata.models.master;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigInteger;

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
@Table(name = "pg_class", schema = "pg_catalog")
@AllArgsConstructor
@NoArgsConstructor
public class PGClass implements Serializable {
    
    @Serial
    private static final Long serialVersionUID = 5201594388813947088L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long oid;

    private String relname;

    private BigInteger relnamespace;

    /**
	 * r = обычная таблица, i = индекс (index), S = последовательность (sequence), 
	 * v = представление (view), m = материализованное представление (materialized view), 
	 * c = составной тип (composite), t = таблица TOAST, f = сторонняя таблица (foreign)
	 * **/
    private String relkind;

    @Override
    public String toString() {
        return String.format(
                "PGClass{oid=%s, relname='%s', relnamespace=%s, relkind='%s'}", 
                oid, relname, relnamespace, relkind);
    }
}
