package com.gpb.jdata.models.master;

import java.io.Serial;
import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *   Информация о типах данных
 */
@Data
@Entity
@Table(name = "pg_type", schema = "pg_catalog")
@AllArgsConstructor
@NoArgsConstructor
public class PGType implements Serializable {
	@Serial
	private static final long serialVersionUID = -3377965576969457305L;

	/**
	 * 	Идентификатор строки (скрытый атрибут; должен выбираться явно)
	 */
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "oid")
	private Long oid;

	/**
	 * Имя типа данных
	 */
	@Column(name = "typname")
	private String typname;

	/**
	 * 	OID пространства имён, содержащего этот тип
	 */
	@Column(name = "typnamespace")
	private Long typnamespace;

    @Override
    public String toString() {
        return String.format(
            "PGType{oid=%s, typname='%s', typnamespace=%s}", 
            oid, typname, typnamespace);
    }
}
