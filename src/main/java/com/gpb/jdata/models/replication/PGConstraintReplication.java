package com.gpb.jdata.models.replication;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Table(name = "pg_constraint_rep", schema = "jdata")
@AllArgsConstructor
@NoArgsConstructor
public class PGConstraintReplication implements Serializable {
	private static final long serialVersionUID = -6098254841657665918L;

	/**
	 * Идентификатор строки
	 * **/
	@Id
	@Column(name = "oid")
	private Long oid;

	/**
	 * Имя ограничения
	 */
	@Column(name = "conname")
	private String conname;

	/**
	 * OID пространства имён, содержащего это ограничение
	 */
	@Column(name = "connamespace")
	private Long connamespace;

	/**
	 * c = ограничение-проверка (check), f = внешний ключ (foreign key), p = первичный ключ (primary key), u = ограничение уникальности (unique), t = триггер ограничения (trigger), x = ограничение-исключение (exclusion)
	 */
	@Column(name = "contype")
	private String contype;

	/**
	 * Таблица, для которой установлено это ограничение; 0, если это не ограничение таблицы
	 */
	@Column(name = "conrelid")
	private Long conrelid;

	/**
	 * Если это внешний ключ, таблица, на которую он ссылается; иначе 0
	 */
	@Column(name = "confrelid")
	private Long confrelid;

	/**
	 * Домен, к которому относится это ограничение; 0, если это не ограничение домена
	 */
	@Column(name = "contypid")
	private Long contypid;

	/**
	 * Для ограничений таблицы (включая внешние ключи, но не триггеры ограничений), определяет список столбцов, образующих ограничение
	 */
	@Column(name = "conkey")
	private List<Integer> conkey;

	/**
	 * Для внешнего ключа определяет список столбцов, на которые он ссылается
	 */
	@Column(name = "confkey")
	private List<Integer> confkey;

	@Column(name = "db")
	private String db;

    @Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PGConstraintReplication that = (PGConstraintReplication) o;
		return Objects.equals(oid, that.oid) && Objects.equals(conname, that.conname) && Objects.equals(connamespace, that.connamespace) && Objects.equals(contype, that.contype) && Objects.equals(conrelid, that.conrelid) && Objects.equals(confrelid, that.confrelid) && Objects.equals(contypid, that.contypid) && Objects.equals(conkey, that.conkey) && Objects.equals(confkey, that.confkey) && Objects.equals(db, that.db);
	}

	@Override
	public int hashCode() {
		return Objects.hash(oid, conname, connamespace, contype, conrelid, confrelid, contypid, conkey, confkey, db);
	}

    @Override
    public String toString() {
        return String.format(
            "PGConstraintReplication{oid=%s"
            + ", conname='%s'"
            + ", connamespace=%s"
            + ", contype='%s'"
            + ", conrelid=%s"
            + ", confrelid=%s"
            + ", contypid=%s"
            + ", conkey=%s"
            + ", confkey=%s"
            + ", db='%s'}", 
            oid,
            conname,
            connamespace,
            contype,
            conrelid,
            confrelid,
            contypid,
            conkey,
            confkey,
            db);
    }
}
