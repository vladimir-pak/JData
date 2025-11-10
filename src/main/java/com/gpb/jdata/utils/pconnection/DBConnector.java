package com.gpb.jdata.utils.pconnection;

import jakarta.persistence.Column;
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
@Table(name="db_connectors")
@AllArgsConstructor
@NoArgsConstructor
public class DBConnector {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    @Column(name = "id")
    private Long id;

    @Column(name="url")
    private String url;

    @Column(name="username")
    private String username;

    @Column(name="password")
    private String password;

    @Column(name="updatedat")
    private Long updatedat;

    @Column(name="deleted")
    private String deleted;

    @Column(name="name")
    private String name;

    @Column(name="type")
    private String type;
    
}
