package com.gpb.jdata.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.gpb.jdata.models.replication.UserEntity;

public interface UserRepository extends JpaRepository<UserEntity, Long> {
    Optional<UserEntity> findByUsername(String username);
}
