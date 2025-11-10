package com.gpb.jdata.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.gpb.jdata.models.master.TransactionsJournal;

@Repository
public interface TransactionsJournalRepository
        extends JpaRepository<TransactionsJournal, Long> {

    @Query(value = "SELECT id, request, flag, response, message, db, timestamp FROM public.transactions_journal"
			+ " WHERE request = :request ORDER BY timestamp DESC LIMIT 1", nativeQuery = true)
	public TransactionsJournal getJournalEntry(@Param("request") String request);

}
