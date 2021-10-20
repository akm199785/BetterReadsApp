package com.springboot.betterreads.author;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

@Repository
@Service
public interface AuthorRepository extends CassandraRepository<Author,String> {
}
