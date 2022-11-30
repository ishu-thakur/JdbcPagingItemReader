package com.springbatchexample.component;

import com.springbatchexample.entity.Loan;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;

@Component
public class LoanResultRowMapper implements RowMapper<Loan> {
    @Override
    public Loan mapRow(ResultSet rs, int i) throws SQLException {
        Loan student = new Loan();
        student.setId(rs.getString("id"));
        student.setContactId(rs.getString("contact_id"));
        student.setCreatedAt(rs.getString("created_at"));
        return student;
    }
}
