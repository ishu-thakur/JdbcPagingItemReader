package com.springbatchexample.component;

import com.springbatchexample.entity.Loan;
import org.springframework.batch.item.ItemProcessor;

public class LoanItemProcessor implements ItemProcessor<Loan, Loan> {

    @Override
    public Loan process(Loan loan) throws Exception {
        return loan;
    }
}
