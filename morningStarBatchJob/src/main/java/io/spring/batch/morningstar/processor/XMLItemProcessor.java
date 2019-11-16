package io.spring.batch.morningstar.processor;

import io.spring.batch.morningstar.domain.Transaction;
import io.spring.batch.morningstar.domain.Transactionxml;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;

public class XMLItemProcessor implements ItemProcessor<Transaction, Transactionxml> {

    @Override
    public Transactionxml process(Transaction transaction) throws Exception {
        Transactionxml transactionxml = new Transactionxml();
        transactionxml.setIsin(transaction.getAccount());
        Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
        marshaller.setClassesToBeBound(Transaction.class);
        StringWriter sw = new StringWriter();
        Result result = new StreamResult(sw);
        marshaller.marshal(transaction, result);
        transactionxml.setRawData(sw.toString());
        return transactionxml;
    }

}


