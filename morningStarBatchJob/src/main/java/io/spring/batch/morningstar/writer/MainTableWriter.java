package io.spring.batch.morningstar.writer;

import io.spring.batch.morningstar.domain.Transactionxml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

import java.util.List;


public class MainTableWriter implements ItemWriter<Transactionxml> {

    private static final Logger LOG = LoggerFactory.getLogger(MainTableWriter.class);
    @Override
    public void write(List<? extends Transactionxml> items) throws Exception {
        for (Transactionxml item : items) {
            LOG.info(item.getIsin());
        }
    }
}
