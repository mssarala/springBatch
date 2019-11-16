package io.spring.batch.morningstar.domain;

import org.springframework.stereotype.Repository;

import java.util.Date;

@Repository
public class TransactionTwo {

  private String amount;

  private Date timestamp;

  public String getAmount() {
    return amount;
  }

  public void setAmount(String amount) {
    this.amount = amount;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }
}
