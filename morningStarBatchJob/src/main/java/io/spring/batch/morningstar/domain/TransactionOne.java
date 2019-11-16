package io.spring.batch.morningstar.domain;

import org.springframework.stereotype.Repository;

@Repository
public class TransactionOne {

  public String getAccount() {
    return account;
  }

  public void setAccount(String account) {
    this.account = account;
  }

  private String account;

}
