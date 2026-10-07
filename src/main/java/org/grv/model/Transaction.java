package org.grv.model;

import java.time.LocalDateTime;

public class Transaction {

    private Integer id;
    private Integer cardId;
    private Integer accountId;
    private Double amount;
    private String merchant;
    private LocalDateTime timeStamp;
    private String location;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getCardId() {
        return cardId;
    }

    public void setCardId(Integer cardId) {
        this.cardId = cardId;
    }

    public Integer getAccountId() {
        return accountId;
    }

    public void setAccountId(Integer accountId) {
        this.accountId = accountId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getMerchant() {
        return merchant;
    }

    public void setMerchant(String merchant) {
        this.merchant = merchant;
    }

    public LocalDateTime getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(LocalDateTime timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Transaction(Integer id, Integer cardId, Integer accountId, Double amount, String merchant, LocalDateTime timeStamp, String location) {
        this.id = id;
        this.cardId = cardId;
        this.accountId = accountId;
        this.amount = amount;
        this.merchant = merchant;
        this.timeStamp = timeStamp;
        this.location = location;
    }


    @Override
    public String toString() {
        return "Transaction{" +
                "id=" + id +
                ", cardId=" + cardId +
                ", accountId=" + accountId +
                ", amount=" + amount +
                ", merchant='" + merchant + '\'' +
                ", timeStamp=" + timeStamp +
                ", location='" + location + '\'' +
                '}';
    }
}
