package com.school.selfcheckout.service.checkout;

/** The relay's durable position: the current epoch, and the last outbox seq published under it. */
public record RelayState(long epoch, long publishedSeq) {
}
