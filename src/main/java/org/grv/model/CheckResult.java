package org.grv.model;

public record CheckResult(String checkName, boolean passed, DeclineReason declineReason) {
}
