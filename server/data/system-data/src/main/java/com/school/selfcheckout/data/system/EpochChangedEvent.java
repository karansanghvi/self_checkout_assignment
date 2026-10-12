package com.school.selfcheckout.data.system;

/**
 * Published inside a service when {@link EpochWatcher} sees that another
 * service -- in practice admin-service -- has reset the database.
 */
public record EpochChangedEvent(long previousEpoch, long newEpoch) {
}
