package com.datastax.oss.driver.api.querybuilder;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * Add support to queries for {@code USING TIMEOUT} clauses.
 *
 * @param <ReturnT> the type of the query
 */
public interface UsingTimeout<ReturnT> {
  long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);

  static void checkCqlDuration(@NonNull final CqlDuration timeout) {
    Preconditions.checkArgument(
        timeout.getMonths() > -1 && timeout.getDays() > -1 && timeout.getNanoseconds() > -1,
        "timeout value must be non-negative");
    Preconditions.checkArgument(
        timeout.getNanoseconds() % NANOS_PER_MILLI == 0,
        "timeout value must be a positive multiple of a millisecond");
  }

  static void checkTimeout(@Nullable final Object timeout) {
    final boolean isNull = timeout == null;
    final boolean isCqlDuration = timeout instanceof CqlDuration;
    Preconditions.checkArgument(
        isNull || isCqlDuration || timeout instanceof BindMarker,
        "timeout value must be a BindMarker or CqlDuration");
    if (isCqlDuration) {
      checkCqlDuration((CqlDuration) timeout);
    }
  }

  @NonNull
  ReturnT usingTimeout(@NonNull CqlDuration timeout);

  @NonNull
  ReturnT usingTimeout(@NonNull BindMarker timeout);

  @NonNull
  ReturnT clearTimeout();
}
