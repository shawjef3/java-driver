package com.datastax.oss.driver.api.querybuilder.insert;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.UsingTimeout;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class InsertUsingTimestampTest {
  final CqlDuration oneMs = CqlDuration.newInstance(0, 0, UsingTimeout.NANOS_PER_MILLI);
  final CqlDuration twoMs = CqlDuration.newInstance(0, 0, UsingTimeout.NANOS_PER_MILLI * 2);

  @Test
  public void should_use_single_using_timeout_if_called_multiple_times() {
    assertThat(insertInto("foo").value("a", literal(1)).usingTimeout(oneMs).usingTimeout(twoMs))
        .hasCql("INSERT INTO foo (a) VALUES (1) USING TIMEOUT 2ms");
  }

  @Test
  public void should_clear_timeout() {
    assertThat(
            insertInto("foo")
                .value("a", literal(1))
                .usingTimeout(oneMs)
                .usingTimeout((CqlDuration) null))
        .hasCql("INSERT INTO foo (a) VALUES (1)");
    assertThat(
            insertInto("foo")
                .value("a", literal(1))
                .usingTimeout(oneMs)
                .usingTimeout((BindMarker) null))
        .hasCql("INSERT INTO foo (a) VALUES (1)");
    assertThat(insertInto("foo").value("a", literal(1)).usingTimeout(oneMs).clearTimeout())
        .hasCql("INSERT INTO foo (a) VALUES (1)");
  }

  @Test
  public void should_fail_with_timeout_nanoseconds() {
    final CqlDuration nanosecondTimeout =
        CqlDuration.newInstance(0, 0, UsingTimeout.NANOS_PER_MILLI + 1);
    Throwable t =
        catchThrowable(
            () -> insertInto("foo").value("a", literal(1)).usingTimeout(nanosecondTimeout));
    Assertions.assertThat(t)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("timeout value must be a positive multiple of a millisecond");
  }

  @Test
  public void should_fail_with_timeout_negative() {
    final CqlDuration negativeTimeout =
        CqlDuration.newInstance(0, 0, TimeUnit.MILLISECONDS.toNanos(-1));
    Throwable t =
        catchThrowable(
            () -> insertInto("foo").value("a", literal(1)).usingTimeout(negativeTimeout));
    Assertions.assertThat(t).hasMessage("timeout value must be non-negative");
  }
}
