package com.datastax.oss.driver.api.querybuilder.select;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.UsingTimeout;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class SelectUsingTest {
  final CqlDuration oneMs = CqlDuration.newInstance(0, 0, UsingTimeout.NANOS_PER_MILLI);
  final CqlDuration twoMs = CqlDuration.newInstance(0, 0, UsingTimeout.NANOS_PER_MILLI * 2);

  @Test
  public void should_generate_timeout() {
    assertThat(selectFrom("foo").all().usingTimeout(CqlDuration.newInstance(0, 0, 1000000)))
        .hasCql("SELECT * FROM foo USING TIMEOUT 1ms");
  }

  @Test
  public void should_use_single_using_timeout_if_called_multiple_times() {
    assertThat(selectFrom("foo").all().usingTimeout(oneMs).usingTimeout(twoMs))
        .hasCql("SELECT * FROM foo USING TIMEOUT 2ms");
  }

  @Test
  public void should_clear_timeout() {
    assertThat(selectFrom("foo").all().usingTimeout(oneMs).usingTimeout((CqlDuration) null))
        .hasCql("SELECT * FROM foo");
    assertThat(selectFrom("foo").all().usingTimeout(oneMs).usingTimeout((BindMarker) null))
        .hasCql("SELECT * FROM foo");
    assertThat(selectFrom("foo").all().usingTimeout(oneMs).clearTimeout())
        .hasCql("SELECT * FROM foo");
  }

  @Test
  public void should_fail_with_timeout_nanoseconds() {
    final CqlDuration nanosecondTimeout =
        CqlDuration.newInstance(0, 0, UsingTimeout.NANOS_PER_MILLI + 1);
    Throwable t = catchThrowable(() -> selectFrom("foo").all().usingTimeout(nanosecondTimeout));
    assertThat(t)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("timeout value must be a positive multiple of a millisecond");
  }

  @Test
  public void should_fail_with_timeout_negative() {
    final CqlDuration negativeTimeout =
        CqlDuration.newInstance(0, 0, TimeUnit.MILLISECONDS.toNanos(-1));
    Throwable t = catchThrowable(() -> selectFrom("foo").all().usingTimeout(negativeTimeout));
    assertThat(t).hasMessage("timeout value must be non-negative");
  }
}
