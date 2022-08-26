package com.datastax.oss.driver.api.querybuilder.select;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import org.junit.Test;

public class SelectBypassCacheTest {
  @Test
  public void should_generate_bypass_cache() {
    assertThat(selectFrom("foo").all().bypassCache()).hasCql("SELECT * FROM foo BYPASS CACHE");
  }

  @Test
  public void should_use_single_bypass_cache_if_called_multiple_times() {
    assertThat(selectFrom("foo").all().bypassCache().bypassCache())
        .hasCql("SELECT * FROM foo BYPASS CACHE");
  }
}
