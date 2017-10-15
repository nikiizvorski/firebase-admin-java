package com.google.firebase.database.tyrus;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.firebase.database.connection.ConnectionUtils;

import java.util.List;
import java.util.Map;

class ListenQuerySpec {

  private final List<String> path;
  private final Map<String, Object> queryParams;

  ListenQuerySpec(List<String> path, Map<String, Object> queryParams) {
    this.path = path;
    this.queryParams = ImmutableMap.copyOf(queryParams);
  }

  public List<String> getPath() {
    return path;
  }

  public Map<String, Object> getQueryParams() {
    return queryParams;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ListenQuerySpec)) {
      return false;
    }
    ListenQuerySpec that = (ListenQuerySpec) o;
    return Objects.equal(path, that.path) && Objects.equal(queryParams, that.queryParams);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(path, queryParams);
  }

  @Override
  public String toString() {
    return ConnectionUtils.pathToString(this.path) + " (params: " + queryParams + ")";
  }
}
