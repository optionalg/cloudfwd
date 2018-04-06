package com.splunk.cloudfwd.impl.sim.errorgen.cookies;

import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksEventPost;

public class SyncAckedEndpoint extends UpdateableCookieEndpoints {
  public SyncAckedEndpoint() {
    super();
    UpdateableCookieEndpoints.setSyncAck(HttpCallbacksEventPost.ACK_HEADER_SYNC_VALUE);
  }
}
