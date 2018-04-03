package com.splunk.cloudfwd.impl.sim;

        import com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints;

public class SyncAckedEndpoint extends UpdateableCookieEndpoints {
  public SyncAckedEndpoint() {
    super();
    UpdateableCookieEndpoints.toggleSyncAck();
  }
}
