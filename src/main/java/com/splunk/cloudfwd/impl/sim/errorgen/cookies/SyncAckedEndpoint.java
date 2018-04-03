package com.splunk.cloudfwd.impl.sim.errorgen.cookies;

        import com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints;

public class SyncAckedEndpoint extends UpdateableCookieEndpoints {
  public SyncAckedEndpoint() {
    super();
    UpdateableCookieEndpoints.setSyncAck("sync");
  }
}
