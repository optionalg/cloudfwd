
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.logging.HttpEventCollectorEventInfo;
import com.splunk.logging.HttpEventCollectorMiddleware;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/*
 * Proprietary and confidential. Copyright Splunk 2017
 */
/**
 *
 * @author ghendrey
 */
public class AckExtractorMiddleware extends HttpEventCollectorMiddleware.HttpSenderMiddleware {
	private ObjectMapper mapper = new ObjectMapper();

	@Override
	public void postEvents(
			final List<HttpEventCollectorEventInfo> events,
			HttpEventCollectorMiddleware.IHttpSender sender,
			HttpEventCollectorMiddleware.IHttpSenderCallback callback) {
		callNext(events, sender,
				new HttpEventCollectorMiddleware.IHttpSenderCallback() {
			public void completed(int statusCode, final String reply) {
				System.out.println("middelwareOK: " + reply);
				if(statusCode == 200){
					try {				
						Map<String, Object> map = mapper.readValue(reply, new TypeReference<Map<String,Object>>(){});
						System.out.println("ACKID:" + map.get("ackId"));
					} catch (IOException ex) {
						Logger.getLogger(AckExtractorMiddleware.class.getName()).
								log(Level.SEVERE, null, ex);
					}
					
				}else{
					Logger.getLogger(AckExtractorMiddleware.class.getName()).
								log(Level.SEVERE, "server didn't return ack ids");
				}
			}

			public void failed(final Exception ex) {
				System.out.println("ooops failed");
			}
		});
	}

}
