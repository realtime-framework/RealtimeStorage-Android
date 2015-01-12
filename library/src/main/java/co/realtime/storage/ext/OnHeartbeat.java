package co.realtime.storage.ext;

import co.realtime.storage.entities.Heartbeat;

public interface OnHeartbeat {
	public void run(Heartbeat heartbeat);

}
