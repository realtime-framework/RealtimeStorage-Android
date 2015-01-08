package co.realtime.storage.ext;

import ibt.ortc.api.Presence;

public interface OnPresence {
	public void run(Presence presence);
}
