package co.realtime.storage.ext;

import co.realtime.storage.StorageRef;

public interface OnReconnecting {
	public void run(StorageRef sender);
}
