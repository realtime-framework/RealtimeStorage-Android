package co.realtime.storage.ext;

import co.realtime.storage.StorageRef;

public interface OnReconnected {
	public void run(StorageRef sender);
}
