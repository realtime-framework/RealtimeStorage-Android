package co.realtime.storage.ext;

import co.realtime.storage.StorageRef;

public interface OnConnected {
    public void run(StorageRef sender);
}
