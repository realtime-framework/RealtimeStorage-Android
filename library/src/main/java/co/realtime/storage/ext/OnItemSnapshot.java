package co.realtime.storage.ext;

import co.realtime.storage.ItemSnapshot;

public interface OnItemSnapshot {
	public void run(ItemSnapshot itemSnapshot);
}
