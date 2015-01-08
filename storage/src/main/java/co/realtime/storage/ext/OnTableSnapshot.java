package co.realtime.storage.ext;

import co.realtime.storage.TableSnapshot;

public interface OnTableSnapshot {
	public void run(TableSnapshot tableSnapshot);
}
