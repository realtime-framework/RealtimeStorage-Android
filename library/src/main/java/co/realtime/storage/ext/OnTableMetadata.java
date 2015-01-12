package co.realtime.storage.ext;

import co.realtime.storage.entities.TableMetadata;

public interface OnTableMetadata {
	public void run(TableMetadata tableMetadata);
}
