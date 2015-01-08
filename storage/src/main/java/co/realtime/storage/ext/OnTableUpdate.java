package co.realtime.storage.ext;

public interface OnTableUpdate {
	public void run(String tableName, String status);
}
