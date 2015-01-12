package co.realtime.storage.ext;

public interface OnTableCreation {
	public void run(String table, Double creationDate, String status);
}
