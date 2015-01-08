package co.realtime.storage.ext;

public interface OnError {
	public void run(Integer code, String errorMessage);
}
