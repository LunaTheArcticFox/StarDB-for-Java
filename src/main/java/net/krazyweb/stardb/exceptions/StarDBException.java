package net.krazyweb.stardb.exceptions;

public class StarDBException extends Exception {

	private static final long serialVersionUID = -279054858068315863L;
	
	public StarDBException(final String message) {
		super(message);
	}
	
	public StarDBException(final String message, final Exception e) {
		super(message, e);
	}

}
