package nettybufrdf;

public class ByteBufRDFException extends Exception {

	private static final long serialVersionUID = 1930482588390397812L;

	public ByteBufRDFException() {
		super();
	}

	public ByteBufRDFException(String msg) {
		super(msg);
	}

	public ByteBufRDFException(Throwable t) {
		super(t);
	}

	public ByteBufRDFException(String msg, Throwable t) {
		super(msg, t);
	}
}
