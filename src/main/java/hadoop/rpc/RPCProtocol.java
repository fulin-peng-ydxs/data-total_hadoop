package hadoop.rpc;

public interface RPCProtocol {
    long versionID = 666;

    void mkdirs(String path);
}