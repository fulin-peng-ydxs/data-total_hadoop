package hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class HDFSClient {
    public static void main(String[] args) throws IOException {
        RPCProtocol client = RPC.getProxy(
                RPCProtocol.class,
                RPCProtocol.versionID,
                new InetSocketAddress("localhost", 8888),
                new Configuration());
        System.out.println("我是客户端");
        client.mkdirs("/input");
    }
}