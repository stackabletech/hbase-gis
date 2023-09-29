package tech.stackable.gis.hbase.coprocessor;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import tech.stackable.gis.hbase.generated.KNN;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Map;

public class KNNClient {
    public static void main(String[] args) throws Throwable {
        // TODO: make these cli args
        var tableArg = "wifi";
        var familyArg = "a";
        var latColArg = "lat";
        var lonColArg = "lon";
        var kArg = 5;
        var latArg = -73.97000655;
        var lonArg = 40.76098703;

        final Configuration conf = HBaseConfiguration.create();
        HBaseAdmin.available(conf);
        final Connection connection = ConnectionFactory.createConnection(conf);
        final TableName tableName = TableName.valueOf(tableArg);
        final Table table = connection.getTable(tableName);

        final KNN.KNNRequest request = KNN.KNNRequest.newBuilder()
                .setCount(kArg)
                .setLat(latArg)
                .setLon(lonArg)
                .setFamily(ByteString.copyFrom(familyArg.getBytes()))
                .setLatCol(ByteString.copyFrom(latColArg.getBytes()))
                .setLonCol(ByteString.copyFrom(lonColArg.getBytes()))
                .build();

        Map<byte[], List<ByteString>> results = table.coprocessorService(
                KNN.KNNService.class,
                null,  // start key
                null,  // end   key
                new Batch.Call<KNN.KNNService, List<ByteString>>() {
                    @Override
                    public List<ByteString> call(KNN.KNNService aggregate) throws IOException {
                        BlockingRpcCallback<KNN.KNNResponse> rpcCallback = new BlockingRpcCallback<>();
                        aggregate.getKNN((RpcController) null, request, rpcCallback);
                        KNN.KNNResponse response = rpcCallback.get();

                        return response.getKeysList();
                    }
                }
        );

        for (List<ByteString> regionKNN : results.values()) {
            System.out.println("region KNN size = " + regionKNN.size());
        }
    }
}

// TODO: switch to async maybe?
class BlockingRpcCallback<R> implements RpcCallback<R> {
    private R result;
    private boolean resultSet = false;

    /**
     * Called on completion of the RPC call with the response object, or {@code null} in the case of
     * an error.
     *
     * @param parameter the response object or {@code null} if an error occurred
     */
    @Override
    public void run(R parameter) {
        synchronized (this) {
            result = parameter;
            resultSet = true;
            this.notifyAll();
        }
    }

    /**
     * Returns the parameter passed to {@link #run(Object)} or {@code null} if a null value was
     * passed.  When used asynchronously, this method will block until the {@link #run(Object)}
     * method has been called.
     *
     * @return the response object or {@code null} if no response was passed
     */
    public synchronized R get() throws IOException {
        while (!resultSet) {
            try {
                this.wait();
            } catch (InterruptedException ie) {
                InterruptedIOException exception = new InterruptedIOException(ie.getMessage());
                exception.initCause(ie);
                throw exception;
            }
        }
        return result;
    }
}