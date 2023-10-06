package tech.stackable.gis.hbase.coprocessor;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import tech.stackable.gis.hbase.generated.KNN;

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

        Map<byte[], List<KNN.Point>> results = table.coprocessorService(
                KNN.KNNService.class,
                null,  // start key
                null,  // end   key
                aggregate -> {
                    BlockingRpcCallback<KNN.KNNResponse> rpcCallback = new BlockingRpcCallback<>();
                    aggregate.getKNN(null, request, rpcCallback);
                    KNN.KNNResponse response = rpcCallback.get();

                    return response.getPointsList();
                }
        );

        for (List<KNN.Point> regionKNN : results.values()) {
            System.out.println("region KNN size = " + regionKNN.size());
        }
    }
}