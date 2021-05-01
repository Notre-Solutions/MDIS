//package notre.ingestion;
//
//import com.google.cloud.bigtable.data.v2.BigtableDataClient;
//import com.google.cloud.bigtable.data.v2.models.RowMutation;
//import com.google.protobuf.ByteString;
//
//public class WriteSimple {
//    private static final String COLUMN_FAMILY_NAME = "stats_summary";
//
//    public static void writeSimple(String projectId, String instanceId, String tableId) {
//        // String projectId = "my-project-id";
//        // String instanceId = "my-instance-id";
//        // String tableId = "mobile-time-series";
//
//        try (BigtableDataClient dataClient = BigtableDataClient.create(projectId, instanceId)) {
//            long timestamp = System.currentTimeMillis() * 1000;
//
//            String rowkey = "phone#4c410523#20190501";
//
//            RowMutation rowMutation =
//                    RowMutation.create(tableId, rowkey)
//                            .setCell(
//                                    COLUMN_FAMILY_NAME,
//                                    ByteString.copyFrom("connected_cell".getBytes()),
//                                    timestamp,
//                                    1)
//                            .setCell(
//                                    COLUMN_FAMILY_NAME,
//                                    ByteString.copyFrom("connected_wifi".getBytes()),
//                                    timestamp,
//                                    1)
//                            .setCell(COLUMN_FAMILY_NAME, "os_build", timestamp, "PQ2A.190405.003");
//
//            dataClient.mutateRow(rowMutation);
//            System.out.printf("Successfully wrote row %s", rowkey);
//
//        } catch (Exception e) {
//            System.out.println("Error during WriteSimple: \n" + e.toString());
//        }
//    }
//}