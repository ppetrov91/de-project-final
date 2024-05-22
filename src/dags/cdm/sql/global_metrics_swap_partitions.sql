SELECT swap_partitions_between_tables (
    'STV202311139__DWH.global_metrics_copy', 
    :dt1,
    :dt1,
    'STV202311139__DWH.global_metrics'
);
