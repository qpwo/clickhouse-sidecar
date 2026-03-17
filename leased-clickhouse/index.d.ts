import { ClickHouseClient, ClickHouseClientConfigOptions } from '@clickhouse/client';

export interface LeasedClickHouseOptions {
    dataDir?: string;
    clientOptions?: Partial<ClickHouseClientConfigOptions>;
}

export declare function getClient(options?: LeasedClickHouseOptions): Promise<ClickHouseClient>;

export * from '@clickhouse/client';
