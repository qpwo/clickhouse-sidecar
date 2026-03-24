import { ClickHouseClient, ClickHouseClientConfigOptions } from '@clickhouse/client';

export interface ClickHouseSidecarOptions {
    dataDir?: string;
    clientOptions?: Partial<ClickHouseClientConfigOptions>;
}

export declare function getClient(options?: ClickHouseSidecarOptions): Promise<ClickHouseClient>;

export * from '@clickhouse/client';
