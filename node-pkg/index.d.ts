import { ClickHouseClient, ClickHouseClientConfigOptions } from '@clickhouse/client';

export interface ClickHouseSidecarOptions {
    dataDir?: string;
    clientOptions?: Partial<ClickHouseClientConfigOptions>;
}

export interface ClickHouseSidecarClient extends ClickHouseClient {
    close(): Promise<void>;
    sidecarUrl: string;
    sidecarPort: number;
    streamUrl(table: string): string;
    _leaseSock: unknown;
}

export declare function getClient(options?: ClickHouseSidecarOptions): Promise<ClickHouseSidecarClient>;
export declare function runDaemon(dataDir: string): Promise<void>;

export * from '@clickhouse/client';
