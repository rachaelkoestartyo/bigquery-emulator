/**
 * Test setup utilities and fixtures for BigQuery emulator tests.
 *
 * This module provides utilities for setting up BigQuery clients,
 * managing test data, and common test operations.
 */

import { BigQuery, Dataset, Table, TableSchema } from '@google-cloud/bigquery';
import {
  BigQueryEmulatorContainer,
  BQ_EMULATOR_PROJECT_ID,
} from './BigQueryEmulatorContainer.js';

export { BQ_EMULATOR_PROJECT_ID };

/**
 * Mock auth client that doesn't require credentials.
 * Used for connecting to the local emulator without authentication.
 */
class NoAuthClient {
  async getAccessToken() {
    return { token: 'mock-token' };
  }

  async getRequestHeaders() {
    return {};
  }
}

/**
 * Creates a BigQuery client configured to connect to the emulator.
 *
 * @param emulator - The running BigQuery emulator container
 * @returns Configured BigQuery client
 */
export function createBigQueryClient(
  emulator: BigQueryEmulatorContainer
): BigQuery {
  const client = new BigQuery({
    projectId: BQ_EMULATOR_PROJECT_ID,
    apiEndpoint: emulator.getConnectionUrl(),
    authClient: new NoAuthClient() as any,
  });

  return client;
}

/**
 * Represents the (dataset_id, table_id) address of a BigQuery table or view.
 */
export interface BigQueryAddress {
  datasetId: string;
  tableId: string;
}

/**
 * Helper class for common BigQuery test operations.
 */
export class BigQueryTestHelper {
  constructor(
    private client: BigQuery,
    private projectId: string = BQ_EMULATOR_PROJECT_ID
  ) {}

  /**
   * Creates a dataset if it doesn't exist.
   */
  async createDataset(datasetId: string): Promise<Dataset> {
    const [dataset] = await this.client.createDataset(datasetId, {
      location: 'US',
    });
    return dataset;
  }

  /**
   * Creates a table with the specified schema.
   */
  async createTable(
    address: BigQueryAddress,
    schema: TableSchema
  ): Promise<Table> {
    const dataset = this.client.dataset(address.datasetId);

    // Ensure dataset exists
    try {
      await dataset.get();
    } catch (error) {
      await this.createDataset(address.datasetId);
    }

    const [table] = await dataset.createTable(address.tableId, {
      schema,
    });

    return table;
  }

  /**
   * Inserts rows into a table.
   */
  async insertRows(
    address: BigQueryAddress,
    rows: Record<string, unknown>[]
  ): Promise<void> {
    const dataset = this.client.dataset(address.datasetId);
    const table = dataset.table(address.tableId);
    await table.insert(rows);
  }

  /**
   * Runs a query and returns the results.
   */
  async query<T = Record<string, unknown>>(query: string): Promise<T[]> {
    const [rows] = await this.client.query({
      query,
      location: 'US',
    });
    return rows as T[];
  }

  /**
   * Runs a query with parameters and returns the results.
   */
  async queryWithParams<T = Record<string, unknown>>(
    query: string,
    params: Record<string, unknown>
  ): Promise<T[]> {
    const [rows] = await this.client.query({
      query,
      params,
      location: 'US',
    });
    return rows as T[];
  }

  /**
   * Checks if a table exists.
   */
  async tableExists(address: BigQueryAddress): Promise<boolean> {
    try {
      const dataset = this.client.dataset(address.datasetId);
      const table = dataset.table(address.tableId);
      await table.get();
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Deletes all datasets in the project (cleanup utility).
   */
  async deleteAllDatasets(): Promise<void> {
    const [datasets] = await this.client.getDatasets();
    await Promise.all(
      datasets.map((dataset) =>
        dataset.delete({ force: true }).catch(() => {
          // Ignore errors during cleanup
        })
      )
    );
  }
}

/**
 * Creates a test helper instance.
 */
export function createTestHelper(client: BigQuery): BigQueryTestHelper {
  return new BigQueryTestHelper(client);
}