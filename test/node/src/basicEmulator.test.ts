/**
 * Basic BigQuery emulator tests.
 *
 * This test suite covers fundamental BigQuery operations to ensure
 * the Node.js client can successfully interact with the emulator.
 * These tests mirror some of the Python tests in emulator_test.py.
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import {
  BigQueryEmulatorContainer,
  BQ_EMULATOR_PROJECT_ID,
} from './utils/BigQueryEmulatorContainer.js';
import {
  createBigQueryClient,
  createTestHelper,
  BigQueryTestHelper,
} from './utils/testSetup.js';
import { BigQuery } from '@google-cloud/bigquery';

describe('Basic BigQuery Emulator Tests', () => {
  let emulator: BigQueryEmulatorContainer;
  let client: BigQuery;
  let helper: BigQueryTestHelper;

  beforeAll(async () => {
    emulator = await BigQueryEmulatorContainer.start();
    client = createBigQueryClient(emulator);
    helper = createTestHelper(client);
  });

  afterAll(async () => {
    await emulator.stop();
  });

  afterEach(async () => {
    // Clean up all datasets after each test
    await helper.deleteAllDatasets();
  });

  describe('Queries Without Tables', () => {
    it('should execute simple SELECT without tables', async () => {
      const query = 'SELECT 1 AS one, 2 AS two';
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0]).toEqual({ one: 1, two: 2 });
    });

    it('should handle UNNEST with STRUCT', async () => {
      const query = `
        SELECT *
        FROM UNNEST([
          STRUCT(1 AS a, 2 AS b),
          STRUCT(3 AS a, 4 AS b)
        ])
      `;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(2);
      expect(rows[0]).toEqual({ a: 1, b: 2 });
      expect(rows[1]).toEqual({ a: 3, b: 4 });
    });

    it('should handle SELECT EXCEPT clause', async () => {
      const query = `
        SELECT * EXCEPT(b)
        FROM UNNEST([
          STRUCT(1 AS a, 2 AS b),
          STRUCT(3 AS a, 4 AS b)
        ])
      `;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(2);
      expect(rows[0]).toEqual({ a: 1 });
      expect(rows[1]).toEqual({ a: 3 });
    });

    it('should handle QUALIFY clause', async () => {
      const query = `
        SELECT *
        FROM UNNEST([
          STRUCT(1 AS a, 2 AS b),
          STRUCT(3 AS a, 4 AS b)
        ])
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER (ORDER BY b DESC) = 1
      `;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0]).toEqual({ a: 3, b: 4 });
    });
  });

  describe('Table Operations', () => {
    it('should create and query empty table', async () => {
      await helper.createTable(
        { datasetId: 'dataset1', tableId: 'table1' },
        {
          fields: [
            { name: 'a', type: 'INTEGER', mode: 'REQUIRED' },
            { name: 'b', type: 'STRING', mode: 'NULLABLE' },
          ],
        }
      );

      const query = `SELECT a, b FROM \`${BQ_EMULATOR_PROJECT_ID}.dataset1.table1\``;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(0);
    });

    it('should insert and query data', async () => {
      await helper.createTable(
        { datasetId: 'dataset1', tableId: 'table1' },
        {
          fields: [
            { name: 'a', type: 'INTEGER', mode: 'REQUIRED' },
            { name: 'b', type: 'STRING', mode: 'NULLABLE' },
          ],
        }
      );

      await helper.insertRows(
        { datasetId: 'dataset1', tableId: 'table1' },
        [
          { a: 1, b: 'foo' },
          { a: 3, b: null },
        ]
      );

      const query = `SELECT a, b FROM \`${BQ_EMULATOR_PROJECT_ID}.dataset1.table1\``;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(2);
      expect(rows[0]).toEqual({ a: 1, b: 'foo' });
      expect(rows[1]).toEqual({ a: 3, b: null });
    });

    it('should handle table existence check', async () => {
      const address = { datasetId: 'dataset1', tableId: 'table1' };

      let exists = await helper.tableExists(address);
      expect(exists).toBe(false);

      await helper.createTable(address, {
        fields: [{ name: 'id', type: 'INTEGER', mode: 'REQUIRED' }],
      });

      exists = await helper.tableExists(address);
      expect(exists).toBe(true);
    });
  });

  describe('Aggregations and Functions', () => {
    beforeAll(async () => {
      await helper.createTable(
        { datasetId: 'dataset1', tableId: 'numbers' },
        {
          fields: [
            { name: 'a', type: 'INTEGER', mode: 'REQUIRED' },
            { name: 'b', type: 'INTEGER', mode: 'NULLABLE' },
          ],
        }
      );

      await helper.insertRows(
        { datasetId: 'dataset1', tableId: 'numbers' },
        [
          { a: 1, b: 2 },
          { a: 3, b: 4 },
        ]
      );
    });

    it('should handle MIN and MAX aggregations', async () => {
      const query = `
        SELECT MIN(a) AS min_a, MAX(b) AS max_b
        FROM \`${BQ_EMULATOR_PROJECT_ID}.dataset1.numbers\`
      `;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0]).toEqual({ min_a: 1, max_b: 4 });
    });

    it('should handle ARRAY_AGG', async () => {
      const query = `
        SELECT b, ARRAY_AGG(a) AS a_list
        FROM UNNEST([
          STRUCT(1 AS a, 2 AS b),
          STRUCT(3 AS a, 2 AS b)
        ])
        GROUP BY b
      `;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0].b).toBe(2);
      expect(rows[0].a_list).toEqual([1, 3]);
    });

    it('should handle array literals', async () => {
      const query = 'SELECT [1, 2, 3] as arr';
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0].arr).toEqual([1, 2, 3]);
    });
  });

  describe('Date and Time Functions', () => {
    it('should handle DATE literals', async () => {
      const query = `
        SELECT DATE '2024-01-15' AS date_value
      `;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      // BigQuery returns dates as Date objects or strings depending on client
      const dateValue = rows[0].date_value;
      expect(dateValue).toBeTruthy();
    });

    it('should handle SAFE.PARSE_DATE with valid input', async () => {
      const query = `SELECT SAFE.PARSE_DATE("%m/%d/%Y", "12/25/2008") as result`;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0].result).toBeTruthy();
    });

    it('should handle SAFE.PARSE_DATE with invalid input', async () => {
      const query = `SELECT SAFE.PARSE_DATE("%m/%d/%Y", "2008-12-25") as result`;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0].result).toBeNull();
    });
  });

  describe('JSON Functions', () => {
    it('should handle TO_JSON with STRUCT', async () => {
      const query = `
        SELECT TO_JSON(
          STRUCT("foo" AS a, 1 AS b)
        ) AS result
      `;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0].result).toEqual({ a: 'foo', b: 1 });
    });

    it('should handle TO_JSON with array', async () => {
      const query = 'SELECT TO_JSON([1, 2, 3]) as result';
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0].result).toEqual([1, 2, 3]);
    });

    it('should handle nested JSON', async () => {
      const query = `
        SELECT TO_JSON(
          STRUCT("foo" AS a, TO_JSON(STRUCT("bar" AS c)) AS b)
        ) AS result
      `;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0].result).toEqual({ a: 'foo', b: { c: 'bar' } });
    });
  });

  describe('Window Functions', () => {
    it('should handle MIN with PARTITION BY', async () => {
      const query = `
        SELECT MIN(a) OVER (PARTITION BY b) AS min_a
        FROM UNNEST([STRUCT(1 AS a, 2 AS b)])
      `;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0].min_a).toBe(1);
    });

    it('should handle MAX with PARTITION BY', async () => {
      const query = `
        SELECT MAX(a) OVER (PARTITION BY b) AS max_a
        FROM UNNEST([STRUCT(1 AS a, 2 AS b)])
      `;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0].max_a).toBe(1);
    });

    it('should handle COUNT with PARTITION BY', async () => {
      const query = `
        SELECT COUNT(a) OVER (PARTITION BY b) AS count_a
        FROM UNNEST([STRUCT(1 AS a, 2 AS b)])
      `;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0].count_a).toBe(1);
    });
  });

  describe('NULL Handling', () => {
    it('should handle NULL in UNNEST', async () => {
      const query = `
        SELECT a
        FROM UNNEST([
          STRUCT(NULL AS a)
        ])
      `;
      const rows = await helper.query(query);

      expect(rows).toHaveLength(1);
      expect(rows[0].a).toBeNull();
    });
  });

  describe('Dataset and Table Management', () => {
    it('should create tables with same name in different datasets', async () => {
      // Create first table
      await helper.createTable(
        { datasetId: 'dataset1', tableId: 'table1' },
        {
          fields: [{ name: 'a', type: 'INTEGER', mode: 'REQUIRED' }],
        }
      );

      // Create second table with same name in different dataset
      await helper.createTable(
        { datasetId: 'dataset2', tableId: 'table1' },
        {
          fields: [{ name: 'b', type: 'INTEGER', mode: 'REQUIRED' }],
        }
      );

      // Insert different data into each
      await helper.insertRows({ datasetId: 'dataset1', tableId: 'table1' }, [
        { a: 1 },
      ]);
      await helper.insertRows({ datasetId: 'dataset2', tableId: 'table1' }, [
        { b: 2 },
      ]);

      // Query each table
      const rows1 = await helper.query(
        `SELECT a FROM \`${BQ_EMULATOR_PROJECT_ID}.dataset1.table1\``
      );
      const rows2 = await helper.query(
        `SELECT b FROM \`${BQ_EMULATOR_PROJECT_ID}.dataset2.table1\``
      );

      expect(rows1).toHaveLength(1);
      expect(rows1[0].a).toBe(1);

      expect(rows2).toHaveLength(1);
      expect(rows2[0].b).toBe(2);
    });
  });
});