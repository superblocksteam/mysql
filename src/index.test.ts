import { MySQLDatasourceConfiguration } from '@superblocksteam/shared';

import {
  DUMMY_ACTION_CONFIGURATION,
  DUMMY_DB_DATASOURCE_CONFIGURATION,
  DUMMY_EXECUTION_CONTEXT,
  DUMMY_EXPECTED_METADATA,
  DUMMY_EXTRA_PLUGIN_EXECUTION_PROPS,
  DUMMY_QUERY_RESULT,
  DUMMY_TABLE_RESULT
} from '@superblocksteam/shared-backend';

jest.mock('@superblocksteam/shared-backend', () => {
  const originalModule = jest.requireActual('@superblocksteam/shared-backend');
  return {
    __esModule: true,
    ...originalModule,
    CreateConnection: jest.fn((target, name, descriptor) => {
      return descriptor;
    }),
    DestroyConnection: jest.fn((target, name, descriptor) => {
      return descriptor;
    })
  };
});

import mariadb from 'mariadb';
jest.mock('mariadb');

import MySQLPlugin from '.';

const plugin: MySQLPlugin = new MySQLPlugin();
plugin.logger = { debug: (): void => undefined };

const datasourceConfiguration = DUMMY_DB_DATASOURCE_CONFIGURATION as MySQLDatasourceConfiguration;
const actionConfiguration = DUMMY_ACTION_CONFIGURATION;
const context = DUMMY_EXECUTION_CONTEXT;
const props = {
  context,
  datasourceConfiguration,
  actionConfiguration,
  ...DUMMY_EXTRA_PLUGIN_EXECUTION_PROPS
};

afterEach(() => {
  jest.restoreAllMocks();
});

describe('Mysql Plugin', () => {
  it('test connection', async () => {
    const connection = {
      connect: () => undefined,
      on: () => undefined,
      query: jest.fn().mockImplementation((): void => undefined)
    };
    mariadb.createConnection = jest.fn().mockImplementation(() => connection);

    await plugin.test(datasourceConfiguration);

    expect(connection.query).toBeCalledTimes(1);
  });

  it('get metadata', async () => {
    const connection = {
      connect: () => undefined,
      on: () => undefined,
      query: jest.fn().mockImplementation(() => {
        return DUMMY_TABLE_RESULT;
      })
    };
    mariadb.createConnection = jest.fn().mockImplementation(() => connection);

    const res = await plugin.metadata(datasourceConfiguration);

    expect(res.dbSchema?.tables[0]).toEqual(DUMMY_EXPECTED_METADATA);
    expect(connection.query).toBeCalledTimes(1);
  });

  it('execute query', async () => {
    const connection = await mariadb.createConnection({});
    connection.query = jest.fn().mockImplementation(() => {
      return DUMMY_QUERY_RESULT;
    });

    const res = await plugin.executePooled(props, connection);

    expect(res.output).toEqual(DUMMY_QUERY_RESULT);
    expect(connection.query).toBeCalledTimes(1);
  });
});
