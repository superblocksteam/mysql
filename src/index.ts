import {
  Column,
  DatasourceMetadataDto,
  DBActionConfiguration,
  ExecutionOutput,
  IntegrationError,
  MySQLDatasourceConfiguration,
  RawRequest,
  ResolvedActionConfigurationProperty,
  Table,
  TableType
} from '@superblocksteam/shared';
import {
  ActionConfigurationResolutionContext,
  BasePlugin,
  normalizeTableColumnNames,
  PluginExecutionProps,
  resolveActionConfigurationPropertyUtil
} from '@superblocksteam/shared-backend';
import { isEmpty } from 'lodash';
// We are using the mariadb module because it has performance
// and feature benefits over the mysql module, and has full compatibility
// with MySQL databases
import { Connection, createConnection } from 'mariadb';

const TEST_CONNECTION_TIMEOUT = 5000;

export default class MySQLPlugin extends BasePlugin {
  pluginName = 'MySQL';

  async resolveActionConfigurationProperty({
    context,
    actionConfiguration,
    files,
    property,
    escapeStrings
  }: // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ActionConfigurationResolutionContext): Promise<ResolvedActionConfigurationProperty> {
    return resolveActionConfigurationPropertyUtil(
      super.resolveActionConfigurationProperty,
      {
        context,
        actionConfiguration,
        files,
        property,
        escapeStrings
      },
      false /* useOrderedParameters */
    );
  }

  async execute({
    context,
    datasourceConfiguration,
    actionConfiguration
  }: PluginExecutionProps<MySQLDatasourceConfiguration>): Promise<ExecutionOutput> {
    const connection = await this.createConnection(datasourceConfiguration);
    try {
      const query = actionConfiguration.body;

      const ret = new ExecutionOutput();
      if (isEmpty(query)) {
        return ret;
      }
      const rows = await connection.query(query, context.preparedStatementContext);
      ret.output = normalizeTableColumnNames(rows);
      return ret;
    } catch (err) {
      throw new IntegrationError(`${this.pluginName} query failed, ${err.message}`);
    } finally {
      if (connection) {
        await connection.end();
      }
    }
  }

  getRequest(actionConfiguration: DBActionConfiguration): RawRequest {
    return actionConfiguration?.body;
  }

  dynamicProperties(): string[] {
    return ['body'];
  }

  async metadata(datasourceConfiguration: MySQLDatasourceConfiguration): Promise<DatasourceMetadataDto> {
    let connection: Connection | undefined;
    try {
      connection = await this.createConnection(datasourceConfiguration);

      const tableQuery =
        'select COLUMN_NAME as name,' +
        '       TABLE_NAME as table_name,' +
        '       COLUMN_TYPE as column_type' +
        ' from information_schema.columns' +
        ' where table_schema = database()' +
        ' order by table_name, ordinal_position';
      const tableResult = await connection.query(tableQuery);

      const entities = tableResult.reduce((acc, attribute) => {
        const entityName = attribute.table_name;
        const entityType = TableType.TABLE;

        const entity = acc.find((o) => o.name === entityName);
        if (entity) {
          const columns = entity.columns;
          entity.columns = [...columns, new Column(attribute.name, attribute.column_type)];
          return [...acc];
        }

        const table = new Table(entityName, entityType);
        table.columns.push(new Column(attribute.name, attribute.column_type));

        return [...acc, table];
      }, []);

      return {
        dbSchema: { tables: entities }
      };
    } catch (err) {
      throw new IntegrationError(`Failed to connect to ${this.pluginName}, ${err.message}`);
    } finally {
      if (connection) {
        await connection.end();
      }
    }
  }

  private async createConnection(
    datasourceConfiguration: MySQLDatasourceConfiguration,
    connectionTimeoutMillis = 30000
  ): Promise<Connection> {
    if (!datasourceConfiguration) {
      throw new IntegrationError(`Datasource not found for ${this.pluginName} step`);
    }
    try {
      const endpoint = datasourceConfiguration.endpoint;
      const auth = datasourceConfiguration.authentication;
      if (!endpoint) {
        throw new IntegrationError(`Endpoint not specified for ${this.pluginName} step`);
      }
      if (!auth) {
        throw new IntegrationError(`Authentication not specified for ${this.pluginName} step`);
      }
      if (!auth.custom?.databaseName?.value) {
        throw new IntegrationError(`Database not specified for ${this.pluginName} step`);
      }
      const connection = await createConnection({
        host: endpoint.host,
        user: auth.username,
        password: auth.password,
        database: auth.custom.databaseName.value,
        port: endpoint.port,
        ssl: datasourceConfiguration.connection?.useSsl ? { rejectUnauthorized: false } : false,
        connectTimeout: connectionTimeoutMillis,
        allowPublicKeyRetrieval: !(datasourceConfiguration.connection?.useSsl ?? false)
      });
      this.attachLoggerToConnection(connection, datasourceConfiguration);

      this.logger.debug(
        `${this.pluginName} connection created. ${datasourceConfiguration.endpoint?.host}:${datasourceConfiguration.endpoint?.port}`
      );
      return connection;
    } catch (err) {
      throw new IntegrationError(`Failed to connect to ${this.pluginName}, ${err.message}`);
    }
  }

  private attachLoggerToConnection(connection: Connection, datasourceConfiguration: MySQLDatasourceConfiguration) {
    if (!datasourceConfiguration.endpoint) {
      return;
    }

    const datasourceEndpoint = `${datasourceConfiguration.endpoint?.host}:${datasourceConfiguration.endpoint?.port}`;

    connection.on('error', (err: Error) => {
      this.logger.debug(`${this.pluginName} connection error. ${datasourceEndpoint}`, err.stack);
    });

    connection.on('end', () => {
      this.logger.debug(`${this.pluginName} connection ended. ${datasourceEndpoint}`);
    });
  }

  async test(datasourceConfiguration: MySQLDatasourceConfiguration): Promise<void> {
    let connection: Connection | null = null;
    try {
      connection = await this.createConnection(datasourceConfiguration, TEST_CONNECTION_TIMEOUT);
      await connection.query('SELECT NOW()');
    } catch (err) {
      throw new IntegrationError(`Test ${this.pluginName} connection failed, ${err.message}`);
    } finally {
      if (connection) {
        await connection.end();
      }
    }
  }
}
