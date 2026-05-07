import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  DeleteCommand,
  UpdateCommand,
  QueryCommand,
  QueryCommandInput,
  UpdateCommandInput,
} from '@aws-sdk/lib-dynamodb';

export class BaseRepository {
  constructor(
    protected tableName: string,
    protected docClient: DynamoDBDocumentClient,
  ) {}

  async getItem<T = Record<string, unknown>>(
    key: Record<string, unknown>,
  ): Promise<T | null> {
    const result = await this.docClient.send(
      new GetCommand({ TableName: this.tableName, Key: key }),
    );
    return (result.Item as T) ?? null;
  }

  async putItem(item: Record<string, unknown>): Promise<void> {
    await this.docClient.send(
      new PutCommand({ TableName: this.tableName, Item: item }),
    );
  }

  async putItemConditional(
    item: Record<string, unknown>,
    conditionExpression: string,
    expressionAttributeNames?: Record<string, string>,
  ): Promise<void> {
    await this.docClient.send(
      new PutCommand({
        TableName: this.tableName,
        Item: item,
        ConditionExpression: conditionExpression,
        ...(expressionAttributeNames
          ? { ExpressionAttributeNames: expressionAttributeNames }
          : {}),
      }),
    );
  }

  async deleteItem(key: Record<string, unknown>): Promise<void> {
    await this.docClient.send(
      new DeleteCommand({ TableName: this.tableName, Key: key }),
    );
  }

  async updateItem(
    params: Omit<UpdateCommandInput, 'TableName'>,
  ): Promise<Record<string, unknown> | undefined> {
    const result = await this.docClient.send(
      new UpdateCommand({ TableName: this.tableName, ...params }),
    );
    return result.Attributes as Record<string, unknown> | undefined;
  }

  async query<T = Record<string, unknown>>(
    params: Omit<Partial<QueryCommandInput>, 'TableName'>,
  ): Promise<T[]> {
    const result = await this.docClient.send(
      new QueryCommand({ TableName: this.tableName, ...params }),
    );
    return (result.Items ?? []) as T[];
  }

  async queryWithPagination<T = Record<string, unknown>>(
    params: Omit<Partial<QueryCommandInput>, 'TableName'>,
  ): Promise<{ items: T[]; lastEvaluatedKey?: Record<string, unknown> }> {
    const result = await this.docClient.send(
      new QueryCommand({ TableName: this.tableName, ...params }),
    );
    return {
      items: (result.Items ?? []) as T[],
      lastEvaluatedKey: result.LastEvaluatedKey as
        | Record<string, unknown>
        | undefined,
    };
  }
}
