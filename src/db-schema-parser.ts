/**
 * DB Schema Parser
 * Parses .prisma files and converts them to DatabaseSchema objects for raw SQL generation
 */

import fs from 'fs';
import {
  DatabaseSchema,
  TableSchema,
  ColumnDefinition,
  ForeignKey,
  Index,
  PRISMA_TO_POSTGRES,
  PrismaFieldAttribute,
  PrismaModelAttribute,
} from './db-schema-types.js';

export class DbSchemaParseError extends Error {
  constructor(
    message: string,
    public cause?: Error
  ) {
    super(message);
    this.name = 'DbSchemaParseError';
  }
}

export class DbSchemaParser {
  /**
   * Parse Prisma schema file and return DatabaseSchema
   */
  static parse(schemaPath: string): DatabaseSchema {
    if (!fs.existsSync(schemaPath)) {
      throw new DbSchemaParseError(`Schema file not found: ${schemaPath}`);
    }

    const content = fs.readFileSync(schemaPath, 'utf8');
    const tables: TableSchema[] = [];

    // Parse enums first to build a set of known enum types
    const enumTypes = this.parseEnums(content);

    // Parse models
    const modelMatches = content.matchAll(/model\s+(\w+)\s*{([^}]+)}/g);

    for (const match of modelMatches) {
      const modelName = match[1];
      const modelBody = match[2];

      try {
        const table = this.parseModel(modelName, modelBody, enumTypes);
        tables.push(table);
      } catch (error) {
        throw new DbSchemaParseError(`Failed to parse model ${modelName}`, error as Error);
      }
    }

    // Convert tables array to Map
    const tablesMap = new Map<string, TableSchema>();
    for (const table of tables) {
      tablesMap.set(table.name, table);
    }

    return new DatabaseSchemaImpl(tablesMap);
  }

  /**
   * Parse enum definitions from schema content
   */
  private static parseEnums(content: string): Set<string> {
    const enumTypes = new Set<string>();
    const enumMatches = content.matchAll(/enum\s+(\w+)\s*{[^}]+}/g);

    for (const match of enumMatches) {
      const enumName = match[1];
      enumTypes.add(enumName);
    }

    return enumTypes;
  }

  /**
   * Parse Prisma schema content string
   */
  static parseSchemaContent(content: string): DatabaseSchema {
    try {
      const tables = new Map<string, TableSchema>();

      // Extract model definitions
      const modelMatches = content.matchAll(/model\s+(\w+)\s*{([^}]*)}/g);

      for (const match of modelMatches) {
        const modelName = match[1];
        const modelBody = match[2];

        const tableSchema = this.parseModel(modelName, modelBody);
        tables.set(tableSchema.name, tableSchema);
      }

      return new DatabaseSchemaImpl(tables);
    } catch (error) {
      throw new DbSchemaParseError('Failed to parse Prisma schema', error as Error);
    }
  }

  /**
   * Parse a single model definition
   */
  private static parseModel(
    modelName: string,
    modelBody: string,
    enumTypes: Set<string> = new Set()
  ): TableSchema {
    const columns: ColumnDefinition[] = [];
    const foreignKeys: ForeignKey[] = [];
    const indexes: Index[] = [];

    // Parse model attributes (@@map, @@id, @@unique, etc.)
    const modelAttributes = this.parseModelAttributes(modelBody);
    const tableName = modelAttributes.map || modelName;

    // Parse fields
    const fieldLines = modelBody
      .split('\n')
      .map(line => line.trim())
      .filter(line => line && !line.startsWith('//') && !line.startsWith('@@'));

    for (const line of fieldLines) {
      if (line.includes('@@')) continue; // Skip model attributes

      const fieldMatch = line.match(/(\w+)\s+(.+)/);
      if (!fieldMatch) continue;

      const fieldName = fieldMatch[1];
      const fieldDefinition = fieldMatch[2];

      const { column, foreignKey, index } = this.parseField(fieldName, fieldDefinition, enumTypes);

      if (column) columns.push(column);
      if (foreignKey) foreignKeys.push(foreignKey);
      if (index) indexes.push(index);
    }

    // Handle composite primary keys from @@id
    if (modelAttributes.id && modelAttributes.id.length > 0) {
      for (const column of columns) {
        column.primaryKey = modelAttributes.id.includes(column.name);
      }
    }

    return {
      name: tableName,
      columns,
      foreignKeys,
      indexes,
    };
  }

  /**
   * Parse model-level attributes (@@map, @@id, etc.)
   */
  private static parseModelAttributes(modelBody: string): PrismaModelAttribute {
    const attributes: PrismaModelAttribute = {};

    // @@map("table_name")
    const mapMatch = modelBody.match(/@@map\s*\(\s*"([^"]+)"\s*\)/);
    if (mapMatch) {
      attributes.map = mapMatch[1];
    }

    // @@id([field1, field2])
    const idMatch = modelBody.match(/@@id\s*\(\s*\[([^\]]+)\]\s*\)/);
    if (idMatch) {
      attributes.id = idMatch[1].split(',').map(f => f.trim().replace(/['"]/g, ''));
    }

    return attributes;
  }

  /**
   * Parse a single field definition
   */
  private static parseField(
    fieldName: string,
    fieldDefinition: string,
    enumTypes: Set<string> = new Set()
  ): {
    column?: ColumnDefinition;
    foreignKey?: ForeignKey;
    index?: Index;
  } {
    // Parse field type and attributes
    // Handle Unsupported types that contain spaces and nested parentheses
    let fieldType: string;
    if (fieldDefinition.startsWith('Unsupported(')) {
      // Find the matching closing parenthesis for Unsupported(...)
      // This handles nested parentheses within the Unsupported type
      let parenCount = 0;
      let endIndex = -1;
      for (let i = 11; i < fieldDefinition.length; i++) {
        // Start after "Unsupported"
        if (fieldDefinition[i] === '(') {
          parenCount++;
        } else if (fieldDefinition[i] === ')') {
          parenCount--;
          if (parenCount === 0) {
            endIndex = i;
            break;
          }
        }
      }

      if (endIndex !== -1) {
        fieldType = fieldDefinition.substring(0, endIndex + 1);
      } else {
        // Fallback to first part if parsing fails
        fieldType = fieldDefinition.split(/\s+/)[0];
      }
    } else {
      // Normal case: split by whitespace and take first part
      fieldType = fieldDefinition.split(/\s+/)[0];
    }

    const attributes = this.parseFieldAttributes(fieldDefinition);

    // Check if this is a relation field (references another model)
    // Skip array relations like Post[] and Comment[] - these are virtual
    if (fieldType.includes('[]')) {
      return {}; // Array relations don't create database columns
    }

    const relationMatch = fieldType.match(/^(\w+)(\?)?$/);
    if (
      relationMatch &&
      !PRISMA_TO_POSTGRES[relationMatch[1]] &&
      !enumTypes.has(relationMatch[1])
    ) {
      // Look for @relation attribute to determine foreign key details
      const relationAttr = fieldDefinition.match(/@relation\s*\([^)]+\)/);
      if (relationAttr) {
        // This might be the "other side" of the relation
        return {};
      }

      // This is a relation field without @relation attribute
      // In Prisma, these don't create actual columns, just navigation properties
      return {};
    }

    // Regular field
    const isOptional = fieldType.endsWith('?');
    const baseType = isOptional ? fieldType.slice(0, -1) : fieldType;

    // Handle Unsupported PostGIS geometry types
    let postgresType: string;
    if (baseType.startsWith('Unsupported("') && baseType.endsWith('")')) {
      // Extract the actual PostgreSQL type from Unsupported("type")
      const unsupportedMatch = baseType.match(/^Unsupported\("([^"]+)"\)$/);
      if (unsupportedMatch) {
        postgresType = unsupportedMatch[1]; // e.g., "geometry(MultiPolygon, 4326)"
      } else {
        postgresType = 'TEXT'; // Fallback
      }
    } else if (enumTypes.has(baseType)) {
      // Handle Prisma enums - map to VARCHAR for simplicity
      postgresType = 'VARCHAR(50)';
    } else {
      postgresType = PRISMA_TO_POSTGRES[baseType] || 'TEXT';
    }

    const column: ColumnDefinition = {
      name: attributes.map || fieldName,
      type: postgresType,
      nullable: isOptional,
      primaryKey: !!attributes.id,
      unique: !!attributes.unique,
      autoIncrement: !!(attributes.autoIncrement || (attributes.id && baseType === 'Int')), // @id Int fields or explicit @default(autoincrement())
      defaultValue: attributes.default?.toString(),
    };

    return { column };
  }

  /**
   * Parse field attributes (@id, @unique, @default, etc.)
   */
  private static parseFieldAttributes(fieldDefinition: string): PrismaFieldAttribute {
    const attributes: PrismaFieldAttribute = {};

    if (fieldDefinition.includes('@id')) {
      attributes.id = true;
    }

    if (fieldDefinition.includes('@unique')) {
      attributes.unique = true;
    }

    // @default(value)
    const defaultMatch = fieldDefinition.match(/@default\s*\(\s*((?:[^()]+|\([^()]*\))+)\s*\)/);
    if (defaultMatch) {
      const defaultValue = defaultMatch[1].trim();

      // Handle common default functions
      if (defaultValue === 'autoincrement()') {
        // Set autoIncrement flag for non-@id fields that use autoincrement()
        attributes.autoIncrement = true;
      } else if (defaultValue === 'now()') {
        attributes.default = 'CURRENT_TIMESTAMP';
      } else {
        // Remove quotes for string literals
        attributes.default = defaultValue.replace(/^["']|["']$/g, '');
      }
    }

    // @map("column_name")
    const mapMatch = fieldDefinition.match(/@map\s*\(\s*"([^"]+)"\s*\)/);
    if (mapMatch) {
      attributes.map = mapMatch[1];
    }

    return attributes;
  }
}

/**
 * Implementation of DatabaseSchema interface
 */
class DatabaseSchemaImpl implements DatabaseSchema {
  constructor(public tables: Map<string, TableSchema>) {}

  getTableNames(): string[] {
    return Array.from(this.tables.keys());
  }

  getTable(name: string): TableSchema | undefined {
    return this.tables.get(name);
  }

  /**
   * Return tables in dependency order (topological sort)
   * Tables with no dependencies come first, then tables that depend on them
   */
  getTablesInDependencyOrder(): string[] {
    const tableNames = this.getTableNames();
    const visited = new Set<string>();
    const visiting = new Set<string>();
    const result: string[] = [];

    const visit = (tableName: string): void => {
      if (visited.has(tableName)) return;
      if (visiting.has(tableName)) {
        throw new DbSchemaParseError(`Circular dependency detected involving table: ${tableName}`);
      }

      visiting.add(tableName);

      const table = this.getTable(tableName);
      if (table) {
        // Visit all tables that this table depends on (via foreign keys)
        for (const fk of table.foreignKeys) {
          if (fk.toTable !== tableName) {
            // Avoid self-references
            visit(fk.toTable);
          }
        }
      }

      visiting.delete(tableName);
      visited.add(tableName);
      result.push(tableName);
    };

    for (const tableName of tableNames) {
      visit(tableName);
    }

    return result;
  }
}
