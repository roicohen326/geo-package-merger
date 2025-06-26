import * as fs from 'fs';
import * as path from 'path';
import Database from 'better-sqlite3';
import { StatusCodes } from 'http-status-codes';

interface TableInfo {
  name: string;
}

interface SqlResult {
  sql: string;
}

const BYTES_TO_MB = 1024 * 1024;

console.log("GeoPackage Merge Tool");

const args = process.argv.slice(2);

if (args.length < 2) {
  console.log(`
Not enough arguments. Try providing at least two GeoPackage files to merge.

Usage:
  npm run merge <file1> <file2> [output]

Examples:
  npm run merge ./data/file1.gpkg ./data/file2.gpkg
  npm run merge ./data/file1.gpkg ./data/file2.gpkg ./custom_output.gpkg
`);
  process.exit(1);
}

const file1 = args[0];
const file2 = args[1];

function validateFilesExist(file1: string, file2: string): void {
  const file1Exists = fs.existsSync(file1);
  const file2Exists = fs.existsSync(file2);
  
  if (!file1Exists || !file2Exists) {
    const missingFiles = [
      !file1Exists ? file1 : null,
      !file2Exists ? file2 : null
    ].filter(Boolean).join(', ');
    
    const error = new Error(`Missing file(s): ${missingFiles}`);
    (error as any).status = StatusCodes.BAD_REQUEST;
    throw error;
  }
}

function logFileSizes(file1: string, file2: string): { file1Name: string; file2Name: string } {
  const sizes = [file1, file2].map(file => (fs.statSync(file).size / BYTES_TO_MB).toFixed(2));
  const names = [file1, file2].map(getName);
  
  names.forEach((name, index) => {
    console.log(`${name} dataset size: ${sizes[index]} MB`);
  });
  
  return { file1Name: names[0], file2Name: names[1] };
}

function getName(filepath: string): string {
  const basename = path.basename(filepath);
  const extname = path.extname(basename);
  return basename.replace(extname, '');
}

function createOutputFilename(file1: string, file2: string, customOutput?: string): string {
  if (customOutput) {
    return customOutput;
  }
  
  const name1 = getName(file1);
  const name2 = getName(file2);
  
  return `merged_${name1}_${name2}.gpkg`;
}

let outputFilename = createOutputFilename(file1, file2, args[2]);

function getDataTables(database: Database.Database): TableInfo[] {
  return database.prepare(`
    SELECT name FROM sqlite_master 
    WHERE type = 'table' 
    AND name NOT LIKE 'sqlite_%' 
    AND name NOT LIKE 'gpkg_%'
    AND name NOT LIKE 'rtree_%'
  `).all() as TableInfo[];
}

function tableExists(targetDb: Database.Database, tableName: string): boolean {
  return !!targetDb.prepare(`
    SELECT name FROM sqlite_master 
    WHERE type='table' AND name = ?
  `).get(tableName);
}

function createTableIfNotExists(sourceDb: Database.Database, targetDb: Database.Database, tableName: string): void {
  if (tableExists(targetDb, tableName)) {
    return;
  }

  console.log(`Creating new table: ${tableName}`);
  const createTableSQL = sourceDb.prepare(`
    SELECT sql FROM sqlite_master 
    WHERE type='table' AND name = ?
  `).get(tableName) as SqlResult;
  
  if (createTableSQL && createTableSQL.sql) {
    targetDb.exec(createTableSQL.sql);
  } else {
    throw new Error(`Could not get table structure for ${tableName}`);
  }
}

function insertTableData(targetDb: Database.Database, tableName: string): number {
  const result = targetDb.prepare(`
    INSERT INTO ${tableName} 
    SELECT * FROM source_db.${tableName}
  `).run();
  
  return result.changes || 0;
}

function processTable(sourceDb: Database.Database, targetDb: Database.Database, table: TableInfo): number {
  createTableIfNotExists(sourceDb, targetDb, table.name);
  return insertTableData(targetDb, table.name);
}

try {
  validateFilesExist(file1, file2);

  const { file1Name, file2Name } = logFileSizes(file1, file2);

  let finalOutputFilename = outputFilename;
  if (fs.existsSync(outputFilename)) {
    const timestamp = Date.now();
    const ext = path.extname(outputFilename);
    const nameWithoutExt = outputFilename.replace(ext, '');
    finalOutputFilename = `${nameWithoutExt}_${timestamp}${ext}`;
    console.log(`Output file exists, creating: ${finalOutputFilename}`);
  }
  
  fs.copyFileSync(file1, finalOutputFilename);
  outputFilename = finalOutputFilename;

  const targetDb = new Database(outputFilename);
  const sourceDb = new Database(file2, { readonly: true });

  const sourceTables = getDataTables(sourceDb);
  console.log('Merging datasets...');
  targetDb.exec(`ATTACH DATABASE '${file2}' AS source_db`);
  
  let totalMerged = 0;
  for (const table of sourceTables) {
    try {
      const rowsAdded = processTable(sourceDb, targetDb, table);
      totalMerged += rowsAdded;
    } catch (err) {
      console.error(`Error processing table ${table.name}: ${err}`);
      throw err;
    }
  }

  targetDb.exec('DETACH DATABASE source_db');
  sourceDb.close();
  targetDb.close();
  
  const finalSize = (fs.statSync(outputFilename).size / BYTES_TO_MB).toFixed(2);
  console.log(`\n${file1Name.toUpperCase()} + ${file2Name.toUpperCase()} MERGE COMPLETE!`);
  console.log(`Total rows added: ${totalMerged}`);
  console.log(`Output: ${outputFilename} (${finalSize} MB)`);
  console.log(`Combined: ${file1Name} + ${file2Name} datasets!`);

} catch (error: any) {
  console.error('Failed:', error.message);
  const exitCode = error.status === StatusCodes.BAD_REQUEST ? 1 : 2;
  process.exit(exitCode);
}
