import * as fs from 'fs';
import Database from 'better-sqlite3';
import { StatusCodes } from 'http-status-codes';

interface TableInfo {
  name: string;
}

interface CountResult {
  count: number;
}

const BYTES_TO_MB = 1024 * 1024;

console.log("GeoPackage Merge Tool");

// Parse command line arguments
const args = process.argv.slice(2);

// Show usage if not enough arguments
if (args.length < 2) {
  console.log('\nUsage:');
  console.log('  npm run merge <file1> <file2> [output]');
  console.log('\nExamples:');
  console.log('  npm run merge ./data/north.gpkg ./data/south.gpkg');
  console.log('  npm run merge ./data/north.gpkg ./data/south.gpkg ./custom_output.gpkg');
  process.exit(1);
}

// Dynamic file paths from command line arguments
const file1 = args[0];
const file2 = args[1];

// Create dynamic output filename based on input filenames
function createOutputFilename(file1: string, file2: string, customOutput?: string): string {
  if (customOutput) {
    return customOutput;
  }
  
  // Extract just the filename without path and extension
  const getName = (filepath: string) => {
    const filename = filepath.split('/').pop() || filepath;
    return filename.replace(/\.gpkg$/i, '');
  };
  
  const name1 = getName(file1);
  const name2 = getName(file2);
  
  return `merged_${name1}_${name2}.gpkg`;
}

let output = createOutputFilename(file1, file2, args[2]);

// Reusable function to get data tables from any database
function getDataTables(database: Database.Database): TableInfo[] {
  return database.prepare(`
    SELECT name FROM sqlite_master 
    WHERE type = 'table' 
    AND name NOT LIKE 'sqlite_%' 
    AND name NOT LIKE 'gpkg_%'
    AND name NOT LIKE 'rtree_%'
  `).all() as TableInfo[];
}

// Helper functions for table processing
function tableExists(targetDb: Database.Database, tableName: string): boolean {
  return !!targetDb.prepare(`
    SELECT name FROM sqlite_master 
    WHERE type='table' AND name = ?
  `).get(tableName);
}

function createTableIfNotExists(sourceDb: Database.Database, targetDb: Database.Database, tableName: string): boolean {
  if (tableExists(targetDb, tableName)) {
    return true;
  }

  console.log(`Creating new table: ${tableName}`);
  const createTableSQL = sourceDb.prepare(`
    SELECT sql FROM sqlite_master 
    WHERE type='table' AND name = ?
  `).get(tableName) as any;
  
  if (createTableSQL?.sql) {
    targetDb.exec(createTableSQL.sql);
    console.log('Table structure created');
    return true;
  } else {
    console.log(`Warning: Could not get table structure for ${tableName}, skipping...`);
    return false;
  }
}

function insertTableData(targetDb: Database.Database, tableName: string): number {
  const result = targetDb.prepare(`
    INSERT INTO ${tableName} 
    SELECT * FROM source_db.${tableName}
  `).run();
  
  const rowsAdded = result.changes || 0;
  console.log(`Added ${rowsAdded} rows from ${tableName}`);
  return rowsAdded;
}

function processTable(sourceDb: Database.Database, targetDb: Database.Database, table: TableInfo, file2Name: string): number {
  const tableName = table.name;
  console.log(`Processing ${file2Name} table: ${tableName}`);
  
  try {
    // Create table if it doesn't exist
    if (!createTableIfNotExists(sourceDb, targetDb, tableName)) {
      return 0; // Skip if table creation failed
    }
    
    // Insert data
    return insertTableData(targetDb, tableName);
    
  } catch (err) {
    console.log(`Error with table ${tableName}: ${err}`);
    return 0;
  }
}

try {
  if (!fs.existsSync(file1)) {
    const error = new Error(`${file1} file missing`);
    (error as any).status = StatusCodes.BAD_REQUEST;
    throw error;
  }
  if (!fs.existsSync(file2)) {
    const error = new Error(`${file2} file missing`);
    (error as any).status = StatusCodes.BAD_REQUEST;
    throw error;
  }

  const size1 = (fs.statSync(file1).size / BYTES_TO_MB).toFixed(2);
  const size2 = (fs.statSync(file2).size / BYTES_TO_MB).toFixed(2);
  const file1Name = file1.split('/').pop()?.replace(/\.gpkg$/i, '') || 'file1';
  const file2Name = file2.split('/').pop()?.replace(/\.gpkg$/i, '') || 'file2';
  console.log(`${file1Name} dataset size: ${size1} MB`);
  console.log(`${file2Name} dataset size: ${size2} MB`);

  console.log(`Step 2: Copy base file from ${file1} to ${output}...`);
  if (fs.existsSync(output)) {
    const timestamp = Date.now();
    const newOutput = output.replace('.gpkg', `_${timestamp}.gpkg`);
    console.log(`Output file exists, creating: ${newOutput}`);
    fs.copyFileSync(file1, newOutput);
    output = newOutput;
  } else {
    fs.copyFileSync(file1, output);
  }
  console.log(`Base file copied (${file1Name} as base)`);

  console.log(`Step 3: Open databases (${file1Name} and ${file2Name})...`);
  const targetDb = new Database(output);
  const sourceDb = new Database(file2, { readonly: true });
  console.log(`Databases opened (${file1Name} and ${file2Name})`);

  console.log(`Step 4: Get table information from ${file1Name} and ${file2Name}...`);
  
  const targetTables = getDataTables(targetDb);
  const sourceTables = getDataTables(sourceDb);
  
  console.log(`${file1Name} has ${targetTables.length} data tables`);
  for (const table of targetTables) {
    const count = targetDb.prepare(`SELECT COUNT(*) as count FROM ${table.name}`).get() as CountResult;
    console.log(`   - ${table.name}: ${count.count} rows`);
  }
  
  console.log(`${file2Name} has ${sourceTables.length} data tables`);
  for (const table of sourceTables) {
    const count = sourceDb.prepare(`SELECT COUNT(*) as count FROM ${table.name}`).get() as CountResult;
    console.log(`   - ${table.name}: ${count.count} rows`);
  }

  console.log('Step 5: Attach and merge different datasets...');
  targetDb.exec(`ATTACH DATABASE '${file2}' AS source_db`);
  
  let totalMerged = 0;
  for (const table of sourceTables) {
    const rowsAdded = processTable(sourceDb, targetDb, table, file2Name);
    totalMerged += rowsAdded;
  }

  targetDb.exec('DETACH DATABASE source_db');
  sourceDb.close();
  targetDb.close();
  
  const finalSize = (fs.statSync(output).size / BYTES_TO_MB).toFixed(2);
  console.log(`\n${file1Name.toUpperCase()} + ${file2Name.toUpperCase()} MERGE COMPLETE!`);
  console.log(`Total rows added: ${totalMerged}`);
  console.log(`Output: ${output} (${finalSize} MB)`);
  console.log(`Combined: ${file1Name} + ${file2Name} datasets!`);

} catch (error: any) {
  console.error('Failed:', error.message);
  const exitCode = error.status === StatusCodes.BAD_REQUEST ? 1 : 2;
  process.exit(exitCode);
}