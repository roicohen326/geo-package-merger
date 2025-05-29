import * as fs from 'fs';
import Database from 'better-sqlite3';

console.log("🚨 GeoPackage Merge with SQLite");

const file1 = './data/test_north.gpkg';
const file2 = './data/test_south.gpkg';
const output = './merged_complete.gpkg';

try {
  console.log('Step 1: Validate files...');
  if (!fs.existsSync(file1)) throw new Error('File 1 missing');
  if (!fs.existsSync(file2)) throw new Error('File 2 missing');
  console.log('✅ Files validated');

  console.log('Step 2: Copy base file...');
  if (fs.existsSync(output)) fs.unlinkSync(output);
  fs.copyFileSync(file1, output);
  console.log('✅ Base file copied');

  console.log('Step 3: Open databases...');
  const targetDb = new Database(output);
  const sourceDb = new Database(file2, { readonly: true });
  console.log('✅ Databases opened');

  console.log('Step 4: Get table info...');
  const tables = sourceDb.prepare(`
    SELECT name FROM sqlite_master 
    WHERE type = 'table' 
    AND name NOT LIKE 'sqlite_%' 
    AND name NOT LIKE 'gpkg_%'
  `).all();
  
  console.log(`Found ${tables.length} data tables to merge`);

  console.log('Step 5: Attach and merge...');
  targetDb.exec(`ATTACH DATABASE '${file2}' AS source_db`);
  
  let totalMerged = 0;
  for (const table of tables) {
    try {
      const tableName = (table as any).name;
      console.log(`Processing table: ${tableName}`);
      
      const result = targetDb.prepare(`
        INSERT OR IGNORE INTO ${tableName} 
        SELECT * FROM source_db.${tableName}
      `).run();
      
      console.log(`✅ Merged ${result.changes} rows from ${tableName}`);
      totalMerged += result.changes || 0;
      
    } catch (err) {
      console.log(`⚠️ Skipped table: ${err}`);
    }
  }

  targetDb.exec('DETACH DATABASE source_db');
  sourceDb.close();
  targetDb.close();
  
  console.log(`🎉 MERGE COMPLETE! Total rows merged: ${totalMerged}`);
  console.log(`Output: ${output}`);

} catch (error: any) {
  console.error('❌ Failed:', error.message);
}