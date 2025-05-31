import * as fs from 'fs';
import Database from 'better-sqlite3';

console.log("🚨 GeoPackage Merge - SYRIA + BLUE MARBLE");

const file1 = './data/Syria.gpkg';
const file2 = './data/blueMarble.gpkg';
const output = './merged_syria_bluemarble.gpkg';

try {
  console.log('Step 1: Validate new GeoPackage files...');
  if (!fs.existsSync(file1)) throw new Error('Syria file missing');
  if (!fs.existsSync(file2)) throw new Error('Blue Marble file missing');
  
  const size1 = (fs.statSync(file1).size / 1024 / 1024).toFixed(2);
  const size2 = (fs.statSync(file2).size / 1024 / 1024).toFixed(2);
  console.log(`✅ Syria dataset: ${size1} MB`);
  console.log(`✅ Blue Marble dataset: ${size2} MB`);

  console.log('Step 2: Copy base file...');
  if (fs.existsSync(output)) fs.unlinkSync(output);
  fs.copyFileSync(file1, output);
  console.log('✅ Base file copied (Syria as base)');

  console.log('Step 3: Open databases...');
  const targetDb = new Database(output);
  const sourceDb = new Database(file2, { readonly: true });
  console.log('✅ Databases opened');

  console.log('Step 4: Analyze both datasets...');
  
  // Get tables from both databases
  const targetTables = targetDb.prepare(`
    SELECT name FROM sqlite_master 
    WHERE type = 'table' 
    AND name NOT LIKE 'sqlite_%' 
    AND name NOT LIKE 'gpkg_%'
    AND name NOT LIKE 'rtree_%'
  `).all();
  
  const sourceTables = sourceDb.prepare(`
    SELECT name FROM sqlite_master 
    WHERE type = 'table' 
    AND name NOT LIKE 'sqlite_%' 
    AND name NOT LIKE 'gpkg_%'
    AND name NOT LIKE 'rtree_%'
  `).all();
  
  console.log(`📊 Syria has ${targetTables.length} data tables`);
  for (const table of targetTables) {
    const count = targetDb.prepare(`SELECT COUNT(*) as count FROM ${(table as any).name}`).get() as any;
    console.log(`   - ${(table as any).name}: ${count.count} rows`);
  }
  
  console.log(`📊 Blue Marble has ${sourceTables.length} data tables`);
  for (const table of sourceTables) {
    const count = sourceDb.prepare(`SELECT COUNT(*) as count FROM ${(table as any).name}`).get() as any;
    console.log(`   - ${(table as any).name}: ${count.count} rows`);
  }

  console.log('Step 5: Attach and merge different datasets...');
  targetDb.exec(`ATTACH DATABASE '${file2}' AS source_db`);
  
  let totalMerged = 0;
  for (const table of sourceTables) {
    try {
      const tableName = (table as any).name;
      console.log(`🔄 Processing Blue Marble table: ${tableName}`);
      
      // Check if table exists in target
      const tableExists = targetDb.prepare(`
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name = ?
      `).get(tableName);
      
      if (!tableExists) {
        // Create table structure from source
        console.log(`🆕 Creating new table: ${tableName}`);
        const createTableSQL = sourceDb.prepare(`
          SELECT sql FROM sqlite_master 
          WHERE type='table' AND name = ?
        `).get(tableName) as any;
        
        if (createTableSQL?.sql) {
          targetDb.exec(createTableSQL.sql);
          console.log(`✅ Table structure created`);
        }
      }
      
      // Insert data (different datasets shouldn't have ID conflicts)
      const result = targetDb.prepare(`
        INSERT INTO ${tableName} 
        SELECT * FROM source_db.${tableName}
      `).run();
      
      console.log(`✅ Added ${result.changes} rows from ${tableName}`);
      totalMerged += result.changes || 0;
      
    } catch (err) {
      console.log(`⚠️ Error with table ${(table as any).name}: ${err}`);
    }
  }

  targetDb.exec('DETACH DATABASE source_db');
  sourceDb.close();
  targetDb.close();
  
  const finalSize = (fs.statSync(output).size / 1024 / 1024).toFixed(2);
  console.log(`\n🎉 SYRIA + BLUE MARBLE MERGE COMPLETE!`);
  console.log(`📊 Total rows added: ${totalMerged}`);
  console.log(`📍 Output: ${output} (${finalSize} MB)`);
  console.log(`🌍 Combined: Syria geographic data + Blue Marble imagery!`);

} catch (error: any) {
  console.error('❌ Failed:', error.message);
}