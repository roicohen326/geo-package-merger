# GeoPackage Merge Tool

## How It Works

1. **Validates** input files exist
2. **Copies** first file as base database
3. **Opens** both databases (source as read-only)
4. **Merges** all data tables from second file into first
5. **Creates** new tables if they don't exist
6. **Outputs** final merged database

## Output Information

The tool displays:
- File sizes of input databases
- Tables being processed
- Total rows merged
- Final output file size and location

## Error Handling

- **Missing files**: Clear error message with missing file paths
- **Database errors**: Detailed error information for troubleshooting
- **Permission issues**: File access error reporting

## Troubleshooting

**"npm: Command not found"**
- Install Node.js from [nodejs.org](https://nodejs.org)

**"Cannot find module"**
```bash
npm install
```

## Dependencies

- `better-sqlite3` - SQLite database interface
- `http-status-codes` - HTTP status code constants
- `typescript` - TypeScript compiler (dev)
- `ts-node` - TypeScript execution engine (dev)

## Requirements

- GeoPackage files must be valid SQLite databases
- Sufficient disk space for output file
- Read access to input files
- Write access to output directory
