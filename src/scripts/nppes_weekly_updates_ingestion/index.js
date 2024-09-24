import fs from 'fs';
import csv from 'csv-parser';
import path from 'path';
import { fileURLToPath } from 'url';
import { ConsoleLogger } from "../../common/logger.js";
const logger = new ConsoleLogger();

// Get the directory name of the current module
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Data files
const dataInputFile = path.join(__dirname, 'update_files', 'npidata_pfile_20240916-20240922.csv');
const dataHeaderFile = path.join(__dirname, 'update_files', 'npidata_pfile_20240916-20240922_fileheader.csv');
// Local files
const localInputFile = path.join(__dirname, 'update_files', 'pl_pfile_20240916-20240922.csv');
const localHeaderFile = path.join(__dirname, 'update_files', 'pl_pfile_20240916-20240922_fileheader.csv');

function readCSV(filePath, headers) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv({ headers, skipLines: 1 }))
      .on('data', (data) => results.push(data))
      .on('end', () => resolve(results))
      .on('error', (error) => reject(error));
  });
}

async function main() {
  try {

    // Read headers from the header file
    const dataHeaderContent = await fs.promises.readFile(dataHeaderFile, 'utf-8');
    const dataHeaders = dataHeaderContent.trim().split(',').map(header => header.replace(/"/g, ''));
    const localHeaderContent = await fs.promises.readFile(localHeaderFile, 'utf-8');
    const localHeaders = localHeaderContent.trim().split(',').map(header => header.replace(/"/g, ''));

    // Read and parse the main CSV file
    const dataJsonArray = await readCSV(dataInputFile, dataHeaders);
    const dataCleanedJsonArray = dataJsonArray.map(obj => 
      Object.fromEntries(
        Object.entries(obj).filter(([_, value]) => value !== '')
      )
    );
    const dataMap = new Map(dataCleanedJsonArray.map(obj => [obj.NPI, obj]));

    const localJsonArray = await readCSV(localInputFile, localHeaders);
    const localCleanedJsonArray = localJsonArray.map(obj => 
      Object.fromEntries(
        Object.entries(obj).filter(([_, value]) => value !== '')
      )
    );
    const localMap = new Map(localCleanedJsonArray.map(obj => [obj.NPI, obj]));
    
    // Merge data from localMap into dataMap where NPIs match
    let mergedCount = 0;
    for (const [npi, localData] of localMap) {
      if (dataMap.has(npi)) {
        const dataRecord = dataMap.get(npi);
        // Append local data to the existing data record
        Object.assign(dataRecord, localData);
        // Update the record in dataMap
        dataMap.set(npi, dataRecord);

        const taxonomy = dataRecord['PROVIDER TAXONOMY CODE'];


        mergedCount++;
      }
    }

    // Output the result
    logger.logBasic(`[main] Total records: ${dataCleanedJsonArray.length}`);
    logger.logBasic(`[main] Merged records: ${mergedCount}`);
  } catch (error) {
    logger.logError('[main] Error processing CSV file:', error);
  }
}

main();
