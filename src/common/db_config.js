import pg from 'pg';
import dotenv from 'dotenv';
import { ConsoleLogger } from './logger.js';
dotenv.config();

const { Pool } = pg;
const logger = new ConsoleLogger();

export const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});

// Test the connection
pool.connect((err, client, release) => {
  if (err) {
    logger.logError('Error acquiring client', err.stack);
    process.exit(1);
  }
  client.query('SELECT NOW()', (err, result) => {
    release();
    if (err) {
      logger.logError('Error executing query', err.stack);
      process.exit(1);
    }
    logger.logBasic('Connected to Database successfully');
  });
});