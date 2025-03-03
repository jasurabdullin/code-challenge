const { Pool } = require('pg');


function createDatabasePool() {
  const pool = new Pool({
    host: process.env.DB_HOST || 'db',
    port: process.env.DB_PORT || '5432',
    user: process.env.DB_USER || 'user',
    password: process.env.DB_PASSWORD || 'pass',
    database: process.env.DB_NAME || 'actifai',
    max: Number.parseInt(process.env.DB_POOL_MAX || '20', 10),
    idleTimeoutMillis: Number.parseInt(process.env.DB_IDLE_TIMEOUT || '30000', 10)
  });

  
  pool.on('error', (err, client) => {
    console.error('Unexpected error on idle client', err);
  });

  pool.on('connect', (client) => {
    console.log('New client connected to database');
  });

  return pool;
}


async function initializeDatabase() {
  const pool = createDatabasePool();
  
  try {
    await pool.query('SELECT NOW()');
    console.log('Database connection established successfully');
    return pool;
  } catch (err) {
    console.error('Error connecting to the database:', err);
    process.exit(1);
  }
}

module.exports = {
  initializeDatabase
};
