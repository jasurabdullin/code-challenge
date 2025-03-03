/**
 * Validates and returns a sort column from a list of valid options
 * @param {String} sortBy - Column to sort by
 * @param {Array} validColumns - Array of valid column names
 * @param {String} defaultColumn - Default column if sortBy is invalid
 * @returns {String} - Valid sort column
 */
function validateSortColumn(sortBy, validColumns, defaultColumn) {
  return sortBy && validColumns.includes(sortBy) ? sortBy : defaultColumn;
}

/**
 * Validates and returns a sort order
 * @param {String} sortOrder - Order to sort by (asc or desc)
 * @param {String} defaultOrder - Default order if sortOrder is invalid
 * @returns {String} - Valid sort order
 */
function validateSortOrder(sortOrder, defaultOrder = 'asc') {
  const validOrders = ['asc', 'desc'];
  return sortOrder && validOrders.includes(sortOrder.toLowerCase()) ? 
    sortOrder.toLowerCase() : defaultOrder;
}

/**
 * Validates and returns a time interval from a list of valid options
 * @param {String} interval - Time interval (day, week, month, quarter, year)
 * @param {String} defaultInterval - Default interval if provided interval is invalid
 * @returns {String} - Valid time interval
 */
function validateInterval(interval, defaultInterval = 'month') {
  const validIntervals = ['day', 'week', 'month', 'quarter', 'year'];
  return interval && validIntervals.includes(interval) ? interval : defaultInterval;
}

/**
 * Validates that a resource exists
 * @param {Object} pool - Database connection pool
 * @param {String} table - Table name
 * @param {Number} id - Resource ID
 * @returns {Promise<Object>} - Resource if found
 * @throws {Error} - If resource not found
 */
async function validateResourceExists(pool, table, id) {
  const result = await pool.query(`SELECT * FROM ${table} WHERE id = $1`, [id]);
  if (result.rows.length === 0) {
    const error = new Error(`${table.charAt(0).toUpperCase() + table.slice(1, -1)} not found`);
    error.statusCode = 404;
    throw error;
  }
  return result.rows[0];
}

module.exports = {
  validateResourceExists,
  validateSortColumn,
  validateSortOrder,
  validateInterval
};