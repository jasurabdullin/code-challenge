/**
 * Builds a parameterized WHERE clause from conditions
 * @param {Array} conditions - Array of condition strings
 * @returns {String} - Joined WHERE clause
 */
function buildWhereClause(conditions) {
  return conditions.length > 0 ? ` WHERE ${conditions.join(' AND ')}` : '';
}

/**
 * Builds pagination parameters
 * @param {Number} page - Page number
 * @param {Number} limit - Items per page
 * @returns {Object} - Pagination parameters
 */
function getPaginationParams(page, limit) {
  const parsedPage = Number.parseInt(page, 10) || 1;
  const parsedLimit = Number.parseInt(limit, 10) || 100;
  const offset = (parsedPage - 1) * parsedLimit;
  
  return {
    limit: parsedLimit,
    offset,
    page: parsedPage
  };
}

/**
 * Formats a response with pagination metadata and HATEOAS links
 * @param {Array} data - Result data
 * @param {Object} pagination - Pagination parameters
 * @param {Number} total - Total count of items
 * @param {String} baseUrl - Base URL for pagination links
 * @param {Object} additionalMeta - Additional metadata to include
 * @returns {Object} - Formatted response
 */
function formatPaginatedResponse(data, pagination, total, baseUrl, additionalMeta = {}) {
  const totalPages = Math.ceil(total / pagination.limit);
  
  return {
    data,
    meta: {
      pagination: {
        page: pagination.page,
        limit: pagination.limit,
        total,
        pages: totalPages
      },
      ...additionalMeta
    },
    links: {
      self: `${baseUrl}?page=${pagination.page}&limit=${pagination.limit}`,
      first: `${baseUrl}?page=1&limit=${pagination.limit}`,
      last: `${baseUrl}?page=${totalPages}&limit=${pagination.limit}`,
      next: pagination.page < totalPages ? 
        `${baseUrl}?page=${pagination.page + 1}&limit=${pagination.limit}` : null,
      prev: pagination.page > 1 ? 
        `${baseUrl}?page=${pagination.page - 1}&limit=${pagination.limit}` : null
    }
  };
}

/**
 * Safely parses a date string with fallback
 * @param {String} dateStr - Date string to parse
 * @param {String} fallback - Fallback date if parsing fails
 * @returns {String} - Valid date string
 */
function parseDate(dateStr, fallback) {
  try {
    const date = new Date(dateStr);
    if (Number.isNaN(date.getTime())) {
      return fallback;
    }
    return dateStr;
  } catch (e) {
    return fallback;
  }
}

module.exports = {
  buildWhereClause,
  getPaginationParams,
  formatPaginatedResponse,
  parseDate
};
