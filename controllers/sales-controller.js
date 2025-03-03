const { 
  getPaginationParams, 
  formatPaginatedResponse, 
  parseDate
} = require('../lib/utils');
const {
  validateSortColumn,
  validateSortOrder
} = require('../lib/validations');

/**
 * List all sales with pagination and filtering
 * 
 * @param {Object} req - Express request object
 * @param {Object} req.query - Query parameters
 * @param {string} [req.query.startDate] - Start date filter (YYYY-MM-DD)
 * @param {string} [req.query.endDate] - End date filter (YYYY-MM-DD)
 * @param {string} [req.query.userId] - Filter by user ID
 * @param {string} [req.query.groupId] - Filter by group ID
 * @param {number} [req.query.minAmount] - Minimum sale amount
 * @param {number} [req.query.maxAmount] - Maximum sale amount
 * @param {number} [req.query.page] - Page number for pagination
 * @param {number} [req.query.limit] - Number of items per page
 * @param {string} [req.query.sortBy] - Field to sort by (date, amount, user_id)
 * @param {string} [req.query.sortOrder] - Sort direction (asc, desc)
 * @param {Object} res - Express response object
 * @param {Object} pool - Database connection pool
 */
async function listSales(req, res, pool) {
  try {
    const { 
      page, 
      limit, 
      startDate, 
      endDate, 
      userId, 
      groupId,
      minAmount,
      maxAmount,
      sortBy,
      sortOrder
    } = req.query;
    
    const pagination = getPaginationParams(page, limit);

    const validStartDate = parseDate(startDate, null);
    const validEndDate = parseDate(endDate, null);

    const validSortColumns = ['date', 'amount', 'user_id'];
    const validSortBy = validateSortColumn(sortBy, validSortColumns, 'date');
    const validSortOrder = validateSortOrder(sortOrder, 'desc');
    
    const validMinAmount = minAmount ? Number.parseFloat(minAmount) : null;
    const validMaxAmount = maxAmount ? Number.parseFloat(maxAmount) : null;
    
    const conditions = [];
    const queryParams = [];
    let paramIndex = 1;
    
    if (validStartDate) {
      conditions.push(`s.date >= $${paramIndex}`);
      queryParams.push(validStartDate);
      paramIndex++;
    }
    
    if (validEndDate) {
      conditions.push(`s.date <= $${paramIndex}`);
      queryParams.push(validEndDate);
      paramIndex++;
    }
    
    if (userId) {
      conditions.push(`s.user_id = $${paramIndex}`);
      queryParams.push(userId);
      paramIndex++;
    }
    
    if (validMinAmount !== null) {
      conditions.push(`s.amount >= $${paramIndex}`);
      queryParams.push(validMinAmount);
      paramIndex++;
    }
    
    if (validMaxAmount !== null) {
      conditions.push(`s.amount <= $${paramIndex}`);
      queryParams.push(validMaxAmount);
      paramIndex++;
    }
    
    // Build the base query
    let query = `
      SELECT 
        s.id,
        s.user_id,
        u.name as user_name,
        s.amount,
        s.date
      FROM 
        sales s
        JOIN users u ON s.user_id = u.id
    `;
    
    // Add group filter if provided
    if (groupId) {
      query += " JOIN user_groups ug ON s.user_id = ug.user_id";
      conditions.push(`ug.group_id = $${paramIndex}`);
      queryParams.push(groupId);
      paramIndex++;
    }
    
    // Add WHERE clause if conditions exist
    if (conditions.length > 0) {
      query += ` WHERE ${conditions.join(' AND ')}`;
    }
    
    // Add ORDER BY
    query += ` ORDER BY s.${validSortBy} ${validSortOrder}`;
    
    // Add pagination
    query += ` LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`;
    queryParams.push(pagination.limit, pagination.offset);
    
    const result = await pool.query(query, queryParams);
    
    // Get total count for pagination
    let countQuery = `
      SELECT COUNT(*) 
      FROM sales s
    `;
    
    if (groupId) {
      countQuery += " JOIN user_groups ug ON s.user_id = ug.user_id";
    }
    
    if (conditions.length > 0) {
      countQuery += ` WHERE ${conditions.join(' AND ')}`;
    }
    
    const countResult = await pool.query(countQuery, queryParams.slice(0, paramIndex - 1));
    const total = Number.parseInt(countResult.rows[0].count, 10);
    
    res.json(formatPaginatedResponse(
      result.rows,
      pagination,
      total,
      '/api/metrics/sales',
      {
        filters: {
          startDate: validStartDate,
          endDate: validEndDate,
          userId,
          groupId,
          minAmount: validMinAmount,
          maxAmount: validMaxAmount
        }
      }
    ));
  } catch (error) {
    console.error("Error listing sales:", error);
    res.status(500).json({ error: "Internal server error" });
  }
}

/**
 * Get a specific sale by ID
 * 
 * @param {Object} req - Express request object
 * @param {Object} req.params - URL parameters
 * @param {string} req.params.id - Sale ID
 * @param {Object} res - Express response object
 * @param {Object} pool - Database connection pool
 */
async function getSale(req, res, pool) {
  try {
    const saleId = req.params.id;
    
    if (!saleId) {
      return res.status(400).json({ error: "Sale ID is required" });
    }
    
    // Query for the sale with user details
    const query = `
      SELECT 
        s.id,
        s.user_id,
        u.name as user_name,
        u.role as user_role,
        s.amount,
        s.date
      FROM 
        sales s
        JOIN users u ON s.user_id = u.id
      WHERE 
        s.id = $1
    `;
    
    const result = await pool.query(query, [saleId]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Sale not found" });
    }
    
    // Get groups that the user belongs to
    const groupsQuery = `
      SELECT 
        g.id,
        g.name
      FROM 
        groups g
        JOIN user_groups ug ON g.id = ug.group_id
      WHERE 
        ug.user_id = $1
    `;
    
    const groupsResult = await pool.query(groupsQuery, [result.rows[0].user_id]);
    
    res.json({
      data: {
        ...result.rows[0],
        user_groups: groupsResult.rows
      },
      links: {
        self: `/api/metrics/sales/${saleId}`,
        user: `/api/metrics/users/${result.rows[0].user_id}`,
        all_sales: '/api/metrics/sales'
      }
    });
  } catch (error) {
    console.error('Error fetching sale:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
}

module.exports = {
  listSales,
  getSale
}; 