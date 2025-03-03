const { 
  getPaginationParams, 
  formatPaginatedResponse, 
  parseDate,
} = require('../lib/utils');
const { 
  validateResourceExists,
  validateSortColumn,
  validateSortOrder,
  validateInterval
} = require('../lib/validations');


/**
 * Get users in a specific group
 * 
 * @param {Object} req - Express request object
 * @param {Object} req.params - URL parameters
 * @param {string} req.params.id - Group ID
 * @param {Object} req.query - Query parameters
 * @param {number} [req.query.page] - Page number for pagination
 * @param {number} [req.query.limit] - Number of items per page
 * @param {string} [req.query.sortBy] - Field to sort by (name, role)
 * @param {string} [req.query.sortOrder] - Sort direction (asc, desc)
 * @param {string} [req.query.role] - Filter by user role
 * @param {Object} res - Express response object
 * @param {Object} pool - Database connection pool
 */
async function getGroupUsers(req, res, pool) {
  try {
    const groupId = req.params.id;
    if (!groupId) {
      return res.status(400).json({ error: 'Group ID is required' });
    }
    
    const { page, limit, sortBy, sortOrder, role } = req.query;
    
    // Validate group exists
    await validateResourceExists(pool, 'groups', groupId);
    
    // Validate sort parameters
    const validSortColumns = ['name', 'role'];
    const validSortBy = validateSortColumn(sortBy, validSortColumns, 'name');
    const validSortOrder = validateSortOrder(sortOrder, 'asc');
    
    // Build query conditions
    const conditions = ["ug.group_id = $1"];
    const queryParams = [groupId];
    let paramIndex = 2;
    
    if (role) {
      conditions.push(`u.role = $${paramIndex}`);
      queryParams.push(role);
      paramIndex++;
    }
    
    // Get pagination parameters
    const pagination = getPaginationParams(page, limit);
    
    // Query for group users
    const query = `
      SELECT 
        u.id,
        u.name,
        u.role
      FROM 
        users u
        JOIN user_groups ug ON u.id = ug.user_id
      WHERE ${conditions.join(' AND ')}
      ORDER BY u.${validSortBy} ${validSortOrder}
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
    
    queryParams.push(pagination.limit, pagination.offset);
    
    const result = await pool.query(query, queryParams);
    
    // Get total count for pagination
    const countQuery = `
      SELECT COUNT(*) 
      FROM users u
      JOIN user_groups ug ON u.id = ug.user_id
      WHERE ${conditions.join(' AND ')}
    `;
    
    const countResult = await pool.query(countQuery, queryParams.slice(0, paramIndex - 1));
    const total = Number.parseInt(countResult.rows[0].count, 10);
    
    // Format response
    res.json(formatPaginatedResponse(
      result.rows,
      pagination,
      total,
      `/api/metrics/groups/${groupId}/users`,
      {
        filters: {
          role
        }
      }
    ));
  } catch (error) {
    console.error('Error fetching group users:', error);
    if (error.statusCode) {
      res.status(error.statusCode).json({ error: error.message });
    } else {
      res.status(500).json({ error: 'Internal server error' });
    }
  }
}

/**
 * Get sales for a specific group
 * 
 * @param {Object} req - Express request object
 * @param {Object} req.params - URL parameters
 * @param {string} req.params.id - Group ID
 * @param {Object} req.query - Query parameters
 * @param {string} [req.query.startDate] - Start date filter (YYYY-MM-DD)
 * @param {string} [req.query.endDate] - End date filter (YYYY-MM-DD)
 * @param {number} [req.query.page] - Page number for pagination
 * @param {number} [req.query.limit] - Number of items per page
 * @param {string} [req.query.sortBy] - Field to sort by (date, amount)
 * @param {string} [req.query.sortOrder] - Sort direction (asc, desc)
 * @param {string} [req.query.userId] - Filter by user ID
 * @param {Object} res - Express response object
 * @param {Object} pool - Database connection pool
 */
async function getGroupSales(req, res, pool) {
  try {
    const groupId = req.params.id;
    if (!groupId) {
      return res.status(400).json({ error: 'Group ID is required' });
    }
    
    const { startDate, endDate, page, limit, sortBy, sortOrder, userId } = req.query;
    
    // Validate group exists
    await validateResourceExists(pool, 'groups', groupId);
    
    // Validate dates
    const validStartDate = parseDate(startDate, null);
    const validEndDate = parseDate(endDate, null);
    
    // Validate sort parameters
    const validSortColumns = ['date', 'amount', 'user_id'];
    const validSortBy = validateSortColumn(sortBy, validSortColumns, 'date');
    const validSortOrder = validateSortOrder(sortOrder, 'desc');
    
    // Build query conditions
    const conditions = ["ug.group_id = $1"];
    const queryParams = [groupId];
    let paramIndex = 2;
    
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
    
    // Get pagination parameters
    const pagination = getPaginationParams(page, limit);
    
    // Query for group sales
    const query = `
      SELECT 
        s.id,
        s.user_id,
        u.name as user_name,
        s.amount,
        s.date
      FROM 
        sales s
        JOIN user_groups ug ON s.user_id = ug.user_id
        JOIN users u ON s.user_id = u.id
      WHERE ${conditions.join(' AND ')}
      ORDER BY s.${validSortBy} ${validSortOrder}
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
    
    queryParams.push(pagination.limit, pagination.offset);
    
    const result = await pool.query(query, queryParams);
    
    // Get total count for pagination
    const countQuery = `
      SELECT COUNT(*) 
      FROM sales s
      JOIN user_groups ug ON s.user_id = ug.user_id
      WHERE ${conditions.join(' AND ')}
    `;
    
    const countResult = await pool.query(countQuery, queryParams.slice(0, paramIndex - 1));
    const total = Number.parseInt(countResult.rows[0].count, 10);
    
    // Format response
    res.json(formatPaginatedResponse(
      result.rows,
      pagination,
      total,
      `/api/metrics/groups/${groupId}/sales`,
      {
        filters: {
          startDate: validStartDate,
          endDate: validEndDate,
          userId
        }
      }
    ));
  } catch (error) {
    console.error('Error fetching group sales:', error);
    if (error.statusCode) {
      res.status(error.statusCode).json({ error: error.message });
    } else {
      res.status(500).json({ error: 'Internal server error' });
    }
  }
}

/**
 * Get sales summary for a specific group
 * 
 * @param {Object} req - Express request object
 * @param {Object} req.params - URL parameters
 * @param {string} req.params.id - Group ID
 * @param {Object} req.query - Query parameters
 * @param {string} [req.query.startDate] - Start date filter (YYYY-MM-DD)
 * @param {string} [req.query.endDate] - End date filter (YYYY-MM-DD)
 * @param {Object} res - Express response object
 * @param {Object} pool - Database connection pool
 */
async function getGroupSalesSummary(req, res, pool) {
  try {
    const groupId = req.params.id;
    if (!groupId) {
      return res.status(400).json({ error: 'Group ID is required' });
    }
    
    const { startDate, endDate } = req.query;
    
    // Validate group exists
    await validateResourceExists(pool, 'groups', groupId);
    
    // Validate dates
    const validStartDate = parseDate(startDate, null);
    const validEndDate = parseDate(endDate, null);
    
    // Build query conditions
    const conditions = ["ug.group_id = $1"];
    const queryParams = [groupId];
    let paramIndex = 2;
    
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
    
    // Query for group sales summary
    const query = `
      SELECT 
        COUNT(*) AS total_sales,
        SUM(s.amount) AS total_revenue,
        AVG(s.amount) AS average_sale,
        MIN(s.amount) AS min_sale,
        MAX(s.amount) AS max_sale,
        COUNT(DISTINCT s.user_id) AS active_users
      FROM 
        sales s
        JOIN user_groups ug ON s.user_id = ug.user_id
      WHERE ${conditions.join(' AND ')}
    `;
    
    const result = await pool.query(query, queryParams);
    
    // Query for sales by user
    const userSalesQuery = `
      SELECT 
        u.id AS user_id,
        u.name AS user_name,
        u.role,
        COUNT(*) AS sales_count,
        SUM(s.amount) AS total_revenue,
        AVG(s.amount) AS average_sale
      FROM 
        sales s
        JOIN user_groups ug ON s.user_id = ug.user_id
        JOIN users u ON s.user_id = u.id
      WHERE ${conditions.join(' AND ')}
      GROUP BY 
        u.id, u.name, u.role
      ORDER BY 
        total_revenue DESC
    `;
    
    const userSalesResult = await pool.query(userSalesQuery, queryParams);
    
    res.json({
      data: {
        summary: result.rows[0],
        user_breakdown: userSalesResult.rows
      },
      meta: {
        filters: {
          startDate: validStartDate,
          endDate: validEndDate
        }
      },
      links: {
        self: `/api/metrics/groups/${groupId}/sales/summary`,
        sales: `/api/metrics/groups/${groupId}/sales`
      }
    });
  } catch (error) {
    console.error('Error fetching group sales summary:', error);
    if (error.statusCode) {
      res.status(error.statusCode).json({ error: error.message });
    } else {
      res.status(500).json({ error: 'Internal server error' });
    }
  }
}

/**
 * Get performance metrics for a specific group
 * 
 * @param {Object} req - Express request object
 * @param {Object} req.params - URL parameters
 * @param {string} req.params.id - Group ID
 * @param {Object} req.query - Query parameters
 * @param {string} [req.query.startDate] - Start date filter (YYYY-MM-DD)
 * @param {string} [req.query.endDate] - End date filter (YYYY-MM-DD)
 * @param {string} [req.query.interval] - Time interval for trends (day, week, month, quarter, year)
 * @param {Object} res - Express response object
 * @param {Object} pool - Database connection pool
 */
async function getGroupPerformance(req, res, pool) {
  try {
    const groupId = req.params.id;
    if (!groupId) {
      return res.status(400).json({ error: 'Group ID is required' });
    }
    
    const { startDate, endDate, interval } = req.query;
    
    // Validate group exists
    await validateResourceExists(pool, 'groups', groupId);
    
    // Validate dates
    const validStartDate = parseDate(startDate, '2021-01-01');
    const validEndDate = parseDate(endDate, '2021-12-31');
    
    // Validate interval
    const validInterval = validateInterval(interval, 'month');  
    
    // Query for performance over time
    const trendsQuery = `
      SELECT 
        DATE_TRUNC($1, s.date) AS time_period,
        COUNT(*) AS sales_count,
        SUM(s.amount) AS total_revenue,
        AVG(s.amount) AS average_revenue,
        COUNT(DISTINCT s.user_id) AS active_users
      FROM 
        sales s
        JOIN user_groups ug ON s.user_id = ug.user_id
      WHERE 
        ug.group_id = $2 AND
        s.date BETWEEN $3 AND $4
      GROUP BY 
        DATE_TRUNC($1, s.date)
      ORDER BY 
        time_period
    `;
    
    const trendsResult = await pool.query(trendsQuery, [
      validInterval, 
      groupId, 
      validStartDate, 
      validEndDate
    ]);
    
    // Query for top performers in the group
    const topPerformersQuery = `
      SELECT 
        u.id,
        u.name,
        u.role,
        COUNT(*) AS sales_count,
        SUM(s.amount) AS total_revenue,
        AVG(s.amount) AS average_revenue,
        RANK() OVER (ORDER BY SUM(s.amount) DESC) AS rank
      FROM 
        sales s
        JOIN user_groups ug ON s.user_id = ug.user_id
        JOIN users u ON s.user_id = u.id
      WHERE 
        ug.group_id = $1 AND
        s.date BETWEEN $2 AND $3
      GROUP BY 
        u.id, u.name, u.role
      ORDER BY 
        total_revenue DESC
      LIMIT 10
    `;
    
    const topPerformersResult = await pool.query(topPerformersQuery, [
      groupId, 
      validStartDate, 
      validEndDate
    ]);
    
    // Query for comparison with other groups
    const comparisonQuery = `
      WITH group_metrics AS (
        SELECT 
          g.id AS group_id,
          g.name AS group_name,
          COUNT(s.id) AS sales_count,
          SUM(s.amount) AS total_revenue,
          AVG(s.amount) AS average_revenue,
          COUNT(DISTINCT s.user_id) AS active_users
        FROM 
          groups g
          JOIN user_groups ug ON g.id = ug.group_id
          JOIN sales s ON ug.user_id = s.user_id
        WHERE 
          s.date BETWEEN $1 AND $2
        GROUP BY 
          g.id, g.name
      )
      SELECT 
        gm.*,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
        RANK() OVER (ORDER BY sales_count DESC) AS sales_rank,
        RANK() OVER (ORDER BY average_revenue DESC) AS avg_revenue_rank
      FROM 
        group_metrics gm
      ORDER BY 
        CASE WHEN gm.group_id = $3 THEN 0 ELSE 1 END,
        total_revenue DESC
    `;
    
    const comparisonResult = await pool.query(comparisonQuery, [
      validStartDate, 
      validEndDate,
      groupId
    ]);
    
    res.json({
      data: {
        trends: trendsResult.rows,
        top_performers: topPerformersResult.rows,
        comparison: comparisonResult.rows
      },
      meta: {
        filters: {
          startDate: validStartDate,
          endDate: validEndDate,
          interval: validInterval
        }
      },
      links: {
        self: `/api/metrics/groups/${groupId}/performance?interval=${validInterval}`,
        group_sales: `/api/metrics/groups/${groupId}/sales`
      }
    });
  } catch (error) {
    console.error('Error fetching group performance:', error);
    if (error.statusCode) {
      res.status(error.statusCode).json({ error: error.message });
    } else {
      res.status(500).json({ error: 'Internal server error' });
    }
  }
}

/**
 * Get performance metrics across all groups
 * 
 * @param {Object} req - Express request object
 * @param {Object} req.query - Query parameters
 * @param {string} [req.query.startDate] - Start date filter (YYYY-MM-DD)
 * @param {string} [req.query.endDate] - End date filter (YYYY-MM-DD)
 * @param {string} [req.query.interval] - Time interval for trends (day, week, month, quarter, year)
 * @param {number} [req.query.limit] - Limit for top groups
 * @param {string} [req.query.sortBy] - Field to sort by (total_revenue, average_revenue, sales_count, active_users, name)
 * @param {string} [req.query.sortOrder] - Sort direction (asc, desc)
 * @param {Object} res - Express response object
 * @param {Object} pool - Database connection pool
 */
async function getAllGroupsPerformance(req, res, pool) {
  try {
    const { 
      startDate, 
      endDate, 
      interval,
      limit,
      sortBy,
      sortOrder
    } = req.query;
    
    // Validate dates
    const validStartDate = parseDate(startDate, '2021-01-01');
    const validEndDate = parseDate(endDate, '2021-12-31');
    
    // Validate interval
    const validIntervals = ['day', 'week', 'month', 'quarter', 'year'];
    const validInterval = interval && validIntervals.includes(interval) ? interval : 'month';
    
    // Validate sort options
    const validSortColumns = ['total_revenue', 'average_revenue', 'sales_count', 'active_users', 'name'];
    const validSortBy = validateSortColumn(sortBy, validSortColumns, 'total_revenue');
    const validSortOrder = validateSortOrder(sortOrder, 'desc');
    
    // Validate limit
    const validLimit = limit ? Number.parseInt(limit, 10) : 10;
    
    // Query for top performing groups
    const topGroupsQuery = `
      SELECT 
        g.id,
        g.name,
        COUNT(s.id) AS sales_count,
        SUM(s.amount) AS total_revenue,
        AVG(s.amount) AS average_revenue,
        COUNT(DISTINCT s.user_id) AS active_users,
        COUNT(DISTINCT ug.user_id) AS total_users,
        MIN(s.date) AS first_sale_date,
        MAX(s.date) AS last_sale_date
      FROM 
        groups g
        JOIN user_groups ug ON g.id = ug.group_id
        JOIN sales s ON ug.user_id = s.user_id
      WHERE 
        s.date BETWEEN $1 AND $2
      GROUP BY 
        g.id, g.name
      ORDER BY 
        ${validSortBy} ${validSortOrder}
      LIMIT $3
    `;
    
    // Query for trends over time
    const trendsQuery = `
      SELECT 
        DATE_TRUNC($1, s.date) AS time_period,
        COUNT(s.id) AS sales_count,
        SUM(s.amount) AS total_revenue,
        AVG(s.amount) AS average_revenue,
        COUNT(DISTINCT s.user_id) AS active_users,
        COUNT(DISTINCT ug.group_id) AS active_groups
      FROM 
        sales s
        JOIN user_groups ug ON s.user_id = ug.user_id
      WHERE 
        s.date BETWEEN $2 AND $3
      GROUP BY 
        DATE_TRUNC($1, s.date)
      ORDER BY 
        time_period
    `;
    
    // Query for group size analysis
    const groupSizeQuery = `
      WITH group_sizes AS (
        SELECT 
          g.id,
          g.name,
          COUNT(DISTINCT ug.user_id) AS user_count
        FROM 
          groups g
          JOIN user_groups ug ON g.id = ug.group_id
        GROUP BY 
          g.id, g.name
      ),
      group_performance AS (
        SELECT 
          g.id,
          SUM(s.amount) AS total_revenue,
          COUNT(s.id) AS sales_count
        FROM 
          groups g
          JOIN user_groups ug ON g.id = ug.group_id
          JOIN sales s ON ug.user_id = s.user_id
        WHERE 
          s.date BETWEEN $1 AND $2
        GROUP BY 
          g.id
      )
      SELECT 
        gs.user_count,
        COUNT(DISTINCT gs.id) AS group_count,
        AVG(gp.total_revenue) AS avg_revenue,
        AVG(gp.sales_count) AS avg_sales
      FROM 
        group_sizes gs
        LEFT JOIN group_performance gp ON gs.id = gp.id
      GROUP BY 
        gs.user_count
      ORDER BY 
        gs.user_count
    `;
    
    // Execute queries
    const [topGroupsResult, trendsResult, groupSizeResult] = await Promise.all([
      pool.query(topGroupsQuery, [validStartDate, validEndDate, validLimit]),
      pool.query(trendsQuery, [validInterval, validStartDate, validEndDate]),
      pool.query(groupSizeQuery, [validStartDate, validEndDate])
    ]);
    
    res.json({
      data: {
        top_groups: topGroupsResult.rows,
        trends: trendsResult.rows,
        group_size_analysis: groupSizeResult.rows
      },
      meta: {
        filters: {
          startDate: validStartDate,
          endDate: validEndDate,
          interval: validInterval,
          limit: validLimit
        }
      },
      links: {
        self: `/api/metrics/groups/performance?interval=${validInterval}`,
        groups_sales: '/api/metrics/groups/sales'
      }
    });
  } catch (error) {
    console.error('Error fetching all groups performance:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
}

module.exports = {
  getGroupUsers,
  getGroupSales,
  getGroupSalesSummary,
  getGroupPerformance,
  getAllGroupsPerformance
};