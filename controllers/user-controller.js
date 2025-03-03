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
 * Get sales for a specific user
 * 
 * @param {Object} req - Request object
 * @param {Object} req.params - URL parameters
 * @param {string} req.params.id - User ID
 * @param {Object} req.query - Query parameters
 * @param {string} [req.query.startDate] - Start date filter (YYYY-MM-DD)
 * @param {string} [req.query.endDate] - End date filter (YYYY-MM-DD)
 * @param {number} [req.query.page] - Page number for pagination
 * @param {number} [req.query.limit] - Number of items per page
 * @param {string} [req.query.sortBy] - Field to sort by (date, amount)
 * @param {string} [req.query.sortOrder] - Sort direction (asc, desc)
 * @param {Object} res - Response object
 * @param {Object} pool - Database connection pool
 */
async function getUserSales(req, res, pool) {
  try {
    const userId = req.params.id;
    if (!userId) {
      return res.status(400).json({ error: 'User ID is required' });
    }
    
    const { startDate, endDate, page, limit, sortBy, sortOrder } = req.query;
    
    // Validate user exists
    await validateResourceExists(pool, 'users', userId);
    
    // Validate dates
    const validStartDate = parseDate(startDate, null);
    const validEndDate = parseDate(endDate, null);
    
    // Validate sort parameters
    const validSortColumns = ['date', 'amount'];
    const validSortBy = validateSortColumn(sortBy, validSortColumns, 'date');
    const validSortOrder = validateSortOrder(sortOrder, 'desc');
    
    // Build query conditions
    const conditions = ["user_id = $1"];
    const queryParams = [userId];
    let paramIndex = 2;
    
    if (validStartDate) {
      conditions.push(`date >= $${paramIndex}`);
      queryParams.push(validStartDate);
      paramIndex++;
    }
    
    if (validEndDate) {
      conditions.push(`date <= $${paramIndex}`);
      queryParams.push(validEndDate);
      paramIndex++;
    }
    
    // Get pagination parameters
    const pagination = getPaginationParams(page, limit);
    
    // Query for user sales
    const query = `
      SELECT 
        id,
        amount,
        date
      FROM 
        sales
      WHERE ${conditions.join(' AND ')}
      ORDER BY ${validSortBy} ${validSortOrder}
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;
    
    queryParams.push(pagination.limit, pagination.offset);
    
    const result = await pool.query(query, queryParams);
    
    // Get total count for pagination
    const countQuery = `
      SELECT COUNT(*) FROM sales WHERE ${conditions.join(' AND ')}
    `;
    
    const countResult = await pool.query(countQuery, queryParams.slice(0, paramIndex - 1));
    const total = Number.parseInt(countResult.rows[0].count, 10);
    
    // Format response
    res.json(formatPaginatedResponse(
      result.rows,
      pagination,
      total,
      `/api/metrics/users/${userId}/sales`,
      {
        filters: {
          startDate: validStartDate,
          endDate: validEndDate
        }
      }
    ));
  } catch (error) {
    console.error('Error fetching user sales:', error);
    if (error.statusCode) {
      res.status(error.statusCode).json({ error: error.message });
    } else {
      res.status(500).json({ error: 'Internal server error' });
    }
  }
}

/**
 * Get sales summary for a specific user
 * 
 * @param {Object} req - Request object
 * @param {Object} req.params - URL parameters
 * @param {string} req.params.id - User ID
 * @param {Object} req.query - Query parameters
 * @param {string} [req.query.startDate] - Start date filter (YYYY-MM-DD)
 * @param {string} [req.query.endDate] - End date filter (YYYY-MM-DD)
 * @param {Object} res - Response object
 * @param {Object} pool - Database connection pool
 */
async function getUserSalesSummary(req, res, pool) {
  try {
    const userId = req.params.id;
    if (!userId) {
      return res.status(400).json({ error: 'User ID is required' });
    }
    
    const { startDate, endDate } = req.query;
    
    // Validate user exists
    await validateResourceExists(pool, 'users', userId);
    
    // Validate dates
    const validStartDate = parseDate(startDate, null);
    const validEndDate = parseDate(endDate, null);
    
    // Build query conditions
    const conditions = ["user_id = $1"];
    const queryParams = [userId];
    let paramIndex = 2;
    
    if (validStartDate) {
      conditions.push(`date >= $${paramIndex}`);
      queryParams.push(validStartDate);
      paramIndex++;
    }
    
    if (validEndDate) {
      conditions.push(`date <= $${paramIndex}`);
      queryParams.push(validEndDate);
      paramIndex++;
    }
    
    // Query for overall summary
    const summaryQuery = `
      SELECT 
        COUNT(*) AS total_sales,
        SUM(amount) AS total_revenue,
        AVG(amount) AS average_sale_amount,
        MIN(amount) AS min_sale_amount,
        MAX(amount) AS max_sale_amount
      FROM 
        sales
      WHERE ${conditions.join(' AND ')}
    `;
    
    const summaryResult = await pool.query(summaryQuery, queryParams);
    
    // Query for monthly breakdown
    const monthlyQuery = `
      SELECT 
        DATE_TRUNC('month', date) AS month,
        COUNT(*) AS sales_count,
        SUM(amount) AS total_revenue,
        AVG(amount) AS average_revenue
      FROM 
        sales
      WHERE ${conditions.join(' AND ')}
      GROUP BY 
        DATE_TRUNC('month', date)
      ORDER BY 
        month
    `;
    
    const monthlyResult = await pool.query(monthlyQuery, queryParams);
    
    res.json({
      data: {
        summary: summaryResult.rows[0],
        monthly_breakdown: monthlyResult.rows
      },
      meta: {
        filters: {
          startDate: validStartDate,
          endDate: validEndDate
        }
      },
      links: {
        self: `/api/metrics/users/${userId}/sales/summary`,
        sales: `/api/metrics/users/${userId}/sales`
      }
    });
  } catch (error) {
    console.error('Error fetching user sales summary:', error);
    if (error.statusCode) {
      res.status(error.statusCode).json({ error: error.message });
    } else {
      res.status(500).json({ error: 'Internal server error' });
    }
  }
}

/**
 * Get performance metrics for a specific user
 * 
 * @param {Object} req - Request object
 * @param {Object} req.params - URL parameters
 * @param {string} req.params.id - User ID
 * @param {Object} req.query - Query parameters
 * @param {string} [req.query.startDate] - Start date filter (YYYY-MM-DD)
 * @param {string} [req.query.endDate] - End date filter (YYYY-MM-DD)
 * @param {string} [req.query.interval] - Time interval for trends (day, week, month, quarter, year)
 * @param {Object} res - Response object
 * @param {Object} pool - Database connection pool
 */
async function getUserPerformance(req, res, pool) {
  try {
    const userId = req.params.id;
    if (!userId) {
      return res.status(400).json({ error: 'User ID is required' });
    }
    
    const { startDate, endDate, interval } = req.query;
    
    // Validate user exists
    await validateResourceExists(pool, 'users', userId);
    
    // Validate dates
    const validStartDate = parseDate(startDate, '2021-01-01');
    const validEndDate = parseDate(endDate, '2021-12-31');
    
    // Validate interval
    const validInterval = validateInterval(interval, 'month');
    
    // Query for performance over time
    const trendsQuery = `
      SELECT 
        DATE_TRUNC($1, date) AS time_period,
        COUNT(*) AS sales_count,
        SUM(amount) AS total_revenue,
        AVG(amount) AS average_revenue
      FROM 
        sales
      WHERE 
        user_id = $2 AND
        date BETWEEN $3 AND $4
      GROUP BY 
        DATE_TRUNC($1, date)
      ORDER BY 
        time_period
    `;
    
    const trendsResult = await pool.query(trendsQuery, [
      validInterval, 
      userId, 
      validStartDate, 
      validEndDate
    ]);
    
    // Query for performance summary
    const summaryQuery = `
      SELECT 
        COUNT(*) AS total_sales,
        SUM(amount) AS total_revenue,
        AVG(amount) AS average_sale_amount,
        MIN(amount) AS min_sale_amount,
        MAX(amount) AS max_sale_amount
      FROM 
        sales
      WHERE 
        user_id = $1 AND
        date BETWEEN $2 AND $3
    `;
    
    const summaryResult = await pool.query(summaryQuery, [
      userId, 
      validStartDate, 
      validEndDate
    ]);
    
    // Query for user ranking within their groups
    const rankingQuery = `
      WITH user_sales AS (
        SELECT 
          g.id AS group_id,
          g.name AS group_name,
          SUM(s.amount) AS user_revenue
        FROM 
          sales s
          JOIN user_groups ug ON s.user_id = ug.user_id
          JOIN groups g ON ug.group_id = g.id
        WHERE 
          s.user_id = $1 AND
          s.date BETWEEN $2 AND $3
        GROUP BY 
          g.id, g.name
      ),
      group_rankings AS (
        SELECT 
          g.id AS group_id,
          g.name AS group_name,
          u.id AS user_id,
          u.name AS user_name,
          SUM(s.amount) AS revenue,
          RANK() OVER (PARTITION BY g.id ORDER BY SUM(s.amount) DESC) AS rank
        FROM 
          sales s
          JOIN user_groups ug ON s.user_id = ug.user_id
          JOIN groups g ON ug.group_id = g.id
          JOIN users u ON s.user_id = u.id
        WHERE 
          s.date BETWEEN $2 AND $3
        GROUP BY 
          g.id, g.name, u.id, u.name
      )
      SELECT 
        us.group_id,
        us.group_name,
        us.user_revenue,
        gr.rank,
        COUNT(*) OVER (PARTITION BY us.group_id) AS total_users
      FROM 
        user_sales us
        JOIN group_rankings gr ON us.group_id = gr.group_id AND gr.user_id = $1
    `;
    
    const rankingResult = await pool.query(rankingQuery, [
      userId, 
      validStartDate, 
      validEndDate
    ]);
    
    res.json({
      data: {
        summary: summaryResult.rows[0],
        trends: trendsResult.rows,
        group_rankings: rankingResult.rows
      },
      meta: {
        filters: {
          startDate: validStartDate,
          endDate: validEndDate,
          interval: validInterval
        }
      },
      links: {
        self: `/api/metrics/users/${userId}/performance?interval=${validInterval}`,
        user: `/api/metrics/users/${userId}`,
        sales: `/api/metrics/users/${userId}/sales`
      }
    });
  } catch (error) {
    console.error('Error fetching user performance:', error);
    if (error.statusCode) {
      res.status(error.statusCode).json({ error: error.message });
    } else {
      res.status(500).json({ error: 'Internal server error' });
    }
  }
}

/**
 * Get performance metrics across all users
 * 
 * @param {Object} req - Request object
 * @param {Object} req.query - Query parameters
 * @param {string} [req.query.startDate] - Start date filter (YYYY-MM-DD)
 * @param {string} [req.query.endDate] - End date filter (YYYY-MM-DD)
 * @param {string} [req.query.interval] - Time interval for trends (day, week, month, quarter, year)
 * @param {string} [req.query.role] - Filter by user role
 * @param {string} [req.query.groupId] - Filter by group ID
 * @param {number} [req.query.limit] - Limit for top performers
 * @param {string} [req.query.sortBy] - Field to sort by (total_revenue, average_revenue, sales_count, name)
 * @param {string} [req.query.sortOrder] - Sort direction (asc, desc)
 * @param {Object} res - Response object
 * @param {Object} pool - Database connection pool
 */
async function getAllUsersPerformance(req, res, pool) {
  try {
    const { 
      startDate, 
      endDate, 
      interval, 
      role,
      groupId,
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
    const validSortColumns = ['total_revenue', 'average_revenue', 'sales_count', 'name'];
    const validSortBy = validateSortColumn(sortBy, validSortColumns, 'total_revenue');
    const validSortOrder = validateSortOrder(sortOrder, 'desc');
    
    // Validate limit
    const validLimit = limit ? Number.parseInt(limit, 10) : 10;
    
    // Build query conditions
    const conditions = [];
    const queryParams = [validStartDate, validEndDate];
    let paramIndex = 3;
    
    if (role) {
      conditions.push(`u.role = $${paramIndex}`);
      queryParams.push(role);
      paramIndex++;
    }
    
    // Build the base query for top performers
    let topPerformersQuery = `
      SELECT 
        u.id,
        u.name,
        u.role,
        COUNT(s.id) AS sales_count,
        SUM(s.amount) AS total_revenue,
        AVG(s.amount) AS average_revenue,
        MIN(s.date) AS first_sale_date,
        MAX(s.date) AS last_sale_date
      FROM 
        users u
        JOIN sales s ON u.id = s.user_id
    `;
    
    // Add group filter if provided
    if (groupId) {
      topPerformersQuery += " JOIN user_groups ug ON u.id = ug.user_id";
      conditions.push(`ug.group_id = $${paramIndex}`);
      queryParams.push(groupId);
      paramIndex++;
    }
    
    // Add date conditions
    conditions.push("s.date BETWEEN $1 AND $2");
    
    // Add WHERE clause if conditions exist
    if (conditions.length > 0) {
      topPerformersQuery += ` WHERE ${conditions.join(' AND ')}`;
    }
    
    // Add GROUP BY, ORDER BY, and LIMIT
    topPerformersQuery += `
      GROUP BY 
        u.id, u.name, u.role
      ORDER BY 
        ${validSortBy} ${validSortOrder}
      LIMIT $${paramIndex}
    `;
    
    queryParams.push(validLimit);
    
    // Query for trends over time
    const trendsQuery = `
      SELECT 
        DATE_TRUNC($1, s.date) AS time_period,
        COUNT(s.id) AS sales_count,
        SUM(s.amount) AS total_revenue,
        AVG(s.amount) AS average_revenue,
        COUNT(DISTINCT s.user_id) AS active_users
      FROM 
        sales s
        JOIN users u ON s.user_id = u.id
        ${groupId ? "JOIN user_groups ug ON s.user_id = ug.user_id" : ""}
      WHERE 
        s.date BETWEEN $2 AND $3
        ${role ? " AND u.role = $4" : ""}
        ${groupId ? " AND ug.group_id = $${role ? 5 : 4}" : ""}
      GROUP BY 
        DATE_TRUNC($1, s.date)
      ORDER BY 
        time_period
    `;
    
    const trendsParams = [validInterval, validStartDate, validEndDate];
    if (role) trendsParams.push(role);
    if (groupId) trendsParams.push(groupId);
    
    // Query for role breakdown
    const roleBreakdownQuery = `
      SELECT 
        u.role,
        COUNT(DISTINCT u.id) AS user_count,
        COUNT(s.id) AS sales_count,
        SUM(s.amount) AS total_revenue,
        AVG(s.amount) AS average_revenue
      FROM 
        users u
        JOIN sales s ON u.id = s.user_id
        ${groupId ? "JOIN user_groups ug ON s.user_id = ug.user_id" : ""}
      WHERE 
        s.date BETWEEN $1 AND $2
        ${groupId ? " AND ug.group_id = $3" : ""}
      GROUP BY 
        u.role
      ORDER BY 
        total_revenue DESC
    `;
    
    const roleParams = [validStartDate, validEndDate];
    if (groupId) roleParams.push(groupId);
    
    // Execute queries
    const [topPerformersResult, trendsResult, roleBreakdownResult] = await Promise.all([
      pool.query(topPerformersQuery, queryParams),
      pool.query(trendsQuery, trendsParams),
      pool.query(roleBreakdownQuery, roleParams)
    ]);
    
    res.json({
      data: {
        top_performers: topPerformersResult.rows,
        trends: trendsResult.rows,
        role_breakdown: roleBreakdownResult.rows
      },
      meta: {
        filters: {
          startDate: validStartDate,
          endDate: validEndDate,
          interval: validInterval,
          role,
          groupId,
          limit: validLimit
        }
      },
      links: {
        self: `/api/metrics/users/performance?interval=${validInterval}`,
        users_sales: `/api/metrics/sales?${groupId ? `groupId=${groupId}&` : ''}${role ? `role=${role}&` : ''}`
      }
    });
  } catch (error) {
    console.error('Error fetching all users performance:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
}

module.exports = {
  getUserSales,
  getUserSalesSummary,
  getUserPerformance,
  getAllUsersPerformance
}; 