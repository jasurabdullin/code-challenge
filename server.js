'use strict';

const express = require('express');
const seeder = require('./seed');
const { initializeDatabase } = require('./lib/db');
const userController = require('./controllers/user-controller');
const groupController = require('./controllers/group-controller');
const salesController = require('./controllers/sales-controller');

const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';

async function start() {
  let pool;
  try {
    pool = await initializeDatabase();
    console.log('Connected to database successfully');
  } catch (err) {
    console.error('Failed to connect to database:', err);
    process.exit(1);
  }
  
  try {
    await seeder.seedDatabase();
    console.log('Database seeded successfully');
  } catch (err) {
    console.error('Failed to seed database:', err);
  }

  const app = express();
  app.use(express.json());

  // Health check
  app.get('/health', (req, res) => res.send('Hello World'));

  // User routes
  app.get('/api/metrics/users/performance', (req, res) => userController.getAllUsersPerformance(req, res, pool));
  app.get('/api/metrics/users/:id/sales', (req, res) => userController.getUserSales(req, res, pool));
  app.get('/api/metrics/users/:id/sales/summary', (req, res) => userController.getUserSalesSummary(req, res, pool));
  app.get('/api/metrics/users/:id/performance', (req, res) => userController.getUserPerformance(req, res, pool));

  // Group routes
  app.get('/api/metrics/groups/performance', (req, res) => groupController.getAllGroupsPerformance(req, res, pool));
  app.get('/api/metrics/groups/:id/sales', (req, res) => groupController.getGroupSales(req, res, pool));
  app.get('/api/metrics/groups/:id/sales/summary', (req, res) => groupController.getGroupSalesSummary(req, res, pool));
  app.get('/api/metrics/groups/:id/performance', (req, res) => groupController.getGroupPerformance(req, res, pool));

  // Sales routes
  app.get('/api/metrics/sales', (req, res) => salesController.listSales(req, res, pool));
  app.get('/api/metrics/sales/:id', (req, res) => salesController.getSale(req, res, pool));

  app.listen(PORT, HOST);
  console.log(`Server is running on http://${HOST}:${PORT}`);
}

start().catch(err => {
  console.error('Failed to start server:', err);
  process.exit(1);
});
