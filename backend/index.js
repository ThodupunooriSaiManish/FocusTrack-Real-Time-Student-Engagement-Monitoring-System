const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
require('dotenv').config();

const { initDB, User, Insight } = require('./db');
const { initKafka, sendActivityEvent, consumer } = require('./kafka');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
  }
});

const JWT_SECRET = process.env.JWT_SECRET || 'supersecretkey';

// Initialize Services
initDB();
initKafka().then(async () => {
  await consumer.subscribe({ topic: 'focus-output', fromBeginning: false });
  
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const data = JSON.parse(message.value.toString());
        console.log('Received from Flink:', data);
        
        // Simple save for all incoming analytics
        const insight = new Insight(data);
        await insight.save();
        
        // Emit globally
        io.emit('global_analytics_update', data);
      } catch (err) {
        console.error('Error processing message', err);
      }
    },
  });
}).catch(err => console.error('Failed to initialize Kafka at startup:', err));

// WebSocket Connection
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);
  socket.on('disconnect', () => console.log('Client disconnected:', socket.id));
});

// Production APIs
app.get('/api/analytics/global', async (req, res) => {
  try {
    const totalStudents = await Insight.distinct('user_id', { type: 'STUDENT' });
    const atRiskCount = await Insight.countDocuments({ is_at_risk: true, created_at: { $gte: new Date(Date.now() - 60000) } });
    const avgFocus = await Insight.aggregate([
      { $match: { type: 'DEPARTMENT_ANALYTICS' } },
      { $group: { _id: null, avg: { $avg: "$focus_index" } } }
    ]);
    res.json({
      total_active: totalStudents.length,
      global_focus_index: avgFocus[0]?.avg || 0,
      at_risk_count: atRiskCount
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/analytics/department/:id', async (req, res) => {
  try {
    const insights = await Insight.find({ department: req.params.id, type: 'SECTION_ANALYTICS' })
      .sort({ created_at: -1 }).limit(10);
    res.json(insights);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/analytics/section/:id', async (req, res) => {
  try {
    // Get latest score for each unique student in this section
    const insights = await Insight.aggregate([
      { $match: { 
          type: 'STUDENT', 
          user_id: { $regex: new RegExp('^' + req.params.id.replace(/-/g, '_')) } 
      } },
      { $sort: { created_at: -1 } },
      { $group: {
          _id: "$user_id",
          user_id: { $first: "$user_id" },
          focus_score: { $first: "$focus_score" },
          status: { $first: "$status" },
          accuracy: { $first: "$accuracy" },
          is_at_risk: { $first: "$is_at_risk" },
          trend: { $first: "$trend" }
      } },
      { $sort: { focus_score: -1 } }
    ]);
    res.json(insights);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/mentor/:id/alerts', async (req, res) => {
  try {
    const alerts = await Insight.find({ mentor_id: req.params.id, type: 'MENTOR_ALERT' })
      .sort({ created_at: -1 }).limit(20);
    res.json(alerts);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Middleware to verify JWT
const verifyToken = (req, res, next) => {
  const token = req.headers['authorization'];
  if (!token) return res.status(403).json({ error: 'No token provided' });
  
  jwt.verify(token.split(' ')[1], JWT_SECRET, (err, decoded) => {
    if (err) return res.status(401).json({ error: 'Unauthorized' });
    req.userId = decoded.id;
    next();
  });
};

// API Routes
app.post('/api/register', async (req, res) => {
  try {
    const { username, password } = req.body;
    const hashedPassword = await bcrypt.hash(password, 10);
    const user = new User({ username, password: hashedPassword });
    await user.save();
    res.status(201).json({ id: user._id, message: 'User registered' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/login', async (req, res) => {
  try {
    const { username, password } = req.body;
    const user = await User.findOne({ username });
    if (!user) return res.status(404).json({ error: 'User not found' });
    
    const valid = await bcrypt.compare(password, user.password);
    if (!valid) return res.status(401).json({ error: 'Invalid password' });
    
    const token = jwt.sign({ id: user._id }, JWT_SECRET, { expiresIn: '24h' });
    res.json({ token, user: { id: user._id, username: user.username, points: user.points } });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/activity', verifyToken, async (req, res) => {
  try {
    const event = { ...req.body, user_id: req.userId.toString(), timestamp: new Date().toISOString() };
    await sendActivityEvent(event);
    res.json({ message: 'Activity logged' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/dashboard', verifyToken, async (req, res) => {
  try {
    const insight = await Insight.findOne({ user_id: req.userId }).sort({ created_at: -1 });
    res.json(insight || {});
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/recommendations', verifyToken, async (req, res) => {
  try {
    const latest = await Insight.findOne({ user_id: req.userId }).sort({ created_at: -1 });
    
    if (!latest) {
      return res.json({ recommendation: 'Keep learning to unlock personalized recommendations!' });
    }
    let recommendation = '';
    
    if (latest.accuracy < 50) {
      recommendation = `We noticed you're struggling with ${latest.weak_topic || 'recent topics'}. Consider reviewing beginner materials.`;
    } else if (latest.engagement_score < 30) {
      recommendation = 'Your engagement is low. Try a short, interactive quiz to get back on track!';
    } else {
      recommendation = 'Great job! You are performing well. Try advanced challenges to earn more points!';
    }
    
    res.json({ recommendation, latest });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Backend server running on port ${PORT}`);
});
