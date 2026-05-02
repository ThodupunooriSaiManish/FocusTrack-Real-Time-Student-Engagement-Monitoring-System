const mongoose = require('mongoose');
require('dotenv').config();

const initDB = async () => {
  try {
    await mongoose.connect(process.env.MONGO_URI || 'mongodb://localhost:27017/learning_engine');
    console.log('MongoDB connected successfully');
  } catch (err) {
    console.error('Error connecting to MongoDB:', err);
  }
};

const userSchema = new mongoose.Schema({
  username: { type: String, unique: true, required: true },
  password: { type: String, required: true },
  points: { type: Number, default: 0 }
});

const insightSchema = new mongoose.Schema({
  type: String, // 'STUDENT', 'SECTION_ANALYTICS', 'DEPARTMENT_ANALYTICS', 'MENTOR_ALERT'
  user_id: String,
  section_id: String,
  department: String,
  mentor_id: String,
  
  // Metrics
  accuracy: Number,
  focus_index: Number, // Replacing health/avg_accuracy terminology
  focus_score: Number,
  
  // Categorical / Intelligence
  status: String, // HIGH_FOCUS, MEDIUM_FOCUS, LOW_FOCUS
  trend: String, // IMPROVING, DECLINING, STABLE
  is_at_risk: { type: Boolean, default: false },
  reasons: [String],
  
  severity: String, // CRITICAL, WARNING
  message: String,
  
  distribution: Object, // { high, medium, low }
  
  created_at: { type: Date, default: Date.now }
});

const User = mongoose.model('User', userSchema);
const Insight = mongoose.model('Insight', insightSchema);

module.exports = { initDB, User, Insight };
