require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const mongoose = require('mongoose');
const { connectMongo } = require('../lib/connect-mongo');
const User = require('../models/User');

async function run() {
  try {
    console.log('Connecting to MongoDB...');
    await connectMongo();
    console.log('Connected.');

    const email = 'user@example.com';
    const password = 'Password123';
    const fullName = 'Vision User';

    let user = await User.findOne({ email });
    if (user) {
      console.log(`User already exists: ${email}`);
      process.exit(0);
    }

    const { salt, hash } = User.hashPassword(password);
    
    user = new User({
      fullName,
      email,
      passwordHash: hash,
      salt,
      emailVerified: true,
      role: 'user',
    });

    await user.save();
    console.log('-----------------------------------');
    console.log('User registered successfully!');
    console.log(`Name:     ${fullName}`);
    console.log(`Email:    ${email}`);
    console.log(`Password: ${password}`);
    console.log('-----------------------------------');
    process.exit(0);
  } catch (err) {
    console.error('Registration failed:', err);
    process.exit(1);
  }
}

run();
