const crypto = require('crypto');

function base64url(str) {
  return Buffer.from(str)
    .toString('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}

function base64urlDecode(str) {
  let s = str.replace(/-/g, '+').replace(/_/g, '/');
  while (s.length % 4) {
    s += '=';
  }
  return Buffer.from(s, 'base64').toString('utf8');
}

function signJWT(payload, secret, expiresInSeconds = 86400) {
  const header = { alg: 'HS256', typ: 'JWT' };
  const exp = Math.floor(Date.now() / 1000) + expiresInSeconds;
  const fullPayload = { ...payload, exp };
  
  const encodedHeader = base64url(JSON.stringify(header));
  const encodedPayload = base64url(JSON.stringify(fullPayload));
  
  const tokenInput = `${encodedHeader}.${encodedPayload}`;
  const signature = crypto
    .createHmac('sha256', secret)
    .update(tokenInput)
    .digest('base64url');
    
  return `${tokenInput}.${signature}`;
}

function verifyJWT(token, secret) {
  const parts = token.split('.');
  if (parts.length !== 3) return null;
  
  const [header, payload, signature] = parts;
  const tokenInput = `${header}.${payload}`;
  const expectedSignature = crypto
    .createHmac('sha256', secret)
    .update(tokenInput)
    .digest('base64url');
    
  if (signature !== expectedSignature) return null;
  
  try {
    const decodedPayload = JSON.parse(base64urlDecode(payload));
    if (decodedPayload.exp && decodedPayload.exp < Date.now() / 1000) {
      return null; // Expired
    }
    return decodedPayload;
  } catch (err) {
    return null;
  }
}

module.exports = {
  signJWT,
  verifyJWT,
};
