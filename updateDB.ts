import fs from 'fs';

fs.writeFileSync('1.db.ts', `export default "${
  fs.readFileSync('0.db').toString('base64')
}";`, 'utf8');
