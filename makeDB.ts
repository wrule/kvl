import fs from 'fs';
import base64Text from './1.db';

export default
function makeDB(file: fs.PathOrFileDescriptor) {
  fs.writeFileSync(file, Buffer.from(base64Text, 'base64'));
}

if (require.main === module) makeDB('0.db');
