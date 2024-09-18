import fs from 'fs';
import crypto from 'crypto';
import sqlite3, { Database } from 'better-sqlite3';
import makeDB from './makeDB';

export default
class TTKV {
  public constructor(
    private file: string,
    private expireTimeMs?: number,
  ) {
    if (!fs.existsSync(file)) makeDB(file);
    this.db = sqlite3(file, { fileMustExist: true });
    this.db.pragma('journal_mode = WAL');
    this.expire();
    setInterval(() => {
      this.expire();
    }, 1000).unref();
  }

  private db: Database;

  public set(key: string, value: string) {
    const insertStmt = this.db.prepare(`INSERT OR IGNORE INTO ttkv (createTime, updateTime, key, value) VALUES (?, ?, ?, ?)`);
    const updateStmt = this.db.prepare(`UPDATE ttkv SET value = ?, updateTime = ? WHERE key = ?`);
    const time = Date.now();
    insertStmt.run(time, time, key, value);
    updateStmt.run(value, time, key);
  }

  public get(key: string) {
    const selectStmt = this.db.prepare(`SELECT value FROM ttkv WHERE key = ?`);
    const item = selectStmt.get(key) as { value: string } | undefined;
    return item?.value;
  }

  public push(name: string, value: string) {
    const key = `${name}:${crypto.randomUUID()}`;
    this.set(key, value);
    return key;
  }

  public pop(name: string) {
    const selectStmt = this.db.prepare(`SELECT value, id FROM ttkv WHERE key LIKE ? || ':%' ORDER BY id DESC LIMIT 1`);
    const deleteStmt = this.db.prepare(`DELETE FROM ttkv WHERE id = ?`);
    const item = selectStmt.get(name) as { value: string, id: number } | undefined;
    if (item?.id) deleteStmt.run(item.id);
    return item?.value;
  }

  public shift(name: string) {
    const selectStmt = this.db.prepare(`SELECT value, id FROM ttkv WHERE key LIKE ? || ':%' ORDER BY id ASC LIMIT 1`);
    const deleteStmt = this.db.prepare(`DELETE FROM ttkv WHERE id = ?`);
    const item = selectStmt.get(name) as { value: string, id: number } | undefined;
    if (item?.id) deleteStmt.run(item.id);
    return item?.value;
  }

  public all(name: string) {
    const selectStmt = this.db.prepare(`SELECT createTime, updateTime, key, value FROM ttkv WHERE key LIKE ? || ':%' ORDER BY createTime DESC`);
    return selectStmt.all(name) as { createTime: number, updateTime: number, key: string, value: string }[];
  }

  public expire() {
    if (!this.expireTimeMs) return;
    const deleteStmt = this.db.prepare(`DELETE FROM ttkv WHERE updateTime <= ?`);
    const result = deleteStmt.run(Date.now() - this.expireTimeMs);
    if (result.changes > 0) this.db.exec('VACUUM');
  }

  public wal_clean() {
    fs.stat(`${this.file}-wal`, (err, stats) => {
      if (err) return;
      if (stats.size > 1024 * 1024 * 32)
        this.db.pragma('wal_checkpoint(RESTART)');
    });
  }
}

async function main() {
  const db = new TTKV('4.db', 60000);
  let count = 1;
  setInterval(() => {
    // console.log(count);
    db.push('test', (count++).toString());
  }, 0);
}

main();
