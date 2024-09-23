import fs from 'fs';
import crypto from 'crypto';
import sqlite3, { Database } from 'better-sqlite3';
import makeDB from './makeDB';

export default
class KVL {
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
    const insertStmt = this.db.prepare(`INSERT OR IGNORE INTO kvl (createTime, updateTime, key, value) VALUES (?, ?, ?, ?)`);
    const updateStmt = this.db.prepare(`UPDATE kvl SET value = ?, updateTime = ? WHERE key = ?`);
    const time = Date.now();
    insertStmt.run(time, time, key, value);
    updateStmt.run(value, time, key);
    return this;
  }

  public get(key: string) {
    const selectStmt = this.db.prepare(`SELECT value FROM kvl WHERE key = ?`);
    const item = selectStmt.get(key) as { value: string } | undefined;
    return item?.value;
  }

  public push(name: string, value: string) {
    const key = `${name}:${crypto.randomUUID()}`;
    this.set(key, value);
    return key;
  }

  public pop(name: string) {
    const selectStmt = this.db.prepare(`SELECT value, id FROM kvl WHERE key LIKE ? || ':%' ORDER BY id DESC LIMIT 1`);
    const deleteStmt = this.db.prepare(`DELETE FROM kvl WHERE id = ?`);
    const item = selectStmt.get(name) as { value: string, id: number } | undefined;
    if (item?.id) deleteStmt.run(item.id);
    return item?.value;
  }

  public shift(name: string) {
    const selectStmt = this.db.prepare(`SELECT value, id FROM kvl WHERE key LIKE ? || ':%' ORDER BY id ASC LIMIT 1`);
    const deleteStmt = this.db.prepare(`DELETE FROM kvl WHERE id = ?`);
    const item = selectStmt.get(name) as { value: string, id: number } | undefined;
    if (item?.id) deleteStmt.run(item.id);
    return item?.value;
  }

  public all(name: string) {
    const selectStmt = this.db.prepare(`SELECT createTime, updateTime, key, value FROM kvl WHERE key LIKE ? || ':%' ORDER BY createTime DESC`);
    return selectStmt.all(name) as { createTime: number, updateTime: number, key: string, value: string }[];
  }

  public tags(key: string): string;
  public tags(key: string, tags: string): void;
  public tags(key: string, tags?: string) {
    if (tags == null) {
      const selectStmt = this.db.prepare(`SELECT labels FROM kvl WHERE key = ?`);
      const item = selectStmt.get(key) as { labels: string } | undefined;
      if (!item) throw Error('item does not exist');
      return item.labels;
    } else {
      const updateStmt = this.db.prepare(`UPDATE kvl SET labels = ?, updateTime = ? WHERE key = ?`);
      const time = Date.now();
      const result = updateStmt.run(tags, time, key);
      if (result.changes < 1) throw Error('item does not exist');
    }
  }

  public page(
    name: string,
    pageNum = 1,
    pageSize = 10,
    tags?: string[],
    tagsOperator?: 'AND' | 'OR',
    orderBy?: 'createTime' | 'updateTime',
    orderDir?: 'ASC' | 'DESC',
  ) {
    const selectStmt = (param: '*' | 'COUNT(1) as total') => {
      return `SELECT ${param} FROM kvl WHERE key LIKE ? || ':%' AND (${(() => {
        if (!tags?.length) return '1 = 1';
        return Array(tags.length).fill(`labels LIKE ('%' || ? || '%')`).join(` ${tagsOperator ?? 'AND'} `);
      })()})`;
    };
    const totalStmt = this.db.prepare(selectStmt('COUNT(1) as total'));
    const { total } = totalStmt.get(name) as { total: number };
    const lastOffset = total > 0 ? total - 1 : 0;
    const pages = Math.floor(lastOffset / pageSize) + 1;
    if (pageNum > pages) pageNum = pages;
  }

  public expire() {
    if (!this.expireTimeMs) return;
    const deleteStmt = this.db.prepare(`DELETE FROM kvl WHERE updateTime <= ?`);
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
  const db = new KVL('2.db');
  db.page('test');
}

main();
