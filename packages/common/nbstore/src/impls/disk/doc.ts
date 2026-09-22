import { applyUpdate, Doc as YDoc } from 'yjs';

import {
  type DocClock,
  type DocClocks,
  type DocRecord,
  DocStorageBase,
  type DocUpdate,
} from '../../storage';
import { type SpaceType } from '../../utils/universal-id';
import { DiskSyncConnection, type DiskSyncEvent } from './api';

export interface DiskDocStorageOptions {
  readonly flavour: string;
  readonly type: SpaceType;
  readonly id: string;
  readonly syncFolder: string;
}

export class DiskDocStorage extends DocStorageBase<DiskDocStorageOptions> {
  static readonly identifier = 'DiskDocStorage';

  readonly syncMetadataScope = 'connection';

  readonly connection: DiskSyncConnection;

  private readonly snapshots = new Map<string, DocRecord>();
  private readonly discoveredRootDocs = new Set<string>();
  private readonly discoveredSources = new Map<string, Date>();

  constructor(options: DiskDocStorageOptions) {
    super(options);
    this.connection = new DiskSyncConnection(options, this.handleDiskEvent);
  }

  override async pushDocUpdate(update: DocUpdate, origin?: string) {
    const { timestamp, reviewRequired } =
      await this.connection.applyLocalUpdate(update);
    const next: DocRecord = {
      docId: update.docId,
      bin: update.bin,
      timestamp,
      editor: update.editor,
    };
    await this.applySnapshotUpdate(next, origin);
    if (reviewRequired) {
      const error = new Error(
        `Review Markdown source candidate: ${reviewRequired}`
      );
      error.name = 'DISK_SOURCE_REVIEW_REQUIRED';
      throw error;
    }
    return { docId: update.docId, timestamp };
  }

  async acknowledgeDocUpdate(docId: string, localSnapshot: Uint8Array) {
    await this.connection.acknowledgeSourceUpdate(docId, localSnapshot);
  }

  async prepareDocImport(
    docId: string,
    localSnapshot: Uint8Array | null,
    localRoot: Uint8Array | null
  ) {
    const snapshot = await this.connection.prepareSourceDoc(
      docId,
      localSnapshot ?? undefined,
      localRoot ?? undefined
    );
    if (snapshot) {
      this.discoveredSources.delete(docId);
      await this.applySnapshotUpdate({
        docId,
        bin: snapshot,
        timestamp: new Date(),
      });
    }
  }

  override async getDocTimestamp(docId: string): Promise<DocClock | null> {
    const snapshot = this.snapshots.get(docId);
    if (!snapshot) {
      const discovered = this.discoveredSources.get(docId);
      return discovered ? { docId, timestamp: discovered } : null;
    }
    return {
      docId,
      timestamp: snapshot.timestamp,
    };
  }

  override async getDocTimestamps(after?: Date): Promise<DocClocks> {
    const timestamps: DocClocks = {};
    for (const [docId, timestamp] of this.discoveredSources) {
      if (!after || timestamp.getTime() > after.getTime()) {
        timestamps[docId] = timestamp;
      }
    }
    for (const [docId, snapshot] of this.snapshots.entries()) {
      if (after && snapshot.timestamp.getTime() <= after.getTime()) {
        continue;
      }
      timestamps[docId] = snapshot.timestamp;
    }
    return timestamps;
  }

  override async deleteDoc(docId: string): Promise<void> {
    this.snapshots.delete(docId);
    this.discoveredSources.delete(docId);
  }

  protected override async getDocSnapshot(docId: string) {
    return this.snapshots.get(docId) ?? null;
  }

  protected override async setDocSnapshot(
    snapshot: DocRecord
  ): Promise<boolean> {
    const existing = this.snapshots.get(snapshot.docId);
    if (
      existing &&
      existing.timestamp.getTime() > snapshot.timestamp.getTime()
    ) {
      return false;
    }
    this.snapshots.set(snapshot.docId, snapshot);
    return true;
  }

  protected override async getDocUpdates(_docId: string): Promise<DocRecord[]> {
    return [];
  }

  protected override async markUpdatesMerged(
    _docId: string,
    updates: DocRecord[]
  ): Promise<number> {
    return updates.length;
  }

  private readonly handleDiskEvent = (event: DiskSyncEvent) => {
    switch (event.type) {
      case 'source-discovered': {
        const timestamp = new Date();
        this.discoveredSources.set(event.docId, timestamp);
        this.emit(
          'update',
          { docId: event.docId, bin: new Uint8Array([0, 0]), timestamp },
          'disk:source-discovered'
        );
        return;
      }
      case 'doc-update': {
        const update: DocRecord = {
          docId: event.update.docId,
          bin: event.update.bin,
          timestamp: event.update.timestamp,
          editor: event.update.editor,
        };
        void this.applySnapshotUpdate(update, event.origin).catch(error => {
          console.warn(
            '[disk] failed to apply remote doc-update, skip event',
            error
          );
        });
        return;
      }
      case 'error': {
        console.warn('[disk] session error', event.message);
        return;
      }
      default: {
        return;
      }
    }
  };

  private async applySnapshotUpdate(update: DocRecord, origin?: string) {
    await using _lock = await this.lockDocForUpdate(update.docId);
    await this.mergeIntoSnapshot(update);
    this.emit('update', update, origin);
    if (update.docId === this.spaceId) {
      this.emitRootMetaDiscoveryUpdates();
    }
  }

  private async mergeIntoSnapshot(update: DocRecord) {
    const current = this.snapshots.get(update.docId);
    if (!current) {
      this.snapshots.set(update.docId, update);
      return;
    }

    const merged = await this.mergeUpdates([current.bin, update.bin]);
    this.snapshots.set(update.docId, {
      ...update,
      bin: merged,
      timestamp:
        current.timestamp.getTime() > update.timestamp.getTime()
          ? current.timestamp
          : update.timestamp,
      editor: update.editor ?? current.editor,
    });
  }

  private emitRootMetaDiscoveryUpdates() {
    const rootSnapshot = this.snapshots.get(this.spaceId);
    if (!rootSnapshot) {
      return;
    }

    const docIds = extractRootMetaDocIds(rootSnapshot.bin);
    // These discovery events are only meant to "introduce" doc ids to the sync
    // peer, so it can connect/pull/push them. They should NOT be treated as a
    // remote clock; otherwise switching sync folders (remote empty) can be
    // incorrectly seen as "remote newer than local" and skip the initial push.
    const discoveryTimestamp = new Date(0);
    for (const docId of docIds) {
      if (docId === this.spaceId || this.discoveredRootDocs.has(docId)) {
        continue;
      }
      this.discoveredRootDocs.add(docId);
      this.emit(
        'update',
        {
          docId,
          bin: new Uint8Array(),
          timestamp: discoveryTimestamp,
        },
        'disk:root-meta-discovery'
      );
    }
  }
}

function extractRootMetaDocIds(rootBin: Uint8Array): string[] {
  const doc = new YDoc();
  try {
    applyUpdate(doc, rootBin);
  } catch {
    return [];
  }

  const meta = doc.getMap<unknown>('meta');
  const pages = meta.get('pages');
  const pagesJson =
    typeof pages === 'object' &&
    pages !== null &&
    'toJSON' in pages &&
    typeof pages.toJSON === 'function'
      ? pages.toJSON()
      : pages;

  if (!Array.isArray(pagesJson)) {
    return [];
  }

  const docIds: string[] = [];
  for (const page of pagesJson) {
    if (!page || typeof page !== 'object') {
      continue;
    }
    const id = (page as { id?: unknown }).id;
    if (typeof id === 'string' && id.length > 0) {
      docIds.push(id);
    }
  }
  return docIds;
}
