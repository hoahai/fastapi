-- Optional migration for provider-based attachment metadata.
-- Apply to the table mapped by TRADSPHERE_DB_TABLES.INVNOTEATTACHMENTS
-- (default: TradSphere_InvNoteAttachment).

ALTER TABLE TradSphere_InvNoteAttachment
  ADD COLUMN IF NOT EXISTS storageProvider VARCHAR(64) NULL,
  ADD COLUMN IF NOT EXISTS providerAssetId VARCHAR(255) NULL,
  ADD COLUMN IF NOT EXISTS providerPublicId VARCHAR(512) NULL,
  ADD COLUMN IF NOT EXISTS providerResourceType VARCHAR(64) NULL,
  ADD COLUMN IF NOT EXISTS accessUrl VARCHAR(2048) NULL,
  ADD COLUMN IF NOT EXISTS originalFileName VARCHAR(255) NULL,
  ADD COLUMN IF NOT EXISTS mimeType VARCHAR(255) NULL,
  ADD COLUMN IF NOT EXISTS fileSize BIGINT NULL,
  ADD COLUMN IF NOT EXISTS uploadedBy VARCHAR(255) NULL,
  ADD COLUMN IF NOT EXISTS tenantSlug VARCHAR(128) NULL,
  ADD COLUMN IF NOT EXISTS ownerEntityType VARCHAR(128) NULL,
  ADD COLUMN IF NOT EXISTS ownerEntityId VARCHAR(128) NULL,
  ADD COLUMN IF NOT EXISTS deletedAt DATETIME NULL;

CREATE INDEX idx_inv_note_attachment_note_deleted
  ON TradSphere_InvNoteAttachment (noteId, deletedAt);

CREATE INDEX idx_inv_note_attachment_provider
  ON TradSphere_InvNoteAttachment (storageProvider, providerPublicId);
