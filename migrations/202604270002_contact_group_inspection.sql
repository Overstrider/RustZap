ALTER TABLE contacts ADD COLUMN IF NOT EXISTS lid text;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS avatar_url text;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS profile_picture_url text;

ALTER TABLE groups ADD COLUMN IF NOT EXISTS subject_owner_jid text;
ALTER TABLE groups ADD COLUMN IF NOT EXISTS profile_picture_media_id text;
ALTER TABLE groups ADD COLUMN IF NOT EXISTS avatar_url text;
ALTER TABLE groups ADD COLUMN IF NOT EXISTS profile_picture_url text;
ALTER TABLE groups ADD COLUMN IF NOT EXISTS members_count integer;
ALTER TABLE groups ADD COLUMN IF NOT EXISTS admins_count integer;

CREATE INDEX IF NOT EXISTS idx_contacts_lid ON contacts(project_id, company_id, lid);
CREATE INDEX IF NOT EXISTS idx_group_members_contact ON group_members(contact_id);
