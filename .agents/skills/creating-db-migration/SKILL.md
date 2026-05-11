---
name: creating-db-migration
description: Use when adding, removing, or modifying database columns or tables in IIB models. Triggers on schema changes, new model fields, Alembic migrations, or "flask db migrate".
---

## Overview

IIB uses Alembic via Flask-Migrate for database schema changes. Migrations are auto-generated from model changes but must be reviewed. The critical rule: **never edit an existing migration file** — always generate a new revision.

## Steps

### 1. Modify the model

Edit `iib/web/models.py` with your schema change (new column, new table, altered constraint). Use SQLAlchemy 2.0 `Mapped` type annotations:

```python
new_field: Mapped[Optional[str]] = db.mapped_column(db.Text, nullable=True)
```

For foreign keys to `image.id`:
```python
new_image_id: Mapped[Optional[int]] = db.mapped_column(db.ForeignKey('image.id'))
new_image: Mapped['Image'] = db.relationship('Image', foreign_keys=[new_image_id])
```

### 2. Generate the migration

```bash
tox -e migrate-db "short description of change"
```

This runs `flask db stamp head && flask db upgrade && flask db migrate -m "..."`. A new file appears in `iib/web/migrations/versions/`.

### 3. Review the generated migration

Open the generated file and verify:
- `upgrade()` creates/alters the correct tables and columns
- `downgrade()` reverses all changes in the correct order
- Foreign key constraints reference the right tables
- Indexes are created where needed (especially on foreign key columns)
- `batch_alter_table` is used for SQLite compatibility in tests

### 4. Test the migration

```bash
tox -e py312 -- tests/test_web/test_migrations.py
```

Also run the full test suite since tests use SQLite and apply all migrations via `flask_migrate.upgrade()` in the `db` fixture.

## Gotchas

- **Never edit existing migration files** — if you need to fix a migration you just generated, delete it and regenerate. If it's already merged, create a new migration that fixes the issue.
- **Tests use SQLite**, so some PostgreSQL-specific features (e.g., array columns, certain constraint types) need SQLite-compatible alternatives.
- **Association tables** (many-to-many) need composite primary keys and `UniqueConstraint`. See `RequestAddBundleDeprecation` in `models.py` for the pattern.
- **JSON fields** are stored as `db.Text` with manual `json.dumps`/`json.loads` (see `RequestCreateEmptyIndex.labels` pattern), not as native JSON columns.
- After generating, check that the `down_revision` points to the correct previous migration head.
