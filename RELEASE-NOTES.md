# gocosmos - Release notes

## 2024-01-29 - v1.1.0

### Added/Refactoring

- Refactored DELETE statement, appending PK values at the end of parameter list is no longer needed.
- Refactored UPDATE statement, appending PK values at the end of parameter list is no longer needed.
- Feature: INSERT/UPSERT statement accepts WITH PK clause. Appending PK values at the end of parameter list is no longer needed.

### Deprecated

- Deprecated: WITH singlePK/SINGLE_PK is now deprecated for INSERT/UPSERT, DELETE and UPDATE statements.

## 2023-12-20 - v1.0.2

### Security

- Security: fix CodeQL code scanning alerts

## 2023-12-19 - v1.0.1

### Fixed/Improvement

- Fix: server may return status 304 without body

## 2023-12-19 - v1.0.0

### Changed

- BREAKING: migrated from btnguyen2k/gocosmos, package name changed.

### Added/Refactoring

- Added feature: better error report when calling REST APIs

### Fixed/Improvement

- Fixed: no content is returned when the DELETE operation is successful
- Fix golint

