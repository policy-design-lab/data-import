# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- This CHANGELOG file.
- Statute-level percentages to CSP state distribution data
  JSON. [#12](https://github.com/policy-design-lab/data-import/issues/12)
- Add base acres and recipient numbers to Title 1
  Commodities. [#14](https://github.com/policy-design-lab/data-import/issues/14)
- Feature to parse topline numbers and generate updated JSON
  documents. [#21](https://github.com/policy-design-lab/data-import/issues/21)

### Changed

- CSP data import program to update the category names and generate updated JSON
  files. [#4](https://github.com/policy-design-lab/data-import/issues/4)
- Title 1 Commodities code and JSON files based on new CSV
  file. [#9](https://github.com/policy-design-lab/data-import/issues/9)
- Title 1 Commodities state distribution JSON structure for bar
  chart. [#7](https://github.com/policy-design-lab/data-import/issues/7)
- SNAP data CSV and JSON files based on changes in SNAP table (May
  2023). [#23](https://github.com/policy-design-lab/data-import/issues/23)
- SNAP topline numbers based on changes in SNAP table (May
  2023). [#25](https://github.com/policy-design-lab/data-import/issues/25)
- All programs summary program to update all programs based on the topline.csv
  file. [#29](https://github.com/policy-design-lab/data-import/issues/29)
- Calculate average of base acres and recipient counts by program instead of
  totals. [#33](https://github.com/policy-design-lab/data-import/issues/33)
- Exclude 2018 base acres and recipient count data. [#35](https://github.com/policy-design-lab/data-import/issues/35)
- All programs and summary JSON files based on latest SNAP data. [#31](https://github.com/policy-design-lab/data-import/issues/31)

### Fixed

- Title 1 Commodities JSON files by adding zero entries. [#6](https://github.com/policy-design-lab/data-import/issues/6)
- Title 2 CSP JSON files by adding zero entries. [#10](https://github.com/policy-design-lab/data-import/issues/10)
- Error in calculating totals in the allprograms.json. [#27](https://github.com/policy-design-lab/data-import/issues/27)
- Average calculation in Title 1 Commodities. [#36](https://github.com/policy-design-lab/data-import/issues/36)
